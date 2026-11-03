"""Milestone 4 on real Postgres: per-endpoint circuit breaker, in-flight cap, rate limit,
fairness, auto-disable with notifications, and the endpoint circuit API.

Reuses the worker module's per-test database, receiver and worker fixtures.
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg
import httpx
import pytest

from hookrelay.db.engine import Database
from hookrelay.relay import Relay
from hookrelay.services import delivery_queue, housekeeping, notifications
from hookrelay.services.notifications import NotificationMessage
from tests.integration.conftest import _asyncpg_dsn, bearer, create_tenant, issue_key
from tests.integration.test_worker import (  # noqa: F401 - pytest fixtures
    _fetch,
    _make_due,
    _setup,
    _val,
    api,
    database,
    db_url,
    make_worker,
    receiver,
    settings_for,
    worker_template,
)
from tests.receiver import Receiver, Reply


async def _run(db_url: str, sql: str, *args: object) -> None:
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.execute(sql, *args)
    finally:
        await conn.close()


async def _state(db_url: str) -> Any:
    [row] = await _fetch(db_url, "SELECT * FROM endpoint_states")
    return row


async def _statuses(db_url: str) -> dict[str, int]:
    rows = await _fetch(db_url, "SELECT status, count(*) AS n FROM deliveries GROUP BY status")
    return {r["status"]: r["n"] for r in rows}


async def _cycle(worker: Any) -> int:
    """One worker cycle; returns how many deliveries it leased."""
    before = worker.stats.leased
    await worker.run_once()
    return int(worker.stats.leased - before)


async def _lease(database: Database, limit: int = 10, **kw: Any) -> list[Any]:
    async with database.session() as session:
        return await delivery_queue.lease_due(
            session, worker_id="test", limit=limit, lease_s=30, **kw
        )


# -- circuit breaker ------------------------------------------------------------------------


async def test_breaker_opens_defers_probes_and_closes(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(503)
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver, events=3)
    worker = make_worker(breaker_failure_threshold=2, breaker_cooldown_base_s=60)
    assert await _cycle(worker) == 3
    state = await _state(db_url)
    assert state["breaker_state"] == "open"
    assert state["consecutive_failures"] == 3
    assert state["trips"] == 1
    assert state["open_until"] is not None

    # Open: due deliveries wait in pending without spending attempts.
    await _make_due(db_url)
    assert await _cycle(worker) == 0
    assert await _val(db_url, "SELECT sum(attempt_count) FROM deliveries") == 3

    circuit = await api.get(f"/v1/endpoints/{endpoint_id}/circuit", headers=tenant.headers)
    assert circuit.status_code == 200, circuit.text
    body = circuit.json()
    assert body["state"] == "open"
    assert body["consecutive_failures"] == 3
    assert body["in_flight"] == 0
    assert body["max_in_flight"] == 10

    # Cooldown over: exactly one probe; its success closes the breaker.
    await _run(db_url, "UPDATE endpoint_states SET open_until = now() - interval '1 second'")
    receiver.handler = lambda _r: Reply(200)
    assert await _cycle(worker) == 1
    state = await _state(db_url)
    assert state["breaker_state"] == "closed"
    assert state["consecutive_failures"] == 0
    assert state["failing_since"] is None
    assert state["trips"] == 0
    assert await _cycle(worker) == 2
    assert await _statuses(db_url) == {"succeeded": 3}


async def test_failed_probe_reopens_with_a_longer_cooldown(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(500)
    await _setup(api, settings_for(), receiver, events=2)
    worker = make_worker(breaker_failure_threshold=1, breaker_cooldown_base_s=10)
    async with worker.database.session() as session:
        [first] = await delivery_queue.lease_due(session, worker_id="w", limit=1, lease_s=30)
    await worker.deliver(first)
    state = await _state(db_url)
    assert state["breaker_state"] == "open"
    first_cooldown = (state["open_until"] - state["opened_at"]).total_seconds()
    assert first_cooldown == pytest.approx(10)

    await _make_due(db_url)
    await _run(db_url, "UPDATE endpoint_states SET open_until = now()")
    leased = await _lease(worker.database)
    assert len(leased) == 1  # half-open: a single probe, the rest waits
    state = await _state(db_url)
    assert state["breaker_state"] == "half_open"
    assert state["open_until"] is None
    assert await _lease(worker.database) == []  # the probe is still in flight

    await worker.deliver(leased[0])
    state = await _state(db_url)
    assert state["breaker_state"] == "open"
    assert state["trips"] == 2
    assert (state["open_until"] - state["opened_at"]).total_seconds() == pytest.approx(20)


async def test_client_errors_do_not_trip_the_breaker(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(410)
    await _setup(api, settings_for(), receiver, events=3)
    worker = make_worker(breaker_failure_threshold=1)
    assert await _cycle(worker) == 3
    state = await _state(db_url)
    assert state["breaker_state"] == "closed"
    assert state["consecutive_failures"] == 0
    assert await _statuses(db_url) == {"failed": 3}  # permanent: failed, not dead


# -- in-flight cap, rate limit, fairness ----------------------------------------------------


async def test_max_in_flight_caps_leases_across_workers(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, database: Database, db_url: str
) -> None:
    await _setup(api, settings_for(), receiver, events=5, max_in_flight=2)
    assert len(await _lease(database)) == 2
    assert await _lease(database) == []  # a second worker sees no free slot
    await _run(
        db_url,
        "UPDATE deliveries SET status = 'succeeded', completed_at = now(), lease_token = NULL, "
        "leased_until = NULL, leased_by = NULL WHERE status = 'delivering'",
    )
    assert len(await _lease(database)) == 2


async def test_default_max_in_flight_applies_without_an_endpoint_override(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, database: Database
) -> None:
    await _setup(api, settings_for(), receiver, events=4)
    assert len(await _lease(database, default_max_in_flight=3)) == 3


async def test_rate_limit_spends_and_refills_tokens(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, database: Database, db_url: str
) -> None:
    await _setup(api, settings_for(), receiver, events=10, rate_limit_per_s=3, max_in_flight=100)
    assert len(await _lease(database)) == 3  # a full bucket holds one second's worth
    assert await _lease(database) == []
    state = await _state(db_url)
    assert state["rate_tokens"] == pytest.approx(0, abs=0.5)
    # One second later the bucket is full again (never more than one second's worth).
    await _run(
        db_url, "UPDATE endpoint_states SET rate_refilled_at = rate_refilled_at - interval '5 s'"
    )
    assert len(await _lease(database)) == 3


async def test_lease_is_fair_across_endpoints(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, database: Database, db_url: str
) -> None:
    tenant, _, _, _ = await _setup(api, settings_for(), receiver, events=5)
    other = await api.post(
        "/v1/endpoints",
        json={"url": f"http://receiver.test:{receiver.port}/other"},
        headers=tenant.headers,
    )
    other_id = other.json()["id"]
    sub = await api.post(
        f"/v1/endpoints/{other_id}/subscriptions",
        json={"event_type": "invoice.*"},
        headers=tenant.headers,
    )
    assert sub.status_code == 201, sub.text
    for _ in range(5):
        assert (
            await api.post(
                "/v1/events",
                json={"event_type": "invoice.paid", "payload": {}},
                headers=tenant.headers,
            )
        ).status_code == 202
    settings = settings_for()
    relay = Relay(database, settings)
    while (await relay.run_once()).claimed:
        pass
    leased = await _lease(database, limit=4)
    per_endpoint: dict[uuid.UUID, int] = {}
    for d in leased:
        per_endpoint[d.endpoint_id] = per_endpoint.get(d.endpoint_id, 0) + 1
    assert sorted(per_endpoint.values()) == [2, 2]  # a deep backlog cannot starve the other


# -- auto-disable and notifications ---------------------------------------------------------


class _RecordingNotifier:
    def __init__(self, *, fail: bool = False) -> None:
        self.sent: list[NotificationMessage] = []
        self.fail = fail

    async def send(self, message: NotificationMessage) -> None:
        if self.fail:
            raise RuntimeError("pager down")
        self.sent.append(message)


async def _trip_and_age(
    *, api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> tuple[Any, str, Any]:
    receiver.handler = lambda _r: Reply(502)
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver, events=2)
    worker = make_worker(breaker_failure_threshold=1, endpoint_auto_disable_after_s=3600)
    assert await _cycle(worker) == 2
    assert (await _state(db_url))["breaker_state"] == "open"
    await _make_due(db_url)
    await _run(
        db_url,
        "UPDATE endpoint_states SET open_until = now(), failing_since = now() - interval '2 hours'",
    )
    assert await _cycle(worker) == 1  # the probe fails too: sustained failure
    return tenant, endpoint_id, worker


async def test_sustained_failure_auto_disables_and_notifies(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    db_url: str,
) -> None:
    tenant, endpoint_id, worker = await _trip_and_age(
        api=api,
        settings_for=settings_for,
        receiver=receiver,
        make_worker=make_worker,
        db_url=db_url,
    )
    got = await api.get(f"/v1/endpoints/{endpoint_id}", headers=tenant.headers)
    assert got.json()["enabled"] is False
    assert got.json()["auto_disabled_at"] is not None
    [note] = await _fetch(db_url, "SELECT * FROM notifications")
    assert note["kind"] == "endpoint.disabled"
    assert note["status"] == "pending"

    # Paused, not failed: the backlog waits for the operator.
    await _make_due(db_url)
    assert await _cycle(worker) == 0
    assert "pending" in await _statuses(db_url)

    notifier = _RecordingNotifier()
    relay = Relay(worker.database, settings_for(), notifier=notifier)
    result = await relay.notify_once()
    assert (result.sent, result.failed, result.abandoned) == (1, 0, 0)
    [message] = notifier.sent
    assert message.payload["reason"] == "sustained_failure"
    assert str(message.endpoint_id) == endpoint_id
    assert await _val(db_url, "SELECT status FROM notifications") == "dispatched"
    assert (await relay.notify_once()).total == 0

    # Re-enabling clears auto_disabled_at and gives the breaker a clean start.
    receiver.handler = lambda _r: Reply(200)
    patched = await api.patch(
        f"/v1/endpoints/{endpoint_id}",
        json={"enabled": True},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["auto_disabled_at"] is None
    state = await _state(db_url)
    assert state["breaker_state"] == "closed"
    assert state["consecutive_failures"] == 0
    assert await _cycle(worker) == 2
    assert await _statuses(db_url) == {"succeeded": 2}


async def test_failing_notifier_retries_then_abandons(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    db_url: str,
) -> None:
    _, _, worker = await _trip_and_age(
        api=api,
        settings_for=settings_for,
        receiver=receiver,
        make_worker=make_worker,
        db_url=db_url,
    )
    relay = Relay(
        worker.database,
        settings_for(notification_max_attempts=2),
        notifier=_RecordingNotifier(fail=True),
    )
    assert (await relay.notify_once()).failed == 1
    [note] = await _fetch(db_url, "SELECT * FROM notifications")
    assert (note["status"], note["attempts"]) == ("pending", 1)
    assert "pager down" in note["last_error"]
    assert note["next_attempt_at"] > note["created_at"]
    await _run(db_url, "UPDATE notifications SET next_attempt_at = now()")
    assert (await relay.notify_once()).abandoned == 1
    assert await _val(db_url, "SELECT status FROM notifications") == "abandoned"
    assert relay.stats.notifications_abandoned == 1

    # Housekeeping purges finished notifications after their retention.
    await _run(db_url, "UPDATE notifications SET completed_at = now() - interval '2 days'")
    async with worker.database.session() as session:
        result = await housekeeping.sweep(
            session, outbox_retention_s=3600, batch_size=100, notification_retention_s=86400
        )
    assert result.notifications == 1
    assert await _val(db_url, "SELECT count(*) FROM notifications") == 0


async def test_logging_notifier_is_the_default(
    database: Database, settings_for: Any, db_url: str
) -> None:
    relay = Relay(database, settings_for())
    assert isinstance(relay.notifier, notifications.LoggingNotifier)
    tenant_id = await _val(
        db_url, "INSERT INTO tenants (slug, name) VALUES ('n-t', 'N') RETURNING id"
    )
    async with database.session() as session:
        await notifications.enqueue(
            session, tenant_id=tenant_id, endpoint_id=None, kind="test.kind", payload={"a": 1}
        )
    assert (await relay.notify_once()).sent == 1
    assert await _val(db_url, "SELECT status FROM notifications") == "dispatched"


# -- endpoint limits API ---------------------------------------------------------------------


async def test_endpoint_limits_are_validated_and_updatable(
    api: httpx.AsyncClient, receiver: Receiver, db_url: str
) -> None:
    tenant = await create_tenant(api)
    url = f"http://receiver.test:{receiver.port}/limits"
    too_many = await api.post(
        "/v1/endpoints", json={"url": url, "max_in_flight": 101}, headers=tenant.headers
    )
    assert too_many.status_code == 422
    assert too_many.json()["error"]["code"] == "max_in_flight_too_high"
    too_fast = await api.post(
        "/v1/endpoints", json={"url": url, "rate_limit_per_s": 1001}, headers=tenant.headers
    )
    assert too_fast.json()["error"]["code"] == "rate_limit_too_high"
    for bad in ({"max_in_flight": 0}, {"rate_limit_per_s": 0}, {"max_in_flight": 1001}):
        response = await api.post("/v1/endpoints", json={"url": url, **bad}, headers=tenant.headers)
        assert response.status_code == 422, bad

    created = await api.post(
        "/v1/endpoints",
        json={"url": url, "max_in_flight": 5, "rate_limit_per_s": 20},
        headers=tenant.headers,
    )
    assert created.status_code == 201, created.text
    body = created.json()
    assert (body["max_in_flight"], body["rate_limit_per_s"], body["auto_disabled_at"]) == (
        5,
        20,
        None,
    )
    # A trigger creates the breaker state row together with the endpoint.
    assert (
        await _val(
            db_url,
            "SELECT breaker_state FROM endpoint_states WHERE endpoint_id = $1",
            uuid.UUID(body["id"]),
        )
        == "closed"
    )

    patched = await api.patch(
        f"/v1/endpoints/{body['id']}",
        json={"rate_limit_per_s": None, "max_in_flight": 50},
        headers={**tenant.headers, "If-Match": '"1"'},
    )
    assert patched.status_code == 200, patched.text
    assert (patched.json()["rate_limit_per_s"], patched.json()["max_in_flight"]) == (None, 50)
    rejected = await api.patch(
        f"/v1/endpoints/{body['id']}",
        json={"max_in_flight": 500},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert rejected.json()["error"]["code"] == "max_in_flight_too_high"

    # Re-sending enabled=true to a healthy endpoint keeps its failure streak.
    await _run(
        db_url,
        "UPDATE endpoint_states SET consecutive_failures = 2, failing_since = now() "
        "WHERE endpoint_id = $1",
        uuid.UUID(body["id"]),
    )
    again = await api.patch(
        f"/v1/endpoints/{body['id']}",
        json={"enabled": True},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert again.status_code == 200
    assert (
        await _val(
            db_url,
            "SELECT consecutive_failures FROM endpoint_states WHERE endpoint_id = $1",
            uuid.UUID(body["id"]),
        )
        == 2
    )


async def test_circuit_api_is_tenant_scoped_and_reader_accessible(
    api: httpx.AsyncClient, receiver: Receiver
) -> None:
    tenant = await create_tenant(api)
    created = await api.post(
        "/v1/endpoints",
        json={"url": f"http://receiver.test:{receiver.port}/c"},
        headers=tenant.headers,
    )
    endpoint_id = created.json()["id"]
    reader = bearer(await issue_key(api, tenant, "reader"))
    got = await api.get(f"/v1/endpoints/{endpoint_id}/circuit", headers=reader)
    assert got.status_code == 200
    assert got.json()["state"] == "closed"
    assert got.json()["rate_limit_per_s"] is None
    other = await create_tenant(api)
    assert (
        await api.get(f"/v1/endpoints/{endpoint_id}/circuit", headers=other.headers)
    ).status_code == 404
