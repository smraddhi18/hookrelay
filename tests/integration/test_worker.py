"""The delivery worker on real Postgres: leasing under concurrency, fencing, lease expiry,
retries, terminal failure, SIGTERM drain, the read API and end-to-end signed delivery to a
local receiver. Each test gets its own migrated database because workers lease across tenants.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from alembic import command

from hookrelay import worker as worker_module
from hookrelay.core.config import Settings
from hookrelay.core.signing import verify
from hookrelay.db.engine import Database
from hookrelay.relay import Relay
from hookrelay.services import delivery_queue
from hookrelay.services.delivery_policy import ErrorClass
from hookrelay.services.delivery_queue import AttemptRecord
from hookrelay.worker import Worker, main, serve
from tests.integration.conftest import (
    TenantCtx,
    _admin,
    _asyncpg_dsn,
    _url_for,
    alembic_config,
    api_client,
    api_settings,
    bearer,
    create_scratch_database,
    create_tenant,
    drop_scratch_database,
    issue_key,
    publish,
    register_event_type,
    subscribe,
)
from tests.receiver import Receiver, Reply, local_resolver, running_receiver


@pytest.fixture(scope="module")
def worker_template() -> Iterator[str]:
    name, url = create_scratch_database("hookrelay_wtpl")
    try:
        command.upgrade(alembic_config(url), "head")
        yield name
    finally:
        drop_scratch_database(name)


@pytest.fixture
def db_url(worker_template: str) -> Iterator[str]:
    name = f"hookrelay_worker_{uuid.uuid4().hex[:10]}"
    asyncio.run(_admin(f'CREATE DATABASE "{name}" TEMPLATE "{worker_template}"'))
    try:
        yield _url_for(name)
    finally:
        drop_scratch_database(name)


@pytest.fixture
async def receiver() -> AsyncIterator[Receiver]:
    async with running_receiver() as r:
        yield r


@pytest.fixture
def settings_for(db_url: str, tmp_path: Path, receiver: Receiver) -> Any:
    def _make(**overrides: Any) -> Settings:
        base: dict[str, Any] = {
            "endpoint_allow_http": True,
            "endpoint_allow_private_networks": True,
            "delivery_allow_private_networks": True,
            "delivery_allowed_ports": [receiver.port],
            "relay_heartbeat_file": str(tmp_path / "relay.hb"),
            "worker_heartbeat_file": str(tmp_path / "worker.hb"),
            "worker_poll_min_ms": 10,
            "worker_poll_max_ms": 50,
            "worker_concurrency": 8,
            "delivery_backoff_base_s": 1.0,
            "delivery_backoff_max_s": 2.0,
        }
        base.update(overrides)
        return api_settings(db_url, **base)

    return _make


@pytest.fixture
async def api(db_url: str, settings_for: Any) -> AsyncIterator[httpx.AsyncClient]:
    s = settings_for()
    async with api_client(
        db_url,
        endpoint_allow_http=s.endpoint_allow_http,
        endpoint_allow_private_networks=s.endpoint_allow_private_networks,
    ) as client:
        yield client


@pytest.fixture
async def database(settings_for: Any) -> AsyncIterator[Database]:
    db = Database.from_settings(settings_for())
    try:
        yield db
    finally:
        await db.dispose()


@pytest.fixture
async def make_worker(settings_for: Any) -> AsyncIterator[Any]:
    created: list[tuple[Worker, Database]] = []

    def _make(**overrides: Any) -> Worker:
        settings = settings_for(**overrides)
        db = Database.from_settings(settings)
        w = Worker(db, settings, resolver=local_resolver)
        created.append((w, db))
        return w

    yield _make
    for w, db in created:
        await w.aclose()
        await db.dispose()


async def _fetch(url: str, sql: str, *args: object) -> list[asyncpg.Record]:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        return list(await conn.fetch(sql, *args))
    finally:
        await conn.close()


async def _val(url: str, sql: str, *args: object) -> Any:
    return (await _fetch(url, sql, *args))[0][0]


async def _endpoint(
    api: httpx.AsyncClient, tenant: TenantCtx, url: str, **extra: Any
) -> tuple[str, str]:
    response = await api.post("/v1/endpoints", json={"url": url, **extra}, headers=tenant.headers)
    assert response.status_code == 201, response.text
    body = response.json()
    return str(body["id"]), str(body["signing_secret"])


async def _setup(
    api: httpx.AsyncClient,
    settings: Settings,
    receiver: Receiver,
    *,
    events: int = 1,
    path: str = "/hook",
    **endpoint_extra: Any,
) -> tuple[TenantCtx, str, str, list[str]]:
    """Tenant + endpoint on the receiver + ``events`` published and fanned out."""
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, "invoice.paid")
    url = f"http://receiver.test:{receiver.port}{path}"
    endpoint_id, secret = await _endpoint(api, tenant, url, **endpoint_extra)
    await subscribe(api, tenant, endpoint_id, "invoice.*")
    event_ids = []
    for i in range(events):
        response = await publish(api, tenant, "invoice.paid", {"i": i})
        assert response.status_code == 202, response.text
        event_ids.append(response.json()["id"])
    db = Database.from_settings(settings)
    try:
        relay = Relay(db, settings)
        while (await relay.run_once()).claimed:
            pass
    finally:
        await db.dispose()
    return tenant, endpoint_id, secret, event_ids


async def _make_due(db_url: str) -> None:
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.execute("UPDATE deliveries SET next_attempt_at = now() WHERE status = 'pending'")
    finally:
        await conn.close()


async def _delivery(db_url: str) -> asyncpg.Record:
    rows = await _fetch(db_url, "SELECT * FROM deliveries")
    assert len(rows) == 1
    return rows[0]


async def _attempts(db_url: str) -> list[asyncpg.Record]:
    return await _fetch(db_url, "SELECT * FROM delivery_attempts ORDER BY attempt_number")


# -- end to end -----------------------------------------------------------------------------


async def test_end_to_end_signed_delivery_and_read_api(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    settings = settings_for()
    tenant, _, secret, [event_id] = await _setup(
        api, settings, receiver, headers={"X-Acme-Tenant": "acme-private"}
    )
    worker = make_worker()
    stats = await worker.run_once()
    assert (stats.leased, stats.succeeded) == (1, 1)

    [received] = receiver.requests
    assert verify(secret, received.headers, received.body) == event_id
    assert received.headers["x-acme-tenant"] == "acme-private"
    assert b'"type":"invoice.paid"' in received.body

    delivery = await _delivery(db_url)
    assert delivery["status"] == "succeeded"
    assert delivery["attempt_count"] == 1
    assert delivery["completed_at"] is not None
    assert delivery["lease_token"] is None
    assert delivery["last_status_code"] == 200

    reader = bearer(await issue_key(api, tenant, "reader"))
    got = await api.get(f"/v1/deliveries/{delivery['id']}", headers=reader)
    assert got.status_code == 200, got.text
    assert got.json()["status"] == "succeeded"
    assert got.json()["last_status_code"] == 200
    assert got.json()["last_error_class"] is None
    listed = await api.get(f"/v1/deliveries/{delivery['id']}/attempts", headers=reader)
    assert listed.status_code == 200
    [attempt] = listed.json()["data"]
    assert attempt["outcome"] == "succeeded"
    assert attempt["attempt_number"] == 1
    assert attempt["status_code"] == 200
    assert attempt["worker_id"] == worker.worker_id
    assert attempt["request_headers"]["x-acme-tenant"] == "********"
    assert attempt["request_headers"]["webhook-signature"] == "v1,[redacted]"
    assert attempt["request_headers"]["webhook-id"] == event_id
    assert "acme-private" not in listed.text
    assert secret.removeprefix("whsec_") not in listed.text
    assert await asyncio.to_thread(Path(settings.worker_heartbeat_file).exists)

    # The event's delivery list shows the new fields too.
    by_event = await api.get(f"/v1/events/{event_id}/deliveries", headers=reader)
    assert by_event.json()["data"][0]["status"] == "succeeded"


async def test_read_api_is_tenant_scoped_and_paginated(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(503, b"down")
    tenant, *_ = await _setup(api, settings_for(), receiver)
    worker = make_worker()
    for _ in range(3):
        await worker.run_once()
        await _make_due(db_url)
    delivery_id = (await _delivery(db_url))["id"]

    page1 = await api.get(f"/v1/deliveries/{delivery_id}/attempts?limit=2", headers=tenant.headers)
    body1 = page1.json()
    assert [a["attempt_number"] for a in body1["data"]] == [3, 2]  # newest first
    assert body1["has_more"]
    page2 = await api.get(
        f"/v1/deliveries/{delivery_id}/attempts",
        params={"limit": 2, "cursor": body1["next_cursor"]},
        headers=tenant.headers,
    )
    assert [a["attempt_number"] for a in page2.json()["data"]] == [1]
    assert not page2.json()["has_more"]

    other = await create_tenant(api)
    for path in (f"/v1/deliveries/{delivery_id}", f"/v1/deliveries/{delivery_id}/attempts"):
        hidden = await api.get(path, headers=other.headers)
        assert hidden.status_code == 404
        assert hidden.json()["error"]["code"] == "delivery_not_found"
        assert (await api.get(path)).status_code == 401
    missing = await api.get(f"/v1/deliveries/{uuid.uuid4()}", headers=tenant.headers)
    assert missing.status_code == 404


# -- retries and terminal failure ------------------------------------------------------------


async def test_retryable_failure_is_rescheduled_then_succeeds(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    replies = iter([Reply(503, b"maintenance", {"retry-after": "1"}), Reply(200, b"ok")])
    receiver.handler = lambda _r: next(replies)
    await _setup(api, settings_for(), receiver)
    worker = make_worker()

    before = datetime.now(UTC)
    stats = await worker.run_once()
    assert stats.retried == 1
    delivery = await _delivery(db_url)
    assert delivery["status"] == "pending"
    assert delivery["attempt_count"] == 1
    assert delivery["last_status_code"] == 503
    assert delivery["last_error_class"] == "http_status"
    assert delivery["lease_token"] is None
    assert delivery["next_attempt_at"] > before  # Retry-After: 1 is a floor
    [attempt] = await _attempts(db_url)
    assert attempt["outcome"] == "retrying"
    assert attempt["next_attempt_at"] == delivery["next_attempt_at"]
    assert attempt["response_body"] == "maintenance"

    assert (await worker.run_once()).leased == 1  # not due yet: stats are cumulative
    assert len(receiver.requests) == 1
    await _make_due(db_url)
    assert (await worker.run_once()).succeeded == 1
    delivery = await _delivery(db_url)
    assert delivery["status"] == "succeeded"
    assert delivery["attempt_count"] == 2
    assert delivery["last_error_class"] is None
    assert [a["outcome"] for a in await _attempts(db_url)] == ["retrying", "succeeded"]
    assert [r.headers["webhook-id"] for r in receiver.requests] == [
        receiver.requests[0].headers["webhook-id"]
    ] * 2  # same id on every retry, so receivers can dedupe


async def test_max_attempts_ends_in_dead(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(500, b"boom")
    await _setup(api, settings_for(), receiver)
    worker = make_worker(delivery_max_attempts=3)
    for _ in range(3):
        await worker.run_once()
        await _make_due(db_url)
    assert (await worker.run_once()).leased == 3  # nothing left to lease
    delivery = await _delivery(db_url)
    assert delivery["status"] == "dead"  # retryable but exhausted: the dead-letter queue
    assert delivery["attempt_count"] == 3
    assert delivery["completed_at"] is not None
    assert [a["outcome"] for a in await _attempts(db_url)] == ["retrying", "retrying", "failed"]
    assert len(receiver.requests) == 3
    assert worker.stats.failed == 1
    assert worker.stats.dead_lettered == 1


async def test_terminal_4xx_fails_immediately(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(410, b"gone")
    await _setup(api, settings_for(), receiver)
    await make_worker().run_once()
    delivery = await _delivery(db_url)
    assert delivery["status"] == "failed"
    assert delivery["last_status_code"] == 410
    [attempt] = await _attempts(db_url)
    assert attempt["outcome"] == "failed"
    assert attempt["error_class"] == "http_status"


async def test_transport_errors_are_retried(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(200, delay_s=1.6)
    await _setup(api, settings_for(), receiver, timeout_ms=1000)
    await make_worker().run_once()
    delivery = await _delivery(db_url)
    assert delivery["status"] == "pending"
    assert delivery["last_status_code"] is None
    assert delivery["last_error_class"] == "timeout"
    [attempt] = await _attempts(db_url)
    assert attempt["status_code"] is None
    assert attempt["error_class"] == "timeout"


async def test_blocked_destination_fails_without_a_request(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    await _setup(api, settings_for(), receiver)
    await make_worker(delivery_allow_private_networks=False).run_once()
    delivery = await _delivery(db_url)
    assert delivery["status"] == "failed"
    assert delivery["last_error_class"] == "destination_blocked"
    assert receiver.requests == []


async def test_max_age_ends_in_dead(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(503)
    await _setup(api, settings_for(), receiver)
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        # Max age counts from the start of the current run (created_at unless replayed).
        await conn.execute("UPDATE deliveries SET run_started_at = now() - interval '2 hours'")
    finally:
        await conn.close()
    await make_worker(delivery_max_age_s=3600).run_once()
    delivery = await _delivery(db_url)
    assert delivery["status"] == "dead"
    assert delivery["last_error_class"] == "max_age_exceeded"
    [attempt] = await _attempts(db_url)
    assert attempt["outcome"] == "failed"
    assert attempt["status_code"] == 503
    assert attempt["error_class"] == "max_age_exceeded"


async def test_paused_endpoints_and_suspended_tenants_are_not_leased(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    await _setup(api, settings_for(), receiver)
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.execute("UPDATE endpoints SET enabled = false")
        worker = make_worker()
        assert (await worker.run_once()).leased == 0
        await conn.execute("UPDATE endpoints SET enabled = true")
        await conn.execute("UPDATE tenants SET status = 'suspended'")
        assert (await worker.run_once()).leased == 0
        assert (await _delivery(db_url))["status"] == "pending"  # paused, not failed
        await conn.execute("UPDATE tenants SET status = 'active'")
        assert (await worker.run_once()).succeeded == 1
    finally:
        await conn.close()


# -- concurrency and fencing -----------------------------------------------------------------


async def test_concurrent_workers_never_deliver_the_same_delivery_twice(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    in_flight: Counter[str] = Counter()
    overlaps: list[str] = []

    async def handler(request: Any) -> Reply:
        key = request.headers["webhook-id"]
        in_flight[key] += 1
        if in_flight[key] > 1:
            overlaps.append(key)  # pragma: no cover - would be the bug under test
        await asyncio.sleep(0.01)
        in_flight[key] -= 1
        return Reply(204, b"")

    receiver.handler = handler
    total = 40
    await _setup(api, settings_for(), receiver, events=total)
    workers = [make_worker(worker_concurrency=4, worker_batch_size=4) for _ in range(5)]

    async def run_until_done() -> None:
        while await _val(db_url, "SELECT count(*) FROM deliveries WHERE status = 'pending'"):
            await asyncio.gather(*(w.run_once() for w in workers))

    await asyncio.wait_for(run_until_done(), timeout=30)
    assert overlaps == []
    ids = Counter(r.headers["webhook-id"] for r in receiver.requests)
    assert len(ids) == total
    assert set(ids.values()) == {1}
    assert sum(w.stats.leased for w in workers) == total
    assert sum(w.stats.succeeded for w in workers) == total
    assert await _val(db_url, "SELECT count(*) FROM delivery_attempts") == total
    assert await _val(db_url, "SELECT count(DISTINCT worker_id) FROM delivery_attempts") > 1


async def test_stale_lease_token_is_rejected(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, database: Database, db_url: str
) -> None:
    settings = settings_for()
    await _setup(api, settings, receiver)
    worker = Worker(database, settings, resolver=local_resolver)
    try:
        async with database.session() as session:
            [first] = await delivery_queue.lease_due(session, worker_id="a", limit=5, lease_s=30)
        # Worker "a" stalls; its lease expires and is reclaimed, then "b" takes a new lease.
        conn = await asyncpg.connect(_asyncpg_dsn(db_url))
        try:
            await conn.execute("UPDATE deliveries SET leased_until = now() - interval '1 second'")
            assert (await worker.reclaim_once()).retried == 1
            await conn.execute("UPDATE deliveries SET next_attempt_at = now()")
        finally:
            await conn.close()
        async with database.session() as session:
            [second] = await delivery_queue.lease_due(session, worker_id="b", limit=5, lease_s=30)
        assert second.id == first.id
        assert second.lease_token != first.lease_token
        assert second.attempt_number == 2

        record = await worker.attempt(first)
        async with database.session() as session:
            assert await delivery_queue.renew_leases(session, [first.lease], lease_s=30) == set()
            stale = await delivery_queue.complete(
                session,
                first,
                record,
                worker_id="a",
                now=datetime.now(UTC),
                policy=worker.policy,
            )
        assert stale is None
        delivery = await _delivery(db_url)
        assert delivery["status"] == "delivering"
        assert delivery["lease_token"] == second.lease_token
        assert delivery["leased_by"] == "b"
        attempts = await _attempts(db_url)
        assert [(a["attempt_number"], a["error_class"]) for a in attempts] == [(1, "lease_expired")]

        async with database.session() as session:
            done = await delivery_queue.complete(
                session,
                second,
                record,
                worker_id="b",
                now=datetime.now(UTC),
                policy=worker.policy,
            )
            assert await delivery_queue.release_leases(session, [first.lease]) == set()
        assert done is not None
        assert done.status == "succeeded"
        assert (await _delivery(db_url))["attempt_count"] == 2
        assert len(await _attempts(db_url)) == 2
    finally:
        await worker.aclose()


async def test_expired_lease_is_reclaimed_and_released(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    await _setup(api, settings_for(), receiver)
    worker = make_worker()
    async with worker.database.session() as session:
        await delivery_queue.lease_due(session, worker_id="crashed", limit=1, lease_s=30)
    assert (await worker.run_once()).leased == 0  # still leased by the crashed worker

    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.execute("UPDATE deliveries SET leased_until = now() - interval '1 second'")
    finally:
        await conn.close()
    reclaimed = await worker.reclaim_once()
    assert (reclaimed.retried, reclaimed.failed) == (1, 0)
    delivery = await _delivery(db_url)
    assert delivery["status"] == "pending"
    assert delivery["lease_token"] is None
    assert delivery["last_error_class"] == "lease_expired"
    [attempt] = await _attempts(db_url)
    assert attempt["worker_id"] == "crashed"
    assert attempt["outcome"] == "retrying"

    await _make_due(db_url)
    assert (await worker.run_once()).succeeded == 1
    delivery = await _delivery(db_url)
    assert delivery["status"] == "succeeded"
    assert delivery["attempt_count"] == 2
    assert len(receiver.requests) == 1


async def test_expired_lease_on_the_last_attempt_is_dead_lettered(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    await _setup(api, settings_for(), receiver)
    worker = make_worker(delivery_max_attempts=1)
    async with worker.database.session() as session:
        await delivery_queue.lease_due(session, worker_id="crashed", limit=1, lease_s=30)
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.execute("UPDATE deliveries SET leased_until = now() - interval '1 second'")
    finally:
        await conn.close()
    assert (await worker.reclaim_once()).failed == 1
    delivery = await _delivery(db_url)
    assert delivery["status"] == "dead"
    assert delivery["completed_at"] is not None
    [attempt] = await _attempts(db_url)
    assert attempt["outcome"] == "failed"


async def test_renewal_extends_leases_and_cancels_lost_ones(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    gate = asyncio.Event()

    async def handler(_request: Any) -> Reply:
        await gate.wait()
        return Reply(200)

    receiver.handler = handler
    await _setup(api, settings_for(), receiver, events=2)
    worker = make_worker(worker_lease_s=30)
    assert await worker.poll_once() == 2
    await asyncio.wait_for(_until(lambda: len(receiver.requests) == 2), 5)
    before = {
        r["id"]: r["leased_until"]
        for r in await _fetch(db_url, "SELECT id, leased_until FROM deliveries")
    }
    await asyncio.sleep(0.05)
    assert await worker.renew_once() == 0
    after = await _fetch(db_url, "SELECT id, leased_until FROM deliveries")
    assert all(r["leased_until"] > before[r["id"]] for r in after)

    # Another worker took one lease over (e.g. after a long GC pause): renewal notices.
    stolen = after[0]["id"]
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.execute(
            "UPDATE deliveries SET lease_token = gen_random_uuid(), leased_by = 'other' "
            "WHERE id = $1",
            stolen,
        )
    finally:
        await conn.close()
    assert await worker.renew_once() == 1
    gate.set()
    await asyncio.wait_for(_until(lambda: worker.in_flight == 0), 5)
    assert worker.stats.lease_lost == 1
    assert worker.stats.succeeded == 1
    rows = {r["id"]: r for r in await _fetch(db_url, "SELECT * FROM deliveries")}
    assert rows[stolen]["status"] == "delivering"
    assert rows[stolen]["leased_by"] == "other"


async def test_run_drains_on_sigterm_and_releases_leases(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    async def handler(request: Any) -> Reply:
        return Reply(200, delay_s=0.0 if request.path == "/fast" else 10.0)

    receiver.handler = handler
    await _setup(api, settings_for(), receiver, events=2)
    worker = make_worker(worker_drain_timeout_s=0.3)
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(_until(lambda: len(receiver.requests) == 2), 5)
    os.kill(os.getpid(), signal.SIGTERM)
    stats = await asyncio.wait_for(task, 5)
    assert stats.released == 2
    rows = await _fetch(db_url, "SELECT * FROM deliveries")
    assert {r["status"] for r in rows} == {"pending"}
    assert {r["attempt_count"] for r in rows} == {0}  # a drained attempt is not spent
    assert all(r["lease_token"] is None for r in rows)
    assert await _val(db_url, "SELECT count(*) FROM delivery_attempts") == 0
    assert signal.getsignal(signal.SIGTERM) in {signal.SIG_DFL, signal.default_int_handler}


async def test_run_finishes_in_flight_attempts_within_the_drain_timeout(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(200, delay_s=0.3)
    await _setup(api, settings_for(), receiver, events=3)
    worker = make_worker(worker_drain_timeout_s=5, worker_concurrency=2)
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(_until(lambda: len(receiver.requests) >= 2), 5)
    worker.stop()
    stats = await asyncio.wait_for(task, 5)
    assert stats.released == 0
    statuses = Counter(r["status"] for r in await _fetch(db_url, "SELECT status FROM deliveries"))
    assert statuses["succeeded"] == stats.succeeded >= 2
    assert statuses["succeeded"] + statuses["pending"] == 3


async def test_run_delivers_continuously_and_survives_loop_errors(
    api: httpx.AsyncClient,
    *,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, *_ = await _setup(api, settings_for(), receiver)
    worker = make_worker(worker_lease_renew_interval_s=0.05, worker_reclaim_interval_s=0.05)
    real_lease = delivery_queue.lease_due
    calls = {"n": 0}

    async def flaky(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        return await real_lease(*args, **kwargs)

    async def broken_reclaim(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("reclaim down")

    monkeypatch.setattr(delivery_queue, "lease_due", flaky)
    monkeypatch.setattr(delivery_queue, "reclaim_expired", broken_reclaim)
    task = asyncio.create_task(worker.run())
    await asyncio.wait_for(_until(lambda: worker.stats.succeeded == 1), 5)
    # A second event published while the worker runs is picked up by the polling loop.
    response = await publish(api, tenant, "invoice.paid", {"late": True})
    assert response.status_code == 202
    db = Database.from_settings(settings_for())
    try:
        await Relay(db, settings_for()).run_once()
    finally:
        await db.dispose()
    await asyncio.wait_for(_until(lambda: worker.stats.succeeded == 2), 5)
    await asyncio.sleep(0.15)  # let the background loops run (and fail) a few times
    worker.stop()
    stats = await asyncio.wait_for(task, 5)
    assert stats.loop_errors >= 2
    assert len(receiver.requests) == 2


@pytest.mark.parametrize("batch_max", [1, 32], ids=["single", "batched"])
async def test_attempt_crash_keeps_the_lease_for_the_reaper(
    api: httpx.AsyncClient,
    *,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
    batch_max: int,
) -> None:
    await _setup(api, settings_for(), receiver)
    worker = make_worker(worker_complete_batch_max=batch_max)

    async def crash(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("db went away")

    # Batched: the group write fails, its per-attempt fallback fails too.
    monkeypatch.setattr(delivery_queue, "complete", crash)
    monkeypatch.setattr(delivery_queue, "complete_many", crash)
    stats = await worker.run_once()
    assert stats.loop_errors == 1
    assert worker.in_flight == 0
    assert (await _delivery(db_url))["status"] == "delivering"


async def test_serve_once_and_cli(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Receiver,
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # serve()/main() use the system resolver, so the endpoint is a literal loopback address.
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, "invoice.paid")
    endpoint_id, _ = await _endpoint(api, tenant, f"http://127.0.0.1:{receiver.port}/hook")
    await subscribe(api, tenant, endpoint_id, "*")
    settings = settings_for()
    for _ in range(2):
        assert (await publish(api, tenant, "invoice.paid")).status_code == 202
    db = Database.from_settings(settings)
    try:
        await Relay(db, settings).run_once()
    finally:
        await db.dispose()

    stats = await serve(settings, once=True)
    assert stats.succeeded == 2

    assert (await publish(api, tenant, "invoice.paid")).status_code == 202
    db = Database.from_settings(settings)
    try:
        await Relay(db, settings).run_once()
    finally:
        await db.dispose()
    monkeypatch.setattr(worker_module, "get_settings", lambda: settings)
    monkeypatch.setattr(worker_module, "configure_logging", lambda _settings: None)
    assert await asyncio.to_thread(main, ["--once"]) == 0
    assert await _val(db_url, "SELECT count(*) FROM deliveries WHERE status = 'succeeded'") == 3
    assert main(["--healthcheck"]) == 0
    assert len(receiver.requests) == 3


async def _until(predicate: Any, interval: float = 0.01) -> None:
    while not predicate():  # noqa: ASYNC110 - polls state owned by other tasks
        await asyncio.sleep(interval)


def test_attempt_record_is_frozen() -> None:
    now = datetime.now(UTC)
    record = AttemptRecord(
        status_code=None,
        error_class=ErrorClass.TIMEOUT,
        error_message="x",
        retryable=True,
        retry_after_s=None,
        latency_ms=1,
        request_url="https://a.example",
        request_headers={},
        response_body=None,
        response_truncated=False,
        started_at=now,
        finished_at=now,
    )
    with pytest.raises(AttributeError):
        record.status_code = 200  # type: ignore[misc]
    assert time.time() > 0
