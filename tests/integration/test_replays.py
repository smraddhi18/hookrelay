"""Milestone 4 replay API on real Postgres: single, by event and bulk replays; roles, tenant
scope, idempotency, the per-tenant rate limit, window validation, caps and the audit trail.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

from hookrelay.services import housekeeping
from tests.integration.conftest import (
    TenantCtx,
    _asyncpg_dsn,
    api_client,
    bearer,
    create_tenant,
    issue_key,
)
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


@pytest.fixture
def dead_backlog(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> Any:
    """Factory: ``events`` deliveries to one endpoint, all dead-lettered after a single 503.

    Returns ``(tenant, endpoint_id, event_ids, worker)``."""

    async def _make(events: int = 1) -> tuple[TenantCtx, str, list[str], Any]:
        receiver.handler = lambda _r: Reply(503)
        tenant, endpoint_id, _, event_ids = await _setup(
            api, settings_for(), receiver, events=events
        )
        worker = make_worker(delivery_max_attempts=1, breaker_failure_threshold=100)
        await worker.run_once()
        rows = await _fetch(db_url, "SELECT status FROM deliveries")
        assert {r["status"] for r in rows} == {"dead"}
        return tenant, endpoint_id, event_ids, worker

    return _make


async def _delivery_ids(db_url: str) -> list[str]:
    rows = await _fetch(db_url, "SELECT id FROM deliveries ORDER BY created_at, id")
    return [str(r["id"]) for r in rows]


@pytest.fixture
async def limited_api(db_url: str) -> AsyncIterator[Any]:
    """Factory for API clients with tightened replay limits (same database)."""
    stack: list[Any] = []

    async def _make(**overrides: Any) -> httpx.AsyncClient:
        cm = api_client(
            db_url, endpoint_allow_http=True, endpoint_allow_private_networks=True, **overrides
        )
        stack.append(cm)
        return await cm.__aenter__()

    yield _make
    for cm in reversed(stack):
        await cm.__aexit__(None, None, None)


def _window(**delta: float) -> dict[str, str]:
    now = datetime.now(UTC)
    return {
        "created_after": (now - timedelta(hours=1)).isoformat(),
        "created_before": (
            now + timedelta(**delta) if delta else now + timedelta(minutes=1)
        ).isoformat(),
    }


# -- single delivery -------------------------------------------------------------------------


async def test_replaying_a_dead_delivery_gives_it_a_fresh_run(
    api: httpx.AsyncClient, receiver: Receiver, db_url: str, dead_backlog: Any
) -> None:
    tenant, _, _, worker = await dead_backlog()
    [delivery_id] = await _delivery_ids(db_url)
    response = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=tenant.headers)
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["id"] == delivery_id  # same id, so the receiver sees the same webhook-id
    assert body["status"] == "pending"
    assert body["replay_count"] == 1
    assert body["completed_at"] is None
    assert body["run_started_at"] is not None
    [row] = await _fetch(db_url, "SELECT attempt_offset, attempt_count FROM deliveries")
    assert (row["attempt_offset"], row["attempt_count"]) == (1, 1)

    # The new run has its own budget (max_attempts=1 would otherwise be exhausted already).
    receiver.handler = lambda _r: Reply(200)
    await worker.run_once()
    got = await api.get(f"/v1/deliveries/{delivery_id}", headers=tenant.headers)
    assert got.json()["status"] == "succeeded"
    attempts = await _fetch(
        db_url, "SELECT attempt_number, outcome FROM delivery_attempts ORDER BY attempt_number"
    )
    assert [(a["attempt_number"], a["outcome"]) for a in attempts] == [
        (1, "failed"),
        (2, "succeeded"),
    ]
    first, second = receiver.requests
    assert first.headers["webhook-id"] == second.headers["webhook-id"]

    # A succeeded delivery can be replayed too (e.g. the receiver lost it).
    again = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=tenant.headers)
    assert again.status_code == 202
    assert again.json()["replay_count"] == 2

    [audit] = (await api.get("/v1/replays", headers=tenant.headers)).json()["data"][:1]
    assert audit["scope"] == "delivery"
    assert audit["replayed"] == 1
    assert audit["filters"]["delivery_id"] == delivery_id


async def test_single_replay_conflicts_roles_and_tenant_scope(
    api: httpx.AsyncClient, db_url: str, dead_backlog: Any
) -> None:
    tenant, endpoint_id, _, _ = await dead_backlog()
    [delivery_id] = await _delivery_ids(db_url)
    path = f"/v1/deliveries/{delivery_id}/replay"

    reader = bearer(await issue_key(api, tenant, "reader"))
    assert (await api.post(path, headers=reader)).status_code == 403
    other = await create_tenant(api)
    missing = await api.post(path, headers=other.headers)
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "delivery_not_found"
    unknown = await api.post(f"/v1/deliveries/{uuid.uuid4()}/replay", headers=tenant.headers)
    assert unknown.status_code == 404

    paused = await api.patch(
        f"/v1/endpoints/{endpoint_id}",
        json={"enabled": False},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert paused.status_code == 200
    blocked = await api.post(path, headers=tenant.headers)
    assert blocked.status_code == 409
    assert blocked.json()["error"]["code"] == "endpoint_unavailable"
    await api.patch(
        f"/v1/endpoints/{endpoint_id}",
        json={"enabled": True},
        headers={**tenant.headers, "If-Match": "*"},
    )

    publisher = bearer(await issue_key(api, tenant, "publisher"))
    assert (await api.post(path, headers=publisher)).status_code == 202
    in_progress = await api.post(path, headers=publisher)
    assert in_progress.status_code == 409
    assert in_progress.json()["error"]["code"] == "delivery_not_replayable"
    assert await _val(db_url, "SELECT count(*) FROM replay_requests") == 1


async def test_idempotency_key_replays_the_response_without_replaying_twice(
    api: httpx.AsyncClient, db_url: str, dead_backlog: Any
) -> None:
    tenant, _, _, _ = await dead_backlog()
    [delivery_id] = await _delivery_ids(db_url)
    headers = {**tenant.headers, "Idempotency-Key": "replay-1"}
    first = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=headers)
    assert first.status_code == 202
    assert "Idempotent-Replayed" not in first.headers
    second = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=headers)
    assert second.status_code == 202
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()
    assert await _val(db_url, "SELECT replay_count FROM deliveries") == 1
    assert await _val(db_url, "SELECT count(*) FROM replay_requests") == 1

    # Same key, different request: rejected rather than silently replayed.
    mismatch = await api.post(f"/v1/deliveries/{uuid.uuid4()}/replay", headers=headers)
    assert mismatch.status_code == 422
    assert mismatch.json()["error"]["code"] == "idempotency_key_mismatch"


# -- by event --------------------------------------------------------------------------------


async def test_event_replay_selects_by_status_and_endpoint(
    api: httpx.AsyncClient, db_url: str, dead_backlog: Any
) -> None:
    tenant, endpoint_id, [event_id], _ = await dead_backlog()
    path = f"/v1/events/{event_id}/replay"
    only_failed = await api.post(path, json={"statuses": ["failed"]}, headers=tenant.headers)
    assert only_failed.status_code == 202, only_failed.text
    assert only_failed.json()["replayed"] == 0  # nothing failed; the delivery is dead
    other_endpoint = await api.post(
        path, json={"endpoint_id": str(uuid.uuid4())}, headers=tenant.headers
    )
    assert other_endpoint.json()["replayed"] == 0

    default = await api.post(path, json={"endpoint_id": endpoint_id}, headers=tenant.headers)
    assert default.status_code == 202
    out = default.json()
    assert (out["scope"], out["replayed"], out["has_more"]) == ("event", 1, False)
    assert out["filters"]["event_id"] == event_id
    assert out["filters"]["statuses"] == ["dead", "failed"]
    assert await _val(db_url, "SELECT status FROM deliveries") == "pending"

    bad = await api.post(path, json={"statuses": ["pending"]}, headers=tenant.headers)
    assert bad.status_code == 422
    other = await create_tenant(api)
    missing = await api.post(path, headers=other.headers)
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "event_not_found"


# -- bulk ------------------------------------------------------------------------------------


async def test_bulk_replay_is_admin_only_windowed_and_capped(
    api: httpx.AsyncClient, db_url: str, dead_backlog: Any, limited_api: Any
) -> None:
    tenant, endpoint_id, _, _ = await dead_backlog(events=3)
    capped = await limited_api(replay_max_deliveries=2, replay_max_window_s=7200)
    publisher = bearer(await issue_key(api, tenant, "publisher"))
    assert (await capped.post("/v1/replays", json=_window(), headers=publisher)).status_code == 403

    too_wide = {
        "created_after": (datetime.now(UTC) - timedelta(hours=5)).isoformat(),
        "created_before": datetime.now(UTC).isoformat(),
    }
    wide = await capped.post("/v1/replays", json=too_wide, headers=tenant.headers)
    assert wide.status_code == 422
    assert wide.json()["error"]["code"] == "replay_window_too_large"
    backwards = {
        "created_after": too_wide["created_before"],
        "created_before": too_wide["created_after"],
    }
    inverted = await capped.post("/v1/replays", json=backwards, headers=tenant.headers)
    assert inverted.json()["error"]["code"] == "invalid_replay_window"
    naive = await capped.post(
        "/v1/replays",
        json={"created_after": "2026-11-03T00:00:00", "created_before": "2026-11-03T01:00:00"},
        headers=tenant.headers,
    )
    assert naive.status_code == 422
    assert (await capped.post("/v1/replays", json={}, headers=tenant.headers)).status_code == 422
    wrong_type = await capped.post(
        "/v1/replays", json={**_window(), "event_type": "order.created"}, headers=tenant.headers
    )
    assert wrong_type.json()["replayed"] == 0

    first = await capped.post(
        "/v1/replays",
        json={**_window(), "endpoint_id": endpoint_id, "event_type": "invoice.paid"},
        headers=tenant.headers,
    )
    assert first.status_code == 202, first.text
    assert (first.json()["replayed"], first.json()["has_more"]) == (2, True)
    second = await capped.post("/v1/replays", json=_window(), headers=tenant.headers)
    assert (second.json()["replayed"], second.json()["has_more"]) == (1, False)
    statuses = await _fetch(db_url, "SELECT DISTINCT status FROM deliveries")
    assert [r["status"] for r in statuses] == ["pending"]

    listing = await capped.get("/v1/replays?limit=2", headers=publisher)
    assert listing.status_code == 200
    page = listing.json()
    assert [r["scope"] for r in page["data"]] == ["filter", "filter"]
    assert page["has_more"] is True
    rest = await capped.get(f"/v1/replays?cursor={page['next_cursor']}", headers=publisher)
    assert len(rest.json()["data"]) == 1
    other = await create_tenant(api)
    assert (await capped.get("/v1/replays", headers=other.headers)).json()["data"] == []


async def test_replays_are_rate_limited_per_tenant(
    api: httpx.AsyncClient, db_url: str, dead_backlog: Any, limited_api: Any
) -> None:
    tenant, _, [event_id], worker = await dead_backlog()
    limited = await limited_api(replay_rate_limit_per_minute=2)
    path = f"/v1/events/{event_id}/replay"
    for _ in range(2):
        assert (await limited.post(path, headers=tenant.headers)).status_code == 202
    throttled = await limited.post(path, headers=tenant.headers)
    assert throttled.status_code == 429
    assert throttled.json()["error"]["code"] == "replay_rate_limited"
    assert 1 <= int(throttled.headers["Retry-After"]) <= 60
    # Other tenants have their own budget.
    other = await create_tenant(api)
    assert (
        await limited.post("/v1/replays", json=_window(), headers=other.headers)
    ).status_code == 202

    # The window slides: old audit rows stop counting (and housekeeping purges them).
    await _run(db_url, "UPDATE replay_requests SET created_at = now() - interval '2 days'")
    assert (await limited.post(path, headers=tenant.headers)).status_code == 202
    async with worker.database.session() as session:
        result = await housekeeping.sweep(
            session, outbox_retention_s=3600, batch_size=100, replay_retention_s=86400
        )
    assert result.replay_requests == 3
    assert await _val(db_url, "SELECT count(*) FROM replay_requests") == 1
