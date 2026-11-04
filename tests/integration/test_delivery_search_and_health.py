"""Milestone 5 tenant read APIs on real Postgres: delivery log search (filters, combinations,
keyset pagination, tenant isolation, validation, index use) and the endpoint health summary
(counts, percentiles, sampling cap, last error, breaker, backlog, windows).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx

from hookrelay.db.engine import Database
from hookrelay.relay import Relay
from hookrelay.services.endpoint_health import HealthPolicy, get_endpoint_health
from tests.integration.conftest import (
    _asyncpg_dsn,
    api_client,
    bearer,
    create_endpoint,
    create_tenant,
    issue_key,
    publish,
    register_event_type,
    subscribe,
)
from tests.integration.test_worker import (  # noqa: F401 - pytest fixtures
    _fetch,
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

# -- search ---------------------------------------------------------------------------------


async def _fan_out(settings: Any) -> None:
    db = Database.from_settings(settings)
    try:
        relay = Relay(db, settings)
        while (await relay.run_once()).claimed:
            pass
    finally:
        await db.dispose()


async def _seed(api: httpx.AsyncClient, settings: Any) -> dict[str, Any]:
    """Tenant with endpoints A (invoice.*) and B (invoice.* + user.*): 3 invoice events and 2
    user events -> 8 deliveries, then statuses and created_at spread out deterministically."""
    tenant = await create_tenant(api)
    for name in ("invoice.paid", "user.created"):
        await register_event_type(api, tenant, name)
    a = await create_endpoint(api, tenant, "a")
    b = await create_endpoint(api, tenant, "b")
    await subscribe(api, tenant, a, "invoice.*")
    await subscribe(api, tenant, b, "invoice.*")
    await subscribe(api, tenant, b, "user.*")
    for i in range(3):
        assert (await publish(api, tenant, "invoice.paid", {"i": i})).status_code == 202
    for i in range(2):
        assert (await publish(api, tenant, "user.created", {"i": i})).status_code == 202
    await _fan_out(settings)
    return {"tenant": tenant, "a": a, "b": b}


async def _spread(db_url: str) -> list[asyncpg.Record]:
    """Give each delivery a distinct created_at (hours ago, newest first by id order) and
    statuses: rows 0,3 failed; 1 dead; 2,5 succeeded; others pending."""
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        rows = await conn.fetch("SELECT id FROM deliveries ORDER BY id")
        statuses = ["failed", "dead", "succeeded", "failed", "pending", "succeeded"]
        for i, row in enumerate(rows):
            status = statuses[i] if i < len(statuses) else "pending"
            await conn.execute(
                "UPDATE deliveries SET created_at = now() - make_interval(hours => $2), "
                "status = $3::text, completed_at = "
                "CASE WHEN $3::text IN ('succeeded','failed','dead') THEN now() END WHERE id = $1",
                row["id"],
                i + 1,
                status,
            )
        return list(
            await conn.fetch(
                "SELECT id, status, event_type, endpoint_id, created_at FROM deliveries "
                "ORDER BY created_at DESC, id DESC"
            )
        )
    finally:
        await conn.close()


async def _search(api: httpx.AsyncClient, headers: dict[str, str], **params: Any) -> Any:
    response = await api.get("/v1/deliveries", params=params, headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


def _ids(rows: list[asyncpg.Record]) -> list[str]:
    return [str(r["id"]) for r in rows]


async def test_search_filters_and_combinations(
    api: httpx.AsyncClient, settings_for: Any, db_url: str
) -> None:
    seeded = await _seed(api, settings_for())
    rows = await _spread(db_url)
    assert len(rows) == 8
    headers = seeded["tenant"].headers

    everything = await _search(api, headers)
    assert [d["id"] for d in everything["data"]] == _ids(rows)  # newest first
    assert everything["has_more"] is False
    assert {"event_type", "status", "endpoint_id"} <= set(everything["data"][0])

    failed = await _search(api, headers, status="failed")
    assert [d["id"] for d in failed["data"]] == _ids([r for r in rows if r["status"] == "failed"])

    several = await _search(api, headers, status=["failed", "dead"])
    expected = [r for r in rows if r["status"] in ("failed", "dead")]
    assert [d["id"] for d in several["data"]] == _ids(expected)

    users = await _search(api, headers, event_type="user.created")
    assert [d["id"] for d in users["data"]] == _ids(
        [r for r in rows if r["event_type"] == "user.created"]
    )
    assert all(d["event_type"] == "user.created" for d in users["data"])
    assert len(users["data"]) == 2

    on_a = await _search(api, headers, endpoint_id=seeded["a"])
    assert [d["id"] for d in on_a["data"]] == _ids(
        [r for r in rows if str(r["endpoint_id"]) == seeded["a"]]
    )
    assert len(on_a["data"]) == 3

    after = rows[4]["created_at"]
    before = rows[1]["created_at"]
    window = await _search(
        api, headers, created_after=after.isoformat(), created_before=before.isoformat()
    )
    # created_after inclusive, created_before exclusive.
    assert [d["id"] for d in window["data"]] == _ids(rows[2:5])

    combo = await _search(
        api,
        headers,
        endpoint_id=seeded["b"],
        event_type="invoice.paid",
        status=["pending", "succeeded", "failed", "dead"],
        created_before=datetime.now(UTC).isoformat(),
    )
    assert [d["id"] for d in combo["data"]] == _ids(
        [
            r
            for r in rows
            if str(r["endpoint_id"]) == seeded["b"] and r["event_type"] == "invoice.paid"
        ]
    )

    nothing = await _search(api, headers, event_type="order.shipped")
    assert nothing == {"data": [], "next_cursor": None, "has_more": False}


async def test_search_pagination_is_stable_under_new_writes(
    api: httpx.AsyncClient, settings_for: Any, db_url: str
) -> None:
    seeded = await _seed(api, settings_for())
    rows = await _spread(db_url)
    headers = seeded["tenant"].headers
    seen: list[str] = []
    page = await _search(api, headers, limit=3)
    seen += [d["id"] for d in page["data"]]
    # A new delivery arrives between pages: it is newer, so it never shifts later pages.
    assert (await publish(api, seeded["tenant"], "user.created", {"late": True})).status_code == 202
    await _fan_out(settings_for())
    while page["has_more"]:
        page = await _search(api, headers, limit=3, cursor=page["next_cursor"])
        seen += [d["id"] for d in page["data"]]
    assert seen == _ids(rows)

    # Filters carry over the cursor (the client repeats them); a filtered walk.
    filtered: list[str] = []
    page = await _search(api, headers, limit=1, status="pending")
    filtered += [d["id"] for d in page["data"]]
    while page["has_more"]:
        page = await _search(api, headers, limit=1, status="pending", cursor=page["next_cursor"])
        filtered += [d["id"] for d in page["data"]]
    pending_now = await _fetch(
        db_url,
        "SELECT id FROM deliveries WHERE status = 'pending' ORDER BY created_at DESC, id DESC",
    )
    assert filtered == _ids(pending_now)


async def test_search_is_tenant_scoped_and_reader_accessible(
    api: httpx.AsyncClient, settings_for: Any, db_url: str
) -> None:
    seeded = await _seed(api, settings_for())
    other = await create_tenant(api)
    assert (await _search(api, other.headers))["data"] == []
    # Another tenant's endpoint id as a filter matches nothing (no 404 probe either).
    assert (await _search(api, other.headers, endpoint_id=seeded["a"]))["data"] == []
    reader = bearer(await issue_key(api, seeded["tenant"], "reader"))
    assert len((await _search(api, reader))["data"]) == 8
    assert (await api.get("/v1/deliveries")).status_code == 401


async def test_search_validation(api: httpx.AsyncClient, settings_for: Any) -> None:
    tenant = await create_tenant(api)
    now = datetime.now(UTC)

    async def get(**params: Any) -> httpx.Response:
        return await api.get("/v1/deliveries", params=params, headers=tenant.headers)

    inverted = await get(created_after=now.isoformat(), created_before=now.isoformat())
    assert inverted.status_code == 422
    assert inverted.json()["error"]["code"] == "invalid_time_range"
    naive = await get(created_after="2026-11-04T10:00:00")
    assert naive.status_code == 422
    assert (await get(status="bogus")).status_code == 422
    assert (await get(status=["failed"] * 6)).status_code == 422
    assert (await get(event_type="Bad Type!")).status_code == 422
    assert (await get(endpoint_id="not-a-uuid")).status_code == 422
    assert (await get(limit=0)).status_code == 422
    assert (await get(cursor="garbage")).status_code == 400
    ok = await get(
        created_after=(now - timedelta(days=1)).isoformat(), created_before=now.isoformat()
    )
    assert ok.status_code == 200


async def test_search_queries_use_the_new_indexes(
    api: httpx.AsyncClient, settings_for: Any, db_url: str
) -> None:
    """Every filter has an index ending in (created_at, id): EXPLAIN (seq scans disabled) shows
    an index scan and no Sort, and the indexes have the expected keys (guards migration 0009)."""
    seeded = await _seed(api, settings_for())
    tenant_id = await _val(db_url, "SELECT tenant_id FROM deliveries LIMIT 1")
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        defs = {
            r["indexname"]: r["indexdef"]
            for r in await conn.fetch("SELECT indexname, indexdef FROM pg_indexes")
        }
        assert "(tenant_id, status, created_at, id)" in defs["ix_deliveries_tenant_status_created"]
        assert (
            "(tenant_id, event_type, created_at, id)" in defs["ix_deliveries_tenant_type_created"]
        )
        assert "(endpoint_id, created_at, id)" in defs["ix_deliveries_endpoint_created"]
        await conn.execute("SET enable_seqscan = off")
        for sql, args in (
            (
                "SELECT id FROM deliveries WHERE tenant_id = $1 AND status = 'failed' "
                "ORDER BY created_at DESC, id DESC LIMIT 10",
                (tenant_id,),
            ),
            (
                "SELECT id FROM deliveries WHERE tenant_id = $1 AND event_type = 'user.created' "
                "ORDER BY created_at DESC, id DESC LIMIT 10",
                (tenant_id,),
            ),
            (
                "SELECT id FROM deliveries WHERE tenant_id = $1 AND endpoint_id = $2 "
                "ORDER BY created_at DESC, id DESC LIMIT 10",
                (tenant_id, uuid.UUID(seeded["a"])),
            ),
        ):
            plan = "\n".join(r[0] for r in await conn.fetch(f"EXPLAIN {sql}", *args))
            assert "Index" in plan, plan
            assert "Sort" not in plan, plan
    finally:
        await conn.close()


# -- health ---------------------------------------------------------------------------------


async def _insert_attempts(
    db_url: str,
    delivery_id: uuid.UUID,
    tenant_id: uuid.UUID,
    specs: list[tuple[str, int | None, int | None, str | None, float]],
    *,
    first_number: int = 1,
) -> None:
    """specs: (outcome, status_code, latency_ms, error_class, seconds_ago)."""
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        await conn.executemany(
            "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
            "status_code, latency_ms, error_class, error_message, request_url, worker_id, "
            "started_at, finished_at, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, "
            "'http://x.test/', 'w', now() - make_interval(secs => $9), "
            "now() - make_interval(secs => $9), now() - make_interval(secs => $9))",
            [
                (
                    tenant_id,
                    delivery_id,
                    first_number + i,
                    outcome,
                    code,
                    latency,
                    err,
                    None if err is None else f"{err} happened",
                    ago,
                )
                for i, (outcome, code, latency, err, ago) in enumerate(specs)
            ],
        )
    finally:
        await conn.close()


async def _health(
    api: httpx.AsyncClient, headers: dict[str, str], endpoint_id: str, **params: Any
) -> httpx.Response:
    return await api.get(f"/v1/endpoints/{endpoint_id}/health", params=params, headers=headers)


async def test_health_with_no_attempts(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver, events=2)
    response = await _health(api, tenant.headers, endpoint_id)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["attempts"] == 0
    assert body["success_rate"] is None
    assert body["latency_p50_ms"] is None
    assert body["latency_p95_ms"] is None
    assert body["errors_by_class"] == {}
    assert body["last_error"] is None
    assert body["last_success_at"] is None
    assert body["sampled"] is False
    assert body["sample_started_at"] is None
    assert body["backlog"] == 2
    assert body["backlog_capped"] is False
    assert body["circuit"]["state"] == "closed"
    assert body["window_s"] == 86400
    assert body["enabled"] is True


async def test_health_counts_percentiles_and_last_error(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, db_url: str
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    [delivery] = await _fetch(db_url, "SELECT id, tenant_id FROM deliveries")
    specs: list[tuple[str, int | None, int | None, str | None, float]] = [
        ("succeeded", 200, 10 * (i + 1), None, 600 - i)
        for i in range(10)  # 10..100 ms
    ]
    specs += [
        ("retrying", 503, 300, "http_status", 120),
        ("retrying", None, None, "timeout", 60),
        ("failed", 410, 5, "http_status", 30),
        ("succeeded", 200, 50, None, 90_000),  # outside the default 24h window
    ]
    await _insert_attempts(db_url, delivery["id"], delivery["tenant_id"], specs)

    body = (await _health(api, tenant.headers, endpoint_id)).json()
    assert (body["attempts"], body["succeeded"], body["retrying"], body["failed"]) == (13, 10, 2, 1)
    assert body["success_rate"] == round(10 / 13, 4)
    # latencies: 10..100, 300, 5 (the timeout has none) -> 12 values
    values = sorted([10 * (i + 1) for i in range(10)] + [300, 5])
    assert body["latency_p50_ms"] == round((values[5] + values[6]) / 2, 1)
    assert body["latency_p95_ms"] > body["latency_p50_ms"]
    assert body["latency_p99_ms"] >= body["latency_p95_ms"]
    assert body["errors_by_class"] == {"http_status": 2, "timeout": 1}
    assert body["last_error"]["error_class"] == "http_status"
    assert body["last_error"]["status_code"] == 410
    assert body["last_error"]["delivery_id"] == str(delivery["id"])
    assert body["last_error"]["error_message"] == "http_status happened"
    assert body["last_success_at"] is not None
    assert body["sampled"] is False

    # A wider window includes the old success.
    wide = (await _health(api, tenant.headers, endpoint_id, window_s=2 * 86400)).json()
    assert wide["attempts"] == 14
    assert wide["succeeded"] == 11
    # A narrow window with no success still reports the last success from any time.
    narrow = (await _health(api, tenant.headers, endpoint_id, window_s=100)).json()
    assert (narrow["attempts"], narrow["succeeded"]) == (2, 0)
    assert narrow["success_rate"] == 0.0
    assert narrow["last_success_at"] == body["last_success_at"]


async def test_health_sampling_caps_the_scan(
    db_url: str, settings_for: Any, receiver: Receiver, api: httpx.AsyncClient
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver)
    [delivery] = await _fetch(db_url, "SELECT id, tenant_id FROM deliveries")
    # 150 attempts: the oldest 50 failed, the newest 100 succeeded.
    specs: list[tuple[str, int | None, int | None, str | None, float]] = [
        ("failed", 500, 1, "http_status", 1000 - i) for i in range(50)
    ] + [("succeeded", 200, 1, None, 500 - i) for i in range(100)]
    await _insert_attempts(db_url, delivery["id"], delivery["tenant_id"], specs)
    s = settings_for()
    async with api_client(
        db_url,
        endpoint_allow_http=s.endpoint_allow_http,
        endpoint_allow_private_networks=s.endpoint_allow_private_networks,
        endpoint_health_max_attempts=100,
    ) as capped:
        body = (await _health(capped, tenant.headers, endpoint_id)).json()
    assert body["sampled"] is True
    assert body["attempts"] == 100
    assert body["success_rate"] == 1.0
    assert body["errors_by_class"] == {}
    assert body["sample_started_at"] > body["window_started_at"]
    # last_error is all-time, not limited to the sample.
    assert body["last_error"]["error_class"] == "http_status"

    full = (await _health(api, tenant.headers, endpoint_id)).json()
    assert full["sampled"] is False
    assert full["attempts"] == 150


async def test_health_reflects_real_deliveries_and_the_breaker(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    receiver.handler = lambda _r: Reply(500)
    settings = settings_for(breaker_failure_threshold=2)
    tenant, endpoint_id, _, _ = await _setup(api, settings, receiver, events=3)
    worker = make_worker(breaker_failure_threshold=2)
    await worker.run_once()
    body = (await _health(api, tenant.headers, endpoint_id)).json()
    assert body["attempts"] >= 2
    assert body["succeeded"] == 0
    assert body["success_rate"] == 0.0
    assert body["errors_by_class"] == {"http_status": body["attempts"]}
    assert body["last_error"]["status_code"] == 500
    assert body["circuit"]["state"] == "open"
    assert body["circuit"]["consecutive_failures"] >= 2
    assert body["backlog"] == 3  # retrying deliveries are pending again
    assert body["latency_p50_ms"] is not None


async def test_health_backlog_cap_and_validation(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, db_url: str
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver, events=3)
    too_wide = await _health(api, tenant.headers, endpoint_id, window_s=8 * 86400)
    assert too_wide.status_code == 422
    assert too_wide.json()["error"]["code"] == "window_too_large"
    assert (await _health(api, tenant.headers, endpoint_id, window_s=59)).status_code == 422
    other = await create_tenant(api)
    assert (await _health(api, other.headers, endpoint_id)).status_code == 404
    assert (await _health(api, tenant.headers, str(uuid.uuid4()))).status_code == 404
    reader = bearer(await issue_key(api, tenant, "reader"))
    assert (await _health(api, reader, endpoint_id)).status_code == 200
    assert (await api.get(f"/v1/endpoints/{endpoint_id}/health")).status_code == 401

    db = Database.from_settings(settings_for())
    try:
        async with db.session() as session:
            health = await get_endpoint_health(
                session,
                uuid.UUID(tenant.id),
                uuid.UUID(endpoint_id),
                policy=HealthPolicy(
                    default_window_s=3600,
                    max_window_s=3600,
                    max_attempts=100,
                    default_max_in_flight=10,
                    backlog_cap=2,
                ),
            )
    finally:
        await db.dispose()
    assert health.backlog == 2
    assert health.backlog_capped is True
    assert health.window_s == 3600
