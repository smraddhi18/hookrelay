"""The outbox relay: matching, exactly-once fan-out under concurrency, crash safety, poison
rows, the polling loop, housekeeping and the CLI. Each test gets its own migrated database
(cloned from a template) because the relay drains every tenant's outbox."""

from __future__ import annotations

import asyncio
import os
import signal
import time
import uuid
from collections.abc import AsyncIterator, Iterator, Sequence
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from hookrelay import relay as relay_module
from hookrelay.core.config import Settings
from hookrelay.db.engine import Database
from hookrelay.relay import Relay, heartbeat_is_fresh, main, serve
from hookrelay.services import fanout
from hookrelay.services.subscriptions import pattern_matches
from tests.integration.conftest import (
    TenantCtx,
    _admin,
    _asyncpg_dsn,
    _url_for,
    alembic_config,
    api_client,
    api_settings,
    create_endpoint,
    create_scratch_database,
    create_tenant,
    drop_scratch_database,
    publish,
    register_event_type,
    subscribe,
)


@pytest.fixture(scope="module")
def relay_template() -> Iterator[str]:
    name, url = create_scratch_database("hookrelay_rtpl")
    try:
        command.upgrade(alembic_config(url), "head")
        yield name
    finally:
        drop_scratch_database(name)


@pytest.fixture
def db_url(relay_template: str) -> Iterator[str]:
    name = f"hookrelay_relay_{uuid.uuid4().hex[:10]}"
    asyncio.run(_admin(f'CREATE DATABASE "{name}" TEMPLATE "{relay_template}"'))
    try:
        yield _url_for(name)
    finally:
        drop_scratch_database(name)


@pytest.fixture
def settings_for(db_url: str, tmp_path: Path) -> Any:
    def _make(**overrides: Any) -> Settings:
        base: dict[str, Any] = {
            "relay_heartbeat_file": str(tmp_path / "relay.heartbeat"),
            "relay_poll_min_ms": 10,
            "relay_poll_max_ms": 40,
        }
        base.update(overrides)
        return api_settings(db_url, **base)

    return _make


@pytest.fixture
async def api(db_url: str) -> AsyncIterator[httpx.AsyncClient]:
    async with api_client(db_url) as client:
        yield client


@pytest.fixture
async def database(settings_for: Any) -> AsyncIterator[Database]:
    db = Database.from_settings(settings_for())
    try:
        yield db
    finally:
        await db.dispose()


async def _fetch(url: str, sql: str, *args: object) -> list[asyncpg.Record]:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        return list(await conn.fetch(sql, *args))
    finally:
        await conn.close()


async def _val(url: str, sql: str, *args: object) -> Any:
    rows = await _fetch(url, sql, *args)
    return rows[0][0]


async def _setup_simple(
    api: httpx.AsyncClient, endpoints: int = 2, event_type: str = "order.placed"
) -> tuple[TenantCtx, list[str]]:
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, event_type)
    ids = []
    for i in range(endpoints):
        endpoint_id = await create_endpoint(api, tenant, f"e{i}")
        await subscribe(api, tenant, endpoint_id, "*")
        ids.append(endpoint_id)
    return tenant, ids


async def _publish_many(
    api: httpx.AsyncClient, tenant: TenantCtx, n: int, event_type: str = "order.placed"
) -> list[str]:
    ids = []
    for i in range(n):
        response = await publish(api, tenant, event_type, {"i": i})
        assert response.status_code == 202, response.text
        ids.append(response.json()["id"])
    return ids


async def _drain(relay: Relay) -> int:
    rounds = 0
    while (await relay.run_once()).claimed:
        rounds += 1
    return rounds


async def test_fan_out_follows_subscriptions(
    api: httpx.AsyncClient, database: Database, settings_for: Any, db_url: str
) -> None:
    tenant = await create_tenant(api)
    names = ["invoice.paid", "invoice.paid.partial", "invoice.created", "user.created", "user"]
    for name in names:
        await register_event_type(api, tenant, name)
    chosen_patterns = [
        ["*"],
        ["invoice.*"],
        ["invoice.paid", "invoice.paid.*"],
        ["user"],
        ["user.*", "invoice.paid"],
    ]
    endpoints: dict[str, list[str]] = {}
    for i, patterns in enumerate(chosen_patterns):
        endpoint_id = await create_endpoint(api, tenant, f"s{i}")
        for pattern in patterns:
            await subscribe(api, tenant, endpoint_id, pattern)
        endpoints[endpoint_id] = patterns
    disabled = await create_endpoint(api, tenant, "disabled")
    await subscribe(api, tenant, disabled, "*")
    await api.patch(
        f"/v1/endpoints/{disabled}",
        json={"enabled": False},
        headers={**tenant.headers, "If-Match": "*"},
    )
    deleted = await create_endpoint(api, tenant, "deleted")
    await subscribe(api, tenant, deleted, "*")
    assert (await api.delete(f"/v1/endpoints/{deleted}", headers=tenant.headers)).status_code == 204
    other = await create_tenant(api)
    await register_event_type(api, other, "invoice.paid")
    await subscribe(api, other, await create_endpoint(api, other, "other"), "*")

    published = {name: (await publish(api, tenant, name)).json()["id"] for name in names}
    other_event = (await publish(api, other, "invoice.paid")).json()["id"]

    relay = Relay(database, settings_for())
    result = await relay.run_once()
    assert result.claimed == result.processed == len(names) + 1
    assert result.failed == 0

    for name, event_id in published.items():
        response = await api.get(f"/v1/events/{event_id}/deliveries", headers=tenant.headers)
        got = {d["endpoint_id"] for d in response.json()["data"]}
        expected = {
            endpoint_id
            for endpoint_id, patterns in endpoints.items()
            if any(pattern_matches(p, name) for p in patterns)
        }
        assert got == expected, name
        for delivery in response.json()["data"]:
            assert delivery["status"] == "pending"
            assert delivery["attempt_count"] == 0
            assert delivery["event_id"] == event_id
        event = (await api.get(f"/v1/events/{event_id}", headers=tenant.headers)).json()
        assert event["fanned_out_at"] is not None
    other_deliveries = await api.get(f"/v1/events/{other_event}/deliveries", headers=other.headers)
    assert len(other_deliveries.json()["data"]) == 1
    assert result.deliveries_created == sum(
        len(d) for d in [await _fetch(db_url, "SELECT 1 FROM deliveries")]
    )
    # Deliveries inherit the event's retention horizon.
    mismatched = await _val(
        db_url,
        "SELECT count(*) FROM deliveries d JOIN events e ON e.id = d.event_id "
        "WHERE d.retain_until <> e.retain_until",
    )
    assert mismatched == 0
    assert relay.stats.outbox_rows_processed == len(names) + 1
    assert (await relay.run_once()).claimed == 0


async def test_events_without_subscribers_are_marked_processed(
    api: httpx.AsyncClient, database: Database, settings_for: Any, db_url: str
) -> None:
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, "lonely.event")
    await _publish_many(api, tenant, 3, "lonely.event")
    result = await Relay(database, settings_for()).run_once()
    assert (result.processed, result.deliveries_created) == (3, 0)
    pending = await _val(db_url, "SELECT count(*) FROM event_outbox WHERE processed_at IS NULL")
    assert pending == 0


async def test_concurrent_relays_fan_out_exactly_once(
    api: httpx.AsyncClient, settings_for: Any, db_url: str
) -> None:
    tenant, endpoints = await _setup_simple(api, endpoints=3)
    events = await _publish_many(api, tenant, 60)
    settings = settings_for(relay_batch_size=7)
    databases = [Database.from_settings(settings) for _ in range(4)]
    try:
        relays = [Relay(db, settings) for db in databases]
        await asyncio.gather(*(_drain(r) for r in relays))
    finally:
        for db in databases:
            await db.dispose()
    assert sum(r.stats.outbox_rows_processed for r in relays) == len(events)
    assert sum(r.stats.deliveries_created for r in relays) == len(events) * len(endpoints)
    assert sum(1 for r in relays if r.stats.outbox_rows_processed) >= 2  # work really was shared
    rows = await _fetch(db_url, "SELECT event_id, endpoint_id FROM deliveries")
    assert len(rows) == len({(r[0], r[1]) for r in rows}) == len(events) * len(endpoints)
    assert await _val(db_url, "SELECT count(*) FROM event_outbox WHERE processed_at IS NULL") == 0


async def test_skip_locked_gives_disjoint_batches(
    api: httpx.AsyncClient, database: Database, settings_for: Any
) -> None:
    tenant, _ = await _setup_simple(api, endpoints=1)
    await _publish_many(api, tenant, 4)
    async with database.session() as held:
        first = await fanout.claim_batch(held, batch_size=2)
        relay = Relay(database, settings_for())
        result = await relay.run_once()  # runs while `held` still locks the first two rows
        assert result.claimed == 2
        assert (await relay.run_once()).claimed == 0
        assert len(first) == 2
        await held.rollback()
    assert (await relay.run_once()).claimed == 2


async def test_crash_mid_batch_leaves_no_partial_fan_out(
    api: httpx.AsyncClient, database: Database, settings_for: Any, db_url: str
) -> None:
    tenant, endpoints = await _setup_simple(api, endpoints=2)
    events = await _publish_many(api, tenant, 5)

    class CrashError(Exception):
        pass

    async def crash_after_fan_out() -> None:
        async with database.session() as session:
            rows = await fanout.claim_batch(session, batch_size=100)
            await fanout.fan_out(session, [r.event_id for r in rows])
            raise CrashError  # the process dies before mark_processed + commit

    with pytest.raises(CrashError):
        await crash_after_fan_out()
    assert await _val(db_url, "SELECT count(*) FROM deliveries") == 0

    relay = Relay(database, settings_for())
    await _drain(relay)
    assert await _val(db_url, "SELECT count(*) FROM deliveries") == len(events) * len(endpoints)

    # A crash *after* commit but before the relay "knew" (or an operator replay) re-delivers the
    # outbox rows: the unique (event, endpoint) constraint makes that a no-op.
    await _fetch(db_url, "UPDATE event_outbox SET processed_at = NULL")
    replay = await relay.run_once()
    assert (replay.processed, replay.deliveries_created) == (len(events), 0)
    assert await _val(db_url, "SELECT count(*) FROM deliveries") == len(events) * len(endpoints)


async def test_poison_row_is_parked_with_backoff(
    api: httpx.AsyncClient,
    database: Database,
    settings_for: Any,
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _ = await _setup_simple(api, endpoints=1)
    events = await _publish_many(api, tenant, 4)
    poison = uuid.UUID(events[1])
    real_fan_out = fanout.fan_out

    async def flaky(session: Any, event_ids: Sequence[uuid.UUID]) -> int:
        if poison in event_ids:
            # A real database error, so the savepoint is genuinely aborted and rolled back.
            await session.execute(text("SELECT 1/0"))
        return await real_fan_out(session, event_ids)

    monkeypatch.setattr(fanout, "fan_out", flaky)
    relay = Relay(database, settings_for(relay_retry_max_s=60))
    result = await relay.run_once()
    assert (result.claimed, result.processed, result.failed) == (4, 3, 1)
    assert relay.stats.batch_fallbacks == 1
    row = (
        await _fetch(
            db_url,
            "SELECT attempts, last_error, processed_at, available_at > now() AS later "
            "FROM event_outbox WHERE event_id = $1",
            poison,
        )
    )[0]
    assert row["attempts"] == 1
    assert row["processed_at"] is None
    assert row["later"] is True
    assert "DivisionByZero" in row["last_error"] or "division by zero" in row["last_error"]
    assert await _val(db_url, "SELECT count(*) FROM deliveries") == 3
    assert (await relay.run_once()).claimed == 0  # parked until available_at

    # Once due again and healthy, it goes through and clears the error.
    monkeypatch.setattr(fanout, "fan_out", real_fan_out)
    await _fetch(db_url, "UPDATE event_outbox SET available_at = now() WHERE event_id = $1", poison)
    retried = await relay.run_once()
    assert (retried.processed, retried.deliveries_created) == (1, 1)
    row = (
        await _fetch(
            db_url, "SELECT attempts, last_error FROM event_outbox WHERE event_id = $1", poison
        )
    )[0]
    assert (row["attempts"], row["last_error"]) == (2, None)


async def test_run_loop_processes_until_stopped(
    api: httpx.AsyncClient, database: Database, settings_for: Any, db_url: str
) -> None:
    tenant, _ = await _setup_simple(api, endpoints=1)
    settings = settings_for(relay_batch_size=2)
    relay = Relay(database, settings)
    task = asyncio.create_task(relay.run())
    await _publish_many(api, tenant, 5)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if await _val(db_url, "SELECT count(*) FROM deliveries") == 5:
            break
        await asyncio.sleep(0.02)
    relay.stop()
    relay.stop()  # idempotent
    await asyncio.wait_for(task, timeout=5)
    assert relay.stats.outbox_rows_processed == 5
    assert relay.stats.housekeeping_runs >= 1
    assert heartbeat_is_fresh(settings.relay_heartbeat_file, 5)


async def test_sigterm_stops_the_loop(database: Database, settings_for: Any) -> None:
    relay = Relay(database, settings_for())
    task = asyncio.create_task(relay.run())
    await asyncio.sleep(0.1)
    os.kill(os.getpid(), signal.SIGTERM)
    await asyncio.wait_for(task, timeout=5)
    assert relay.stopping
    # Handlers are removed again, so a later SIGTERM would not be swallowed by a dead relay.
    assert signal.getsignal(signal.SIGTERM) in {signal.SIG_DFL, signal.default_int_handler}


async def test_loop_survives_iteration_errors(
    database: Database, settings_for: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    relay = Relay(database, settings_for(relay_heartbeat_file="/nonexistent-dir/heartbeat"))
    calls = 0

    async def broken() -> Any:
        nonlocal calls
        calls += 1
        if calls >= 3:
            relay.stop()
        raise DBAPIError("SELECT", {}, OSError("connection reset"))

    async def broken_housekeeping() -> Any:
        raise RuntimeError("sweep failed")

    monkeypatch.setattr(relay, "run_once", broken)
    monkeypatch.setattr(relay, "housekeep", broken_housekeeping)
    await asyncio.wait_for(relay.run(), timeout=5)
    assert relay.stats.loop_errors >= 3
    assert relay._heartbeat_warned is True


async def test_housekeeping_purges_expired_rows(
    api: httpx.AsyncClient, database: Database, settings_for: Any, db_url: str
) -> None:
    tenant, _ = await _setup_simple(api, endpoints=2)
    keep = (await publish(api, tenant, "order.placed", {"keep": True}, key="live")).json()["id"]
    doomed = (await publish(api, tenant, "order.placed", {"old": True}, key="old")).json()["id"]
    relay = Relay(database, settings_for(relay_outbox_retention_s=0))
    await _drain(relay)
    await _fetch(
        db_url,
        "UPDATE idempotency_keys SET created_at = now() - interval '3 days', "
        "expires_at = now() - interval '1 day' WHERE key = 'old'",
    )
    await _fetch(
        db_url,
        "UPDATE events SET created_at = now() - interval '40 days', "
        "retain_until = now() - interval '10 days' WHERE id = $1",
        uuid.UUID(doomed),
    )
    swept = await relay.housekeep()
    # The expired event goes (cascading to its idempotency key and deliveries); the
    # processed outbox row of the live event goes because retention is 0s.
    assert swept.events == 1
    assert swept.total >= 2
    assert await _val(db_url, "SELECT count(*) FROM events") == 1
    assert str(await _val(db_url, "SELECT id FROM events")) == keep
    assert await _val(db_url, "SELECT count(*) FROM event_outbox") == 0
    assert await _val(db_url, "SELECT count(*) FROM deliveries") == 2
    assert await _val(db_url, "SELECT array_agg(key) FROM idempotency_keys") == ["live"]
    assert relay.stats.housekeeping_runs == 1
    assert (await relay.housekeep()).total == 0


async def test_housekeeping_keeps_unprocessed_outbox_rows(
    api: httpx.AsyncClient, database: Database, settings_for: Any, db_url: str
) -> None:
    tenant, _ = await _setup_simple(api, endpoints=1)
    await _publish_many(api, tenant, 2)
    swept = await Relay(database, settings_for(relay_outbox_retention_s=0)).housekeep()
    assert swept.outbox_rows == 0
    assert await _val(db_url, "SELECT count(*) FROM event_outbox") == 2


async def test_serve_once_and_cli(
    api: httpx.AsyncClient, settings_for: Any, db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    tenant, _ = await _setup_simple(api, endpoints=1)
    await _publish_many(api, tenant, 2)
    settings = settings_for()
    stats = await serve(settings, once=True)
    assert stats.outbox_rows_processed == 2

    await _publish_many(api, tenant, 1)
    monkeypatch.setattr(relay_module, "get_settings", lambda: settings)
    monkeypatch.setattr(relay_module, "configure_logging", lambda _settings: None)
    # main() calls asyncio.run, which cannot nest inside this test's loop: run it in a thread.
    assert await asyncio.to_thread(main, ["--once"]) == 0
    assert await _val(db_url, "SELECT count(*) FROM deliveries") == 3

    heartbeat = Path(settings.relay_heartbeat_file)
    await asyncio.to_thread(heartbeat.unlink, missing_ok=True)
    assert main(["--healthcheck"]) == 1
    await asyncio.to_thread(heartbeat.touch)
    assert main(["--healthcheck"]) == 0
    stale = time.time() + settings.relay_heartbeat_max_age_s + 1
    assert not heartbeat_is_fresh(settings.relay_heartbeat_file, 60, now=stale)
