"""Milestone 9e: cascade-safe repair of rows stranded in the events / deliveries DEFAULT
partitions, on a real Postgres 16.

Every test compares full row images (``to_jsonb`` of every row of the event graph and the
outbox) before and after, so "no row lost" also means "no column changed"."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from alembic import command

from hookrelay import partitions as partitions_cli
from hookrelay.db.partitioning import DELIVERIES, DELIVERY_ATTEMPTS, EVENTS, day_bounds
from hookrelay.services import partition_repair as repair_mod
from hookrelay.services.partition_repair import (
    DEFERRED,
    REPAIRED,
    TOO_LARGE,
    UNSUPPORTED,
    WOULD_REPAIR,
    component_plan,
    repair_day,
    repair_defaults,
)
from hookrelay.services.partitions import (
    REPAIR_GATE_KEY,
    PartitionReport,
    connect_kwargs,
    maintain,
)
from tests.conftest import make_settings
from tests.integration.conftest import (
    _asyncpg_dsn,
    alembic_config,
    create_scratch_database,
    drop_scratch_database,
)
from tests.integration.test_partitions import (
    Graph,
    World,
    _attempt,
    _create_partition,
    _graph,
    _no_sleep,
    _policy,
    _today,
    _world,
)

GRAPH_TABLES = ("events", "deliveries", "delivery_attempts", "event_outbox")


@pytest.fixture
def db_url() -> Iterator[str]:
    name, url = create_scratch_database("hookrelay_repair")
    try:
        command.upgrade(alembic_config(url), "head")
        yield url
    finally:
        drop_scratch_database(name)


async def _connect(db_url: str) -> asyncpg.Connection:
    return await asyncpg.connect(
        _asyncpg_dsn(db_url), **connect_kwargs(_policy(), application_name="test-repair")
    )


@pytest.fixture
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    c = await _connect(db_url)
    try:
        yield c
    finally:
        await c.close()


async def _maintain_all(conn: asyncpg.Connection, **overrides: Any) -> dict[str, PartitionReport]:
    base: dict[str, Any] = {"premake_days": 14, "repair_default": True}
    base.update(overrides)
    reports = await maintain(conn, _policy(**base), sleep=_no_sleep)
    return {r.table: r for r in reports}


async def _stranded_graph(
    conn: asyncpg.Connection, world: World, at: datetime, *, attempts: int = 2
) -> Graph:
    """An event with one delivery, ``attempts`` attempts and a processed outbox row."""
    g = await _graph(conn, world, at)
    for n in range(1, attempts + 1):
        await _attempt(conn, world, g, n)
    await conn.execute(
        "INSERT INTO event_outbox (tenant_id, event_id, event_created_at, processed_at, attempts) "
        "VALUES ($1, $2, $3, now(), 1)",
        world.tenant,
        g.event,
        g.key,
    )
    return g


async def _images(conn: asyncpg.Connection) -> dict[str, list[str]]:
    """Every row of the graph tables as sorted JSON (partition-independent)."""
    out: dict[str, list[str]] = {}
    for table in GRAPH_TABLES:
        rows = await conn.fetch(f"SELECT to_jsonb(t)::text AS j FROM {table} AS t")  # noqa: S608
        out[table] = sorted(r["j"] for r in rows)
    return out


async def _homes(conn: asyncpg.Connection, table: str) -> set[str]:
    rows = await conn.fetch(f"SELECT DISTINCT tableoid::regclass::text AS p FROM {table}")  # noqa: S608
    return {r["p"] for r in rows}


async def _assert_fk_integrity(conn: asyncpg.Connection) -> None:
    orphans = await conn.fetchval(
        "SELECT (SELECT count(*) FROM deliveries AS d WHERE NOT EXISTS (SELECT 1 FROM events AS e "
        " WHERE e.id = d.event_id AND e.created_at = d.event_created_at)) "
        "+ (SELECT count(*) FROM delivery_attempts AS a WHERE NOT EXISTS (SELECT 1 FROM "
        " deliveries AS d WHERE d.id = a.delivery_id AND d.event_created_at = a.event_created_at)) "
        "+ (SELECT count(*) FROM event_outbox AS o WHERE NOT EXISTS (SELECT 1 FROM events AS e "
        " WHERE e.id = o.event_id AND e.created_at = o.event_created_at))"
    )
    assert orphans == 0


def _at(day: date, hours: int = 3) -> datetime:
    return day_bounds(day)[0] + timedelta(hours=hours)


async def test_a_stranded_day_moves_as_one_component_without_losing_a_row(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    future, past = today + timedelta(days=12), today - timedelta(days=3)
    for i in range(3):
        await _stranded_graph(conn, world, _at(future, i + 1))
    await _stranded_graph(conn, world, _at(past), attempts=1)
    control = await _stranded_graph(conn, world, _at(today))  # in today's partitions already
    assert await _homes(conn, "events") == {EVENTS.default_partition, EVENTS.partition_name(today)}
    before = await _images(conn)

    reports = await _maintain_all(conn)

    assert sorted(reports["events"].repaired_days) == sorted([future.isoformat(), past.isoformat()])
    assert reports["events"].repaired_rows == 4
    assert reports["deliveries"].repaired_rows == 4
    assert reports["delivery_attempts"].repaired_rows == 7
    assert all(r.complete and not r.blocked_days and not r.repair_skipped for r in reports.values())
    for table in (EVENTS, DELIVERIES, DELIVERY_ATTEMPTS):
        assert reports[table.name].default_rows == 0
        homes = await _homes(conn, table.name)
        assert table.default_partition not in homes
        assert {table.partition_name(future), table.partition_name(past)} <= homes
    assert await _images(conn) == before  # every row, every column
    await _assert_fk_integrity(conn)
    # The repaired graph is live: deleting the event still cascades through the new partitions.
    await conn.execute("DELETE FROM events WHERE id = $1", control.event)
    assert await conn.fetchval("SELECT count(*) FROM deliveries") == 4


async def test_repair_is_idempotent(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    await _stranded_graph(conn, world, _at(today + timedelta(days=20)))
    first = await _maintain_all(conn)
    assert first["events"].repaired_days
    after_first = await _images(conn)
    second = await _maintain_all(conn)
    assert all(not r.repaired_days and r.repaired_rows == 0 for r in second.values())
    assert all(r.complete for r in second.values())
    assert await _images(conn) == after_first


async def test_dry_run_counts_and_changes_nothing(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=10)
    await _stranded_graph(conn, world, _at(day))
    before = await _images(conn)
    result = await repair_day(conn, EVENTS, day, _policy(dry_run=True), sleep=_no_sleep)
    assert result.status == WOULD_REPAIR
    assert result.rows == {"events": 1, "deliveries": 1, "event_outbox": 1, "delivery_attempts": 2}
    reports = await _maintain_all(conn, dry_run=True)
    assert reports["events"].repair_planned == [day.isoformat()]
    assert reports["events"].blocked_days == [day.isoformat()]  # still reported as blocked
    assert await _homes(conn, "events") == {EVENTS.default_partition}
    assert not await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", EVENTS.partition_name(day))
    assert await _images(conn) == before
    assert (
        await conn.fetchval("SELECT count(*) FROM pg_class WHERE relname LIKE '_hr_repair%'") == 0
    )


async def test_without_the_flag_blocked_days_are_still_reported(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _stranded_graph(conn, world, _at(day))
    before = await _images(conn)
    reports = await _maintain_all(conn, repair_default=False)
    assert reports["events"].blocked_days == [day.isoformat()]
    assert reports["deliveries"].blocked_days == [day.isoformat()]
    assert not reports["events"].complete
    assert reports["events"].repaired_days == []
    assert await _images(conn) == before


async def test_a_day_over_the_row_budget_is_skipped_and_still_blocked(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    big, small = today + timedelta(days=9), today + timedelta(days=11)
    for i in range(2):
        await _stranded_graph(conn, world, _at(big, i + 1))
    await _stranded_graph(conn, world, _at(small), attempts=0)
    before = await _images(conn)
    # big: 2 events -> 2 + 2 deliveries + 4 attempts + 2 outbox = 10 rows; small: 3 rows.
    reports = await _maintain_all(conn, repair_max_rows=5)
    events = reports["events"]
    assert events.repair_skipped == [f"{big.isoformat()} {TOO_LARGE}"]
    assert events.repaired_days == [small.isoformat()]
    assert events.blocked_days == [big.isoformat()]
    assert not events.complete
    assert await _images(conn) == before
    assert await _homes(conn, "events") == {EVENTS.default_partition, EVENTS.partition_name(small)}
    # The root alone over budget is refused before anything is copied or locked.
    root_only = await repair_day(conn, EVENTS, big, _policy(repair_max_rows=1), sleep=_no_sleep)
    assert root_only.status == TOO_LARGE
    assert "events rows" in (root_only.detail or "")


async def test_the_day_budget_defers_the_rest(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    days = [today + timedelta(days=k) for k in (15, 16, 17)]
    for day in days:
        await _stranded_graph(conn, world, _at(day), attempts=0)
    results = await repair_defaults(conn, _policy(repair_max_days=2), sleep=_no_sleep)
    assert [(r.day, r.status) for r in results] == [
        (days[0], REPAIRED),
        (days[1], REPAIRED),
        (days[2], DEFERRED),
    ]
    reports = await _maintain_all(conn, repair_max_days=2)
    assert reports["events"].repaired_days == [days[2].isoformat()]


async def test_only_deliveries_stranded_repairs_from_deliveries(conn: asyncpg.Connection) -> None:
    """The events partition of the day exists (e.g. premade by hand), the deliveries one not."""
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=13)
    await _create_partition(conn, day, EVENTS)
    await _stranded_graph(conn, world, _at(day))
    assert await _homes(conn, "deliveries") == {DELIVERIES.default_partition}
    before = await _images(conn)
    reports = await _maintain_all(conn)
    assert reports["events"].repaired_days == []
    assert reports["deliveries"].repaired_days == [day.isoformat()]
    assert reports["deliveries"].repaired_rows == 1
    assert reports["delivery_attempts"].repaired_rows == 2
    assert await _homes(conn, "deliveries") == {DELIVERIES.partition_name(day)}
    assert await _images(conn) == before
    await _assert_fk_integrity(conn)


async def test_days_past_retention_are_left_to_the_sweep(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    old = today - timedelta(days=40)
    await _stranded_graph(conn, world, _at(old), attempts=0)
    assert await repair_defaults(conn, _policy(), sleep=_no_sleep) == []
    reports = await _maintain_all(conn)
    assert reports["events"].default_rows_deleted == 1  # swept, as before
    assert await conn.fetchval("SELECT count(*) FROM events") == 0


async def test_concurrent_runs_repair_once(db_url: str, conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    for k in (9, 10, 11):
        await _stranded_graph(conn, world, _at(today + timedelta(days=k)))
    before = await _images(conn)
    a, b = await _connect(db_url), await _connect(db_url)
    try:
        # Real sleeps: the repair-gate holder waits for table locks the other run holds.
        policy = _policy(premake_days=14, repair_default=True, retry_base_s=0.05, ddl_retries=20)
        ra, rb = await asyncio.gather(maintain(a, policy), maintain(b, policy))
    finally:
        await a.close()
        await b.close()
    repaired = [d for r in (*ra, *rb) for d in r.repaired_days]
    assert sorted(repaired) == sorted((today + timedelta(days=k)).isoformat() for k in (9, 10, 11))
    assert not any(r.repair_skipped for r in (*ra, *rb))
    assert await _images(conn) == before
    await _assert_fk_integrity(conn)
    assert await _homes(conn, "events") == {
        EVENTS.partition_name(today + timedelta(days=k)) for k in (9, 10, 11)
    }


async def test_a_held_advisory_lock_skips_the_repair(db_url: str, conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _stranded_graph(conn, world, _at(day))
    holder = await _connect(db_url)
    try:
        assert await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            "hookrelay.partitions:delivery_attempts",
        )
        reports = await _maintain_all(conn)
        assert reports["delivery_attempts"].skipped_locked
        assert reports["events"].repaired_days == []
        assert reports["events"].blocked_days == [day.isoformat()]
    finally:
        await holder.close()


async def test_a_row_lock_defers_the_day_then_the_next_run_repairs_it(
    db_url: str, conn: asyncpg.Connection
) -> None:
    """A worker holding a delivery row lock (its lease) makes the day wait for it, bounded by
    lock_timeout; nothing is half-moved, and the next run completes the day."""
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    g = await _stranded_graph(conn, world, _at(day))
    before = await _images(conn)
    worker = await _connect(db_url)
    try:
        tx = worker.transaction()
        await tx.start()
        await worker.execute("SELECT 1 FROM deliveries WHERE id = $1 FOR UPDATE", g.delivery)
        reports = await _maintain_all(conn)
        assert reports["events"].repair_skipped == [f"{day.isoformat()} {DEFERRED}"]
        assert not reports["events"].complete
        await tx.rollback()
    finally:
        await worker.close()
    assert await _images(conn) == before
    reports = await _maintain_all(conn)
    assert reports["events"].repaired_days == [day.isoformat()]
    assert await _images(conn) == before


async def test_a_non_cascading_reference_makes_the_day_unsupported(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    g = await _stranded_graph(conn, world, _at(day), attempts=0)
    await conn.execute(
        "CREATE TABLE audit_refs (tenant_id uuid, event_id uuid, event_created_at timestamptz, "
        "FOREIGN KEY (tenant_id, event_id, event_created_at) "
        "REFERENCES events (tenant_id, id, created_at) ON DELETE SET NULL)"
    )
    await conn.execute("INSERT INTO audit_refs VALUES ($1, $2, $3)", world.tenant, g.event, g.key)
    before = await _images(conn)
    reports = await _maintain_all(conn)
    assert reports["events"].repair_skipped == [f"{day.isoformat()} {UNSUPPORTED}"]
    assert await _images(conn) == before
    assert await conn.fetchval("SELECT event_id FROM audit_refs") == g.event
    await conn.execute("DELETE FROM audit_refs")
    reports = await _maintain_all(conn)  # no referencing rows left: repairable
    assert reports["events"].repaired_days == [day.isoformat()]


async def test_a_table_reachable_twice_is_unsupported(conn: asyncpg.Connection) -> None:
    await conn.execute(
        "CREATE TABLE diamond (id uuid PRIMARY KEY, tenant_id uuid, event_id uuid, "
        "event_created_at timestamptz, delivery_id uuid, "
        "FOREIGN KEY (tenant_id, event_id, event_created_at) "
        "REFERENCES events (tenant_id, id, created_at) ON DELETE CASCADE, "
        "FOREIGN KEY (tenant_id, delivery_id, event_created_at) "
        "REFERENCES deliveries (tenant_id, id, event_created_at) ON DELETE CASCADE)"
    )
    with pytest.raises(Exception, match="two paths"):
        await component_plan(conn, "events")
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _stranded_graph(conn, world, _at(day), attempts=0)
    result = await repair_day(conn, EVENTS, day, _policy(), sleep=_no_sleep)
    assert result.status == UNSUPPORTED


async def test_component_plan_follows_the_catalog(conn: asyncpg.Connection) -> None:
    plan = await component_plan(conn, "events")
    assert [n.table for n in plan.nodes] == [
        "events",
        "deliveries",
        "event_outbox",
        "delivery_attempts",
    ]
    assert plan.guards == ()
    assert [n.table for n in (await component_plan(conn, "deliveries")).nodes] == [
        "deliveries",
        "delivery_attempts",
    ]


async def test_cli_repair_flag_exit_codes_and_textfile(
    db_url: str, conn: asyncpg.Connection, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=8)  # past the migration's premade days, inside the window
    await _stranded_graph(conn, world, _at(day))
    settings = make_settings(database_url=db_url, partition_premake_days=9)
    monkeypatch.setattr(partitions_cli, "get_settings", lambda: settings)
    assert await asyncio.to_thread(partitions_cli.main, []) == partitions_cli.EXIT_INCOMPLETE
    dry = await asyncio.to_thread(partitions_cli.main, ["--dry-run", "--repair-default"])
    assert dry == partitions_cli.EXIT_INCOMPLETE  # the day is still blocked
    textfile = tmp_path / "hookrelay_partitions.prom"
    rc = await asyncio.to_thread(
        partitions_cli.main, ["--repair-default", "--metrics-textfile", str(textfile)]
    )
    assert rc == 0
    body = textfile.read_text()
    assert (
        'hookrelay_partition_repaired_rows_total{service="partitions",table="events"} 1.0' in body
    )
    assert 'table="delivery_attempts"} 2.0' in body
    # PARTITION_REPAIR_DEFAULT turns it on without the flag.
    on = make_settings(database_url=db_url, partition_premake_days=9, partition_repair_default=True)
    reports = await partitions_cli.run(on)
    assert all(r.complete and r.repaired_rows == 0 for r in reports)


async def test_a_repaired_graph_keeps_working_for_leases_and_new_attempts(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today - timedelta(days=1)
    g = await _stranded_graph(conn, world, _at(day), attempts=1)
    await _maintain_all(conn)
    # New writes keyed by the event day go to the repaired partitions, FKs intact.
    assert await _attempt(conn, world, g, 2) == DELIVERY_ATTEMPTS.partition_name(day)
    await conn.execute(
        "UPDATE deliveries SET attempt_count = attempt_count + 1 WHERE id = $1 "
        "AND event_created_at = $2",
        g.delivery,
        g.key,
    )
    assert await conn.fetchval(
        "SELECT tableoid::regclass::text FROM deliveries WHERE id = $1", g.delivery
    ) == DELIVERIES.partition_name(day)


async def test_existing_child_partitions_are_refilled_through_the_parent(
    conn: asyncpg.Connection,
) -> None:
    """Only the deliveries partition of the day exists: events and attempts are staged and
    attached, the deliveries go back through the parent table in between (parents first)."""
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _create_partition(conn, day, DELIVERIES)
    await _stranded_graph(conn, world, _at(day))
    assert await _homes(conn, "deliveries") == {DELIVERIES.partition_name(day)}
    before = await _images(conn)
    result = await repair_day(conn, EVENTS, day, _policy(), sleep=_no_sleep)
    assert result.status == REPAIRED
    assert set(result.phase_ms) == {"lock", "copy", "delete", "stage", "attach"}
    assert await _homes(conn, "events") == {EVENTS.partition_name(day)}
    assert await _homes(conn, "delivery_attempts") == {DELIVERY_ATTEMPTS.partition_name(day)}
    assert await _images(conn) == before
    await _assert_fk_integrity(conn)


async def test_a_writer_racing_into_the_stranded_day_waits_and_loses_nothing(
    db_url: str, conn: asyncpg.Connection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """While a day is repaired the event graph is write-locked (reads continue): an ingest of a
    new event for that day waits at the parent table lock, before tuple routing, so after the
    commit it is routed into the new day partition, not into the DEFAULT."""
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _stranded_graph(conn, world, _at(day))
    writer = await asyncpg.connect(_asyncpg_dsn(db_url))
    original = repair_mod._DayRepairer._snapshot
    seen: dict[str, Any] = {}

    async def snapshot_then_race(self: Any, root_default: str, where: str) -> dict[str, int]:
        rows = await original(self, root_default, where)
        seen["task"] = asyncio.create_task(
            writer.fetchval(
                "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
                "created_at, retain_until) VALUES ($1, 'x.y', '{}', 2, $2::timestamptz, $2::timestamptz, $2::timestamptz + interval "
                "'1 day') RETURNING tableoid::regclass::text",
                world.tenant,
                _at(day, 9),
            )
        )
        await asyncio.sleep(0.3)
        seen["blocked"] = not seen["task"].done()
        return rows

    monkeypatch.setattr(repair_mod._DayRepairer, "_snapshot", snapshot_then_race)
    try:
        before = await _images(conn)
        result = await repair_day(conn, EVENTS, day, _policy(), sleep=_no_sleep)
        assert result.status == REPAIRED
        assert seen["blocked"]  # the writer waited for the repair to commit
        landed = await seen["task"]
    finally:
        await writer.close()
    assert landed == EVENTS.partition_name(day)
    after = await _images(conn)
    assert len(after["events"]) == len(before["events"]) + 1
    assert set(before["events"]) <= set(after["events"])
    assert {k: v for k, v in after.items() if k != "events"} == {
        k: v for k, v in before.items() if k != "events"
    }
    assert await _homes(conn, "events") == {EVENTS.partition_name(day)}


async def test_a_held_repair_gate_leaves_the_repair_to_its_holder(
    db_url: str, conn: asyncpg.Connection
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _stranded_graph(conn, world, _at(day))
    holder = await _connect(db_url)
    try:
        assert await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", REPAIR_GATE_KEY
        )
        reports = await _maintain_all(conn)
        assert reports["events"].repaired_days == []
        assert reports["events"].blocked_days == [day.isoformat()]
        assert not any(r.skipped_locked for r in reports.values())  # the tables were maintained
    finally:
        await holder.close()
    assert (await _maintain_all(conn))["events"].repaired_days == [day.isoformat()]


async def test_the_gate_holder_waits_for_a_table_lock_to_free_up(
    db_url: str, conn: asyncpg.Connection
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    day = today + timedelta(days=12)
    await _stranded_graph(conn, world, _at(day))
    holder = await _connect(db_url)
    key = "hookrelay.partitions:delivery_attempts"
    try:
        assert await holder.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key)

        async def release_soon() -> None:
            await asyncio.sleep(0.3)
            await holder.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", key)

        policy = _policy(premake_days=14, repair_default=True, retry_base_s=0.05, ddl_retries=10)
        reports, _ = await asyncio.gather(maintain(conn, policy), release_soon())
    finally:
        await holder.close()
    by_table = {r.table: r for r in reports}
    assert not by_table["delivery_attempts"].skipped_locked
    assert by_table["events"].repaired_days == [day.isoformat()]
