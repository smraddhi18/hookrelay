"""Milestones 9c/9d: range partitioning of the whole event graph (events, deliveries,
delivery_attempts, one UTC-day partition per event day) and its maintenance job, on real Postgres.

Every test migrates its own scratch database (the maintenance job runs DDL and takes advisory
locks, so it must not share the session database the rest of the suite uses).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any, NamedTuple

import asyncpg
import pytest
from alembic import command

from hookrelay import partitions as partitions_cli
from hookrelay.core.config import Settings
from hookrelay.db.partitioning import (
    DELIVERIES,
    DELIVERY_ATTEMPTS,
    EVENTS,
    PARTITIONED_TABLES,
    PartitionedTable,
    day_bounds,
)
from hookrelay.observability.gauges import refresh_partition_gauges
from hookrelay.observability.metrics import SERVICE_API, build_metrics, render_metrics
from hookrelay.services.partitions import (
    PartitionMaintainer,
    PartitionPolicy,
    PartitionReport,
    advisory_key,
    connect_kwargs,
    ensure_premade,
    maintain,
)
from tests.conftest import make_settings
from tests.integration.conftest import (
    _asyncpg_dsn,
    alembic_config,
    create_scratch_database,
    drop_scratch_database,
)
from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant

PARENT = DELIVERY_ATTEMPTS.name
DEFAULT = DELIVERY_ATTEMPTS.default_partition


@pytest.fixture
def db_url() -> Iterator[str]:
    name, url = create_scratch_database("hookrelay_part")
    try:
        command.upgrade(alembic_config(url), "head")
        yield url
    finally:
        drop_scratch_database(name)


@pytest.fixture
async def conn(db_url: str) -> AsyncIterator[asyncpg.Connection]:
    """A maintenance-style session (UTC, short lock timeout), like hookrelay-partitions'."""
    c = await asyncpg.connect(
        _asyncpg_dsn(db_url), **connect_kwargs(_policy(), application_name="test-partitions")
    )
    try:
        yield c
    finally:
        await c.close()


def _policy(**overrides: Any) -> PartitionPolicy:
    base: dict[str, Any] = {
        "premake_days": 7,
        "retention_days": 30,
        "lock_timeout_ms": 200,
        "ddl_retries": 1,
        "retry_base_s": 0.0,
        "sweep_batch_size": 2,
        "sweep_max_batches": 50,
    }
    base.update(overrides)
    return PartitionPolicy(**base)


async def _no_sleep(_s: float) -> None:
    return None


async def _maintain(
    conn: asyncpg.Connection, table: PartitionedTable = DELIVERY_ATTEMPTS, **overrides: Any
) -> PartitionReport:
    """One table's run (attempts by default)."""
    return await PartitionMaintainer(conn, _policy(**overrides), table=table, sleep=_no_sleep).run()


async def _maintain_all(conn: asyncpg.Connection, **overrides: Any) -> dict[str, PartitionReport]:
    reports = await maintain(conn, _policy(**overrides), sleep=_no_sleep)
    return {r.table: r for r in reports}


async def _today(conn: asyncpg.Connection) -> date:
    now: datetime = await conn.fetchval("SELECT now()")
    return now.astimezone(UTC).date()


class World(NamedTuple):
    tenant: uuid.UUID
    endpoint: uuid.UUID


class Graph(NamedTuple):
    """One event with its delivery (and later attempts), all keyed by ``key`` = event day."""

    event: uuid.UUID
    delivery: uuid.UUID
    key: datetime


async def _world(conn: asyncpg.Connection) -> World:
    tenant = await _tenant(conn, f"p-{uuid.uuid4().hex[:8]}")
    await _event_type(conn, tenant, "x.y")
    endpoint = await _endpoint(conn, tenant, f"https://{uuid.uuid4().hex[:8]}.example.com/")
    return World(tenant, endpoint)


async def _graph(conn: asyncpg.Connection, world: World, at: datetime | None = None) -> Graph:
    """An event created at ``at`` (default: now) and one delivery for it."""
    key: datetime = at if at is not None else await conn.fetchval("SELECT now()")
    event = await conn.fetchval(
        "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
        "created_at, retain_until) VALUES ($1, 'x.y', '{}', 2, $2::timestamptz, $2::timestamptz, "
        "$2::timestamptz + interval '1 day') "
        "RETURNING id",
        world.tenant,
        key,
    )
    delivery = await conn.fetchval(
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, "
        "event_created_at) VALUES ($1, $2, $3, $4, $5) RETURNING id",
        world.tenant,
        event,
        world.endpoint,
        key + timedelta(days=1),
        key,
    )
    return Graph(event, delivery, key)


async def _attempt(
    conn: asyncpg.Connection, world: World, g: Graph, n: int = 1, *, key: datetime | None = None
) -> str:
    """Insert attempt ``n`` of ``g``; returns its partition. ``key`` overrides the partition key
    (to prove the composite FK rejects a wrong one)."""
    row = await conn.fetchrow(
        "INSERT INTO delivery_attempts (tenant_id, delivery_id, event_created_at, "
        "attempt_number, outcome, status_code, request_url, worker_id, started_at, finished_at) "
        "VALUES ($1, $2, $3, $4, 'succeeded', 200, 'https://a.example.com/', 'w', now(), now()) "
        "RETURNING tableoid::regclass::text AS partition",
        world.tenant,
        g.delivery,
        key if key is not None else g.key,
        n,
    )
    assert row is not None
    return str(row["partition"])


async def _where(conn: asyncpg.Connection, table: str, row_id: uuid.UUID) -> str:
    return str(
        await conn.fetchval(f"SELECT tableoid::regclass::text FROM {table} WHERE id = $1", row_id)  # noqa: S608
    )


async def _partitions(conn: asyncpg.Connection, table: str = PARENT) -> list[str]:
    rows = await conn.fetch(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = $1::regclass ORDER BY 1",
        table,
    )
    return [r["relname"] for r in rows]


async def _exists(conn: asyncpg.Connection, table: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table))


async def _create_partition(
    conn: asyncpg.Connection, day: date, table: PartitionedTable = DELIVERY_ATTEMPTS
) -> str:
    """A partition for a past day, as an earlier run would have premade it."""
    name = table.partition_name(day)
    lower, upper = day_bounds(day)
    await conn.execute(
        f"CREATE TABLE {name} PARTITION OF {table.name} "
        f"FOR VALUES FROM ('{lower.isoformat()}') TO ('{upper.isoformat()}')"
    )
    return name


async def _create_day(conn: asyncpg.Connection, day: date) -> None:
    for table in PARTITIONED_TABLES:
        await _create_partition(conn, day, table)


async def _count(conn: asyncpg.Connection, table: str) -> int:
    return int(await conn.fetchval(f"SELECT count(*) FROM {table}"))  # noqa: S608


# --- layout -------------------------------------------------------------------------------


async def test_migration_creates_daily_partitions_and_a_default_per_table(
    conn: asyncpg.Connection,
) -> None:
    today = await _today(conn)
    for table in PARTITIONED_TABLES:
        expected = {table.partition_name(today + timedelta(days=k)) for k in range(8)}
        assert set(await _partitions(conn, table.name)) == {*expected, table.default_partition}
        assert (
            await conn.fetchval("SELECT relkind::text FROM pg_class WHERE relname = $1", table.name)
            == "p"
        )
    keys = {
        "pk_events": "PRIMARY KEY (id, created_at)",
        "pk_deliveries": "PRIMARY KEY (id, event_created_at)",
        "pk_delivery_attempts": "PRIMARY KEY (id, event_created_at)",
        "uq_deliveries_event_endpoint": "UNIQUE (event_id, endpoint_id, event_created_at)",
        "uq_delivery_attempts_number": "UNIQUE (delivery_id, attempt_number, event_created_at)",
    }
    for name, definition in keys.items():
        found = await conn.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname = $1 AND conparentid = 0",
            name,
        )
        assert found == definition, name
    # external_id uniqueness moved to a plain claim table (events cannot carry it any more).
    assert await _exists(conn, "event_external_ids")
    # A new partition inherits the indexes and the triggers of its parent.
    part = DELIVERY_ATTEMPTS.partition_name(today)
    parent_idx = await conn.fetchval("SELECT count(*) FROM pg_indexes WHERE tablename = $1", PARENT)
    assert await conn.fetchval("SELECT count(*) FROM pg_indexes WHERE tablename = $1", part) == (
        parent_idx
    )
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal",
            part,
        )
        == 1
    )


async def test_rows_land_in_their_event_day_or_the_default_partition(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    today = now.astimezone(UTC).date()
    tomorrow = day_bounds(today + timedelta(days=1))[0] + timedelta(hours=5)
    cases = (
        (now, today),
        (tomorrow, today + timedelta(days=1)),
        (now + timedelta(days=40), None),  # beyond the premade horizon: DEFAULT, never an error
        (now - timedelta(days=400), None),
    )
    for at, day in cases:
        g = await _graph(conn, world, at)
        for table in PARTITIONED_TABLES:
            expected = table.default_partition if day is None else table.partition_name(day)
            row_id = g.event if table is EVENTS else g.delivery
            if table is DELIVERY_ATTEMPTS:
                assert await _attempt(conn, world, g) == expected
            else:
                assert await _where(conn, table.name, row_id) == expected
    # The BEFORE INSERT triggers still derive endpoint_id / event_type on partitioned tables.
    assert [
        r[0] for r in await conn.fetch("SELECT DISTINCT endpoint_id FROM delivery_attempts")
    ] == [world.endpoint]
    assert [r[0] for r in await conn.fetch("SELECT DISTINCT event_type FROM deliveries")] == ["x.y"]


async def test_the_partition_key_cannot_be_wrong_or_missing(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    g = await _graph(conn, world)
    other_day = g.key - timedelta(days=3)
    # A key that is not the delivery's: rejected by the composite FK, not silently re-routed.
    with pytest.raises(asyncpg.ForeignKeyViolationError) as exc:
        await _attempt(conn, world, g, key=other_day)
    assert exc.value.constraint_name == "fk_delivery_attempts_delivery_same_tenant"
    with pytest.raises(asyncpg.ForeignKeyViolationError) as exc:
        await _attempt(conn, world, Graph(g.event, uuid.uuid4(), g.key))
    assert exc.value.constraint_name == "fk_delivery_attempts_delivery_same_tenant"
    # A delivery for an event under the wrong day, or with no day at all.
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await conn.execute(
            "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, "
            "event_created_at) VALUES ($1, $2, $3, now() + interval '1 day', $4)",
            world.tenant,
            g.event,
            world.endpoint,
            other_day,
        )
    with pytest.raises(asyncpg.NotNullViolationError):
        await conn.execute(
            "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
            "VALUES ($1, $2, $3, now() + interval '1 day')",
            world.tenant,
            g.event,
            world.endpoint,
        )


async def test_deleting_an_event_cascades_through_every_partition(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    graphs = [await _graph(conn, world, now + timedelta(days=d)) for d in (0, 1, 60)]
    for g in graphs:
        await _attempt(conn, world, g)
    await conn.execute("DELETE FROM events")
    for table in ("events", "deliveries", "delivery_attempts"):
        assert await _count(conn, table) == 0


async def _plan(conn: asyncpg.Connection, sql: str, *args: object) -> str:
    return "\n".join(r[0] for r in await conn.fetch(f"EXPLAIN (COSTS OFF) {sql}", *args))


async def test_day_windows_and_keyed_lookups_prune_to_their_partitions(
    conn: asyncpg.Connection,
) -> None:
    today = await _today(conn)
    lower, upper = day_bounds(today)
    for table in PARTITIONED_TABLES:
        window = f"{table.key} >= '{lower.isoformat()}' AND {table.key} < '{upper.isoformat()}'"
        plan = await _plan(conn, f"SELECT count(*) FROM {table.name} WHERE {window}")  # noqa: S608
        assert table.partition_name(today) in plan
        assert table.partition_name(today + timedelta(days=1)) not in plan
        assert table.default_partition not in plan  # nothing in DEFAULT inside a covered range
    # The hot paths carry the day with the id: one partition, whatever the table.
    for table in PARTITIONED_TABLES:
        plan = await _plan(
            conn,
            f"SELECT * FROM {table.name} WHERE id = $1 AND {table.key} = '{lower.isoformat()}'",  # noqa: S608
            uuid.uuid4(),
        )
        probed = [p for p in await _partitions(conn, table.name) if p in plan]
        assert probed == [table.partition_name(today)], (table.name, plan)
    # An id alone cannot prune: one index probe per partition (the documented trade-off).
    for table in PARTITIONED_TABLES:
        plan = await _plan(conn, f"SELECT * FROM {table.name} WHERE id = $1", uuid.uuid4())  # noqa: S608
        assert sum(p in plan for p in await _partitions(conn, table.name)) == 9, table.name
    # now()-relative windows on the key prune at executor start-up.
    old = await _create_partition(conn, today - timedelta(days=3))
    await _create_partition(conn, today - timedelta(days=2))
    stable = "\n".join(
        r[0]
        for r in await conn.fetch(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT count(*) "
            "FROM delivery_attempts WHERE event_created_at >= now() - interval '1 hour'"
        )
    )
    assert "Subplans Removed: 2" in stable
    assert old not in stable


async def test_attempt_numbers_are_unique_per_delivery(conn: asyncpg.Connection) -> None:
    """All attempts of a delivery share its event day, so (delivery, number, day) is a true
    per-delivery constraint: the weaker key Postgres requires costs no correctness here."""
    world = await _world(conn)
    g = await _graph(conn, world)
    await _attempt(conn, world, g, 1)
    with pytest.raises(asyncpg.UniqueViolationError):
        await _attempt(conn, world, g, 1)
    await conn.execute("UPDATE delivery_attempts SET created_at = created_at + interval '2 days'")
    with pytest.raises(asyncpg.UniqueViolationError):  # a replay a week later: same partition
        await _attempt(conn, world, g, 1)
    assert await _attempt(conn, world, g, 2) == DELIVERY_ATTEMPTS.partition_name(await _today(conn))


# --- maintenance --------------------------------------------------------------------------


async def test_premake_is_idempotent_and_extends_the_horizon_on_every_table(
    conn: asyncpg.Connection,
) -> None:
    first = await _maintain_all(conn)
    assert set(first) == {t.name for t in PARTITIONED_TABLES}
    today = await _today(conn)
    for report in first.values():
        assert report.created == []
        assert report.complete
        assert report.partitions == 8
        assert report.premade_until == day_bounds(today + timedelta(days=7))[1]
    longer = await _maintain_all(conn, premake_days=10)
    for table in PARTITIONED_TABLES:
        assert longer[table.name].created == [
            table.partition_name(today + timedelta(days=k)) for k in (8, 9, 10)
        ]
    again = await _maintain_all(conn, premake_days=10)
    assert all(r.created == [] and r.partitions == 11 for r in again.values())
    # Created partitions are complete (indexes, FKs, triggers came with ATTACH) and the
    # temporary bound CHECK is gone.
    for table in PARTITIONED_TABLES:
        new = table.partition_name(today + timedelta(days=10))
        reference = table.partition_name(today)
        for catalog in ("pg_indexes", "pg_trigger"):
            col = "tablename" if catalog == "pg_indexes" else "tgrelid::regclass::text"
            counts = [
                await conn.fetchval(f"SELECT count(*) FROM {catalog} WHERE {col} = $1", p)  # noqa: S608
                for p in (new, reference)
            ]
            assert counts[0] == counts[1], (new, catalog)
        checks = await conn.fetch(
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid = $1::regclass AND conname LIKE '%bound'",
            new,
        )
        assert checks == []


async def test_attempts_in_the_default_partition_move_into_a_new_partition(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    far = day_bounds(today + timedelta(days=20))[0] + timedelta(hours=3)
    g = await _graph(conn, world, far)
    assert await _attempt(conn, world, g, 1) == DEFAULT
    assert await _attempt(conn, world, g, 2) == DEFAULT
    report = await _maintain(conn)
    target = DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=20))
    assert report.created == [target]
    assert report.moved_from_default == 2
    assert report.default_rows == 0
    rows = await conn.fetch("SELECT tableoid::regclass::text AS p FROM delivery_attempts")
    assert {r["p"] for r in rows} == {target}


async def test_days_stranded_in_default_are_blocked_not_moved_for_parent_tables(
    conn: asyncpg.Connection,
) -> None:
    """Moving events/deliveries out of DEFAULT is DELETE + INSERT, and the DELETE would cascade
    into the rows that reference them: the maintenance job refuses (and says so) instead."""
    world = await _world(conn)
    today = await _today(conn)
    far = day_bounds(today + timedelta(days=12))[0] + timedelta(hours=3)
    g = await _graph(conn, world, far)
    await _attempt(conn, world, g)
    reports = await _maintain_all(conn, premake_days=14)
    day = (today + timedelta(days=12)).isoformat()
    assert reports["events"].blocked_days == [day]
    assert reports["deliveries"].blocked_days == [day]
    assert not reports["events"].complete
    assert reports["delivery_attempts"].blocked_days == []  # a leaf table is movable
    assert reports["delivery_attempts"].moved_from_default == 1
    assert not await _exists(conn, EVENTS.partition_name(today + timedelta(days=12)))
    assert await _where(conn, "events", g.event) == EVENTS.default_partition  # untouched
    assert await _count(conn, "deliveries") == 1


async def test_retention_drops_an_aligned_day_child_first(conn: asyncpg.Connection) -> None:
    world = await _world(conn)
    today = await _today(conn)
    old_day, kept_day = today - timedelta(days=31), today - timedelta(days=29)
    await _create_day(conn, old_day)
    await _create_day(conn, kept_day)
    old = await _graph(conn, world, day_bounds(old_day)[0] + timedelta(hours=1))
    kept = await _graph(conn, world, day_bounds(kept_day)[0] + timedelta(hours=1))
    for g in (old, kept):
        await _attempt(conn, world, g, 1)
        await _attempt(conn, world, g, 2)

    dry = await _maintain_all(conn, dry_run=True)
    for table in PARTITIONED_TABLES:
        assert dry[table.name].detached == [table.partition_name(old_day)]
        assert await _exists(conn, table.partition_name(old_day))

    reports = await _maintain_all(conn)
    for table in PARTITIONED_TABLES:
        name = table.partition_name(old_day)
        r = reports[table.name]
        assert r.detached == [name]
        assert r.dropped == [name]
        assert r.detach_mode == "lock"  # a DEFAULT partition rules out CONCURRENTLY
        assert r.complete
        assert r.cutoff == day_bounds(today - timedelta(days=30))[0]
        assert not await _exists(conn, name)
        assert await _exists(conn, table.partition_name(kept_day))
    # The old day left as a unit; the kept day is whole.
    assert [r["id"] for r in await conn.fetch("SELECT id FROM events")] == [kept.event]
    assert [r["id"] for r in await conn.fetch("SELECT id FROM deliveries")] == [kept.delivery]
    assert await _count(conn, "delivery_attempts") == 2


async def test_a_parent_partition_is_never_dropped_while_children_reference_it(
    conn: asyncpg.Connection,
) -> None:
    """Postgres refuses to detach a referenced partition: if a child drop is skipped (here: not
    maintained at all) the parents report ``blocked`` and keep every row."""
    world = await _world(conn)
    today = await _today(conn)
    old_day = today - timedelta(days=31)
    await _create_day(conn, old_day)
    g = await _graph(conn, world, day_bounds(old_day)[0] + timedelta(hours=1))
    await _attempt(conn, world, g)
    reports = await maintain(conn, _policy(), tables=(EVENTS, DELIVERIES), sleep=_no_sleep)
    by = {r.table: r for r in reports}
    assert by["deliveries"].blocked == [DELIVERIES.partition_name(old_day)]
    assert by["events"].blocked == [EVENTS.partition_name(old_day)]
    assert not by["events"].complete
    assert by["events"].dropped == []
    assert by["deliveries"].dropped == []
    assert await _exists(conn, EVENTS.partition_name(old_day))
    assert [r["id"] for r in await conn.fetch("SELECT id FROM events")] == [g.event]
    assert await _count(conn, "delivery_attempts") == 1
    # Once the child is maintained too, the next full run takes the whole day.
    again = await _maintain_all(conn)
    assert all(r.dropped for r in again.values())
    assert await _count(conn, "events") == 0


async def test_default_partitions_are_swept_child_first_and_orphans_dropped(
    conn: asyncpg.Connection,
) -> None:
    world = await _world(conn)
    today = await _today(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    stale = [await _graph(conn, world, now - timedelta(days=90 + n)) for n in range(5)]
    fresh = await _graph(conn, world, now - timedelta(days=5))
    for g in (*stale, fresh):
        await _attempt(conn, world, g)
    assert await _count(conn, f"ONLY {DEFAULT}") == 6  # 5 stale + the fresh day (no partition)
    # A table a crashed run detached but never dropped.
    orphan = DELIVERY_ATTEMPTS.partition_name(today - timedelta(days=45))
    await conn.execute(f"CREATE TABLE {orphan} (LIKE delivery_attempts)")

    reports = await _maintain_all(conn)
    attempts = reports["delivery_attempts"]
    assert attempts.default_rows_deleted == 5
    assert orphan in attempts.dropped
    assert attempts.complete
    # The stale events leave DEFAULT too (their deliveries cascade); the fresh day stays.
    assert reports["events"].default_rows_deleted == 5
    assert not await _exists(conn, orphan)
    assert [r["id"] for r in await conn.fetch("SELECT id FROM events")] == [fresh.event]
    assert await _count(conn, "delivery_attempts") == 1


async def test_without_default_partitions_detach_is_concurrent_where_postgres_allows(
    conn: asyncpg.Connection,
) -> None:
    await conn.execute(f"DROP TABLE {DEFAULT}")
    today = await _today(conn)
    old = await _create_partition(conn, today - timedelta(days=40))
    report = await _maintain(conn)
    assert report.detached == [old]
    assert report.dropped == [old]
    assert report.detach_mode == "concurrently"
    assert report.default_rows == 0


async def test_without_default_partitions_a_whole_day_drops_concurrently(
    conn: asyncpg.Connection,
) -> None:
    """Postgres allows ``DETACH ... CONCURRENTLY`` of a referenced partition, so removing the
    DEFAULTs (detach first: the FKs reference them) gives a concurrent, child-first day drop."""
    for table in reversed(PARTITIONED_TABLES):
        await conn.execute(f"ALTER TABLE {table.name} DETACH PARTITION {table.default_partition}")
        await conn.execute(f"DROP TABLE {table.default_partition}")
    world = await _world(conn)
    today = await _today(conn)
    old_day = today - timedelta(days=31)
    await _create_day(conn, old_day)
    g = await _graph(conn, world, day_bounds(old_day)[0] + timedelta(hours=1))
    await _attempt(conn, world, g)
    reports = await _maintain_all(conn)
    for table in PARTITIONED_TABLES:
        r = reports[table.name]
        assert r.detach_mode == "concurrently"
        assert r.dropped == [table.partition_name(old_day)]
        assert r.complete
    assert [await _count(conn, t.name) for t in PARTITIONED_TABLES] == [0, 0, 0]


async def test_an_interrupted_concurrent_detach_is_finalized(db_url: str) -> None:
    dsn = _asyncpg_dsn(db_url)
    maint = await asyncpg.connect(dsn, server_settings={"TimeZone": "UTC"})
    reader = await asyncpg.connect(dsn)
    try:
        await maint.execute(f"DROP TABLE {DEFAULT}")
        today = await _today(maint)
        old = await _create_partition(maint, today - timedelta(days=40))
        # An open snapshot that read the parent makes DETACH CONCURRENTLY wait in its second
        # phase; cancel it there (statement_timeout) and the partition is left pending.
        tx = reader.transaction(isolation="repeatable_read")
        await tx.start()
        await reader.fetchval("SELECT count(*) FROM delivery_attempts")
        await maint.execute("SET statement_timeout = '500ms'")
        with pytest.raises(asyncpg.QueryCanceledError):
            await maint.execute(
                f"ALTER TABLE delivery_attempts DETACH PARTITION {old} CONCURRENTLY"
            )
        await maint.execute("RESET statement_timeout")
        await tx.rollback()
        pending = await maint.fetchval(
            "SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = $1::regclass", old
        )
        assert pending is True
        report = await _maintain(maint)
        assert report.finalized == [old]
        assert report.dropped == [old]
        assert not await _exists(maint, old)
    finally:
        await reader.close()
        await maint.close()


async def test_lock_timeouts_are_retried_then_deferred(db_url: str) -> None:
    dsn = _asyncpg_dsn(db_url)
    maint = await asyncpg.connect(
        dsn, **connect_kwargs(_policy(lock_timeout_ms=100), application_name="t")
    )
    blocker = await asyncpg.connect(dsn)
    try:
        tx = blocker.transaction()
        await tx.start()
        # SHARE conflicts with ATTACH's SHARE UPDATE EXCLUSIVE on the parent.
        await blocker.execute("LOCK TABLE delivery_attempts IN SHARE MODE")
        sleeps: list[float] = []

        async def sleep(s: float) -> None:
            sleeps.append(s)

        policy = _policy(premake_days=9, lock_timeout_ms=100, ddl_retries=2, retry_base_s=0.01)
        report = await PartitionMaintainer(maint, policy, sleep=sleep).run()
        today = await _today(maint)
        stuck = [DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=k)) for k in (8, 9)]
        # Both creates, then the DEFAULT sweep (its DELETE conflicts with SHARE too).
        assert report.deferred == [*stuck, DEFAULT]
        assert report.created == []
        assert report.complete is False
        assert sleeps == [0.01, 0.02] * 3  # capped exponential backoff per statement
        await tx.rollback()
        retry = await PartitionMaintainer(maint, policy, sleep=sleep).run()
        assert retry.created == stuck
        assert retry.complete
    finally:
        await blocker.close()
        await maint.close()


async def test_a_second_runner_is_a_no_op(db_url: str, conn: asyncpg.Connection) -> None:
    holder = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        key = advisory_key(DELIVERY_ATTEMPTS)
        assert await holder.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key)
        reports = await _maintain_all(conn, premake_days=12)
        assert reports["delivery_attempts"].skipped_locked
        assert reports["delivery_attempts"].created == []
        assert len(reports["events"].created) == 5  # only the held table is skipped
        assert len(reports["deliveries"].created) == 5
    finally:
        await holder.close()
    after = await _maintain_all(conn, premake_days=12)
    assert len(after["delivery_attempts"].created) == 5
    assert after["events"].created == []


async def test_advisory_keys_are_distinct_per_table() -> None:
    assert len({advisory_key(t) for t in PARTITIONED_TABLES}) == 3


async def test_the_relay_safety_net_premakes_but_never_retires(conn: asyncpg.Connection) -> None:
    today = await _today(conn)
    old = await _create_partition(conn, today - timedelta(days=40), EVENTS)
    # Simulate the CronJob having stopped: tomorrow's and the day after's partitions are gone.
    for table in reversed(PARTITIONED_TABLES):
        for k in (1, 2):
            name = table.partition_name(today + timedelta(days=k))
            await conn.execute(f"ALTER TABLE {table.name} DETACH PARTITION {name}")
            await conn.execute(f"DROP TABLE {name}")
    reports = await ensure_premade(conn, _policy(), min_days_ahead=2)
    by = {r.table: r for r in reports}
    for table in PARTITIONED_TABLES:
        assert by[table.name].created == [
            table.partition_name(today + timedelta(days=k)) for k in (1, 2)
        ]
    assert await _exists(conn, old)  # premake only: retention stays the CronJob's job
    assert all(r.detached == [] and r.dropped == [] for r in reports)
    # Nothing missing: nothing to do (and nothing created further ahead than asked).
    again = await ensure_premade(conn, _policy(), min_days_ahead=2)
    assert all(r.created == [] for r in again)
    assert not await _exists(conn, EVENTS.partition_name(today + timedelta(days=9)))


async def test_relay_ensure_partitions_premakes_counts_and_warns(
    db_url: str, conn: asyncpg.Connection
) -> None:
    from hookrelay.db.engine import Database  # noqa: PLC0415
    from hookrelay.relay import Relay  # noqa: PLC0415

    today = await _today(conn)
    gone = [today + timedelta(days=k) for k in (1, 2, 3)]
    for table in reversed(PARTITIONED_TABLES):
        for day in gone:
            name = table.partition_name(day)
            await conn.execute(f"ALTER TABLE {table.name} DETACH PARTITION {name}")
            await conn.execute(f"DROP TABLE {name}")
    settings = make_settings(database_url=db_url, relay_partition_min_days_ahead=2)
    database = Database.from_settings(settings)
    metrics = build_metrics("relay")
    try:
        relay = Relay(database, settings, metrics=metrics)
        reports = await relay.ensure_partitions()
        assert {r.table: len(r.created) for r in reports} == {t.name: 2 for t in PARTITIONED_TABLES}
        assert relay.stats.partitions_premade == 6
        assert await _exists(conn, EVENTS.partition_name(today + timedelta(days=2)))
        # Only as far ahead as asked: day 3 stays missing for the CronJob to make.
        assert not await _exists(conn, EVENTS.partition_name(today + timedelta(days=3)))
        assert all(r.created == [] for r in await relay.ensure_partitions())
        assert relay.stats.partitions_premade == 6
    finally:
        await database.dispose()
    body = render_metrics(metrics)[0].decode()
    for table in PARTITIONED_TABLES:
        needle = (
            f'hookrelay_partition_safety_premakes_total{{service="relay",table="{table.name}"}} 2.0'
        )
        assert needle in body


async def test_partition_gauges(db_url: str, conn: asyncpg.Connection) -> None:
    from hookrelay.db.engine import Database  # noqa: PLC0415

    world = await _world(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    g = await _graph(conn, world, now + timedelta(days=50))
    await _attempt(conn, world, g)
    metrics = build_metrics(SERVICE_API)
    database = Database.from_settings(make_settings(database_url=db_url))
    try:
        async with database.session() as session:
            snaps = await refresh_partition_gauges(session, metrics)
    finally:
        await database.dispose()
    assert [s.table for s in snaps] == [t.name for t in PARTITIONED_TABLES]
    for snap in snaps:
        assert snap.partitions == 8
        assert snap.default_rows == 1
        assert 6 * 86_400 < snap.premade_seconds <= 8 * 86_400
    body = render_metrics(metrics)[0].decode()
    for table in PARTITIONED_TABLES:
        assert f'hookrelay_partitions{{service="api",table="{table.name}"}} 8.0' in body
        assert f'hookrelay_partition_default_rows{{service="api",table="{table.name}"}} 1.0' in body
    assert "hookrelay_partition_premade_seconds" in body


# --- entrypoint ---------------------------------------------------------------------------


def _settings(db_url: str, **overrides: Any) -> Settings:
    return make_settings(database_url=db_url, **overrides)


async def test_cli_run_and_exit_codes(db_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(db_url, partition_premake_days=9)
    reports = await partitions_cli.run(settings, dry_run=True)
    assert [r.table for r in reports] == [t.name for t in PARTITIONED_TABLES]
    assert all(len(r.created) == 2 and r.dry_run for r in reports)
    monkeypatch.setattr(partitions_cli, "get_settings", lambda: settings)
    # main() owns its event loop (asyncio.run), so run it off this loop's thread.
    assert await asyncio.to_thread(partitions_cli.main, []) == 0
    assert all(r.created == [] for r in await partitions_cli.run(settings))

    # Not migrated to 0012 yet: a clear exit code instead of a traceback.
    await asyncio.to_thread(command.downgrade, alembic_config(db_url), "0011")
    assert await asyncio.to_thread(partitions_cli.main, []) == partitions_cli.EXIT_NOT_PARTITIONED


async def test_cli_reports_incomplete_runs(db_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    dsn = _asyncpg_dsn(db_url)
    blocker = await asyncpg.connect(dsn)
    try:
        tx = blocker.transaction()
        await tx.start()
        await blocker.execute("LOCK TABLE delivery_attempts IN SHARE MODE")
        settings = _settings(
            db_url, partition_premake_days=8, partition_lock_timeout_ms=50, partition_ddl_retries=0
        )
        monkeypatch.setattr(partitions_cli, "get_settings", lambda: settings)
        rc = await asyncio.to_thread(partitions_cli.main, [])
        assert rc == partitions_cli.EXIT_INCOMPLETE
        await tx.rollback()
    finally:
        await blocker.close()
