"""Milestone 9c: delivery_attempts range partitioning and its maintenance job, on real Postgres.

Every test migrates its own scratch database (the maintenance job runs DDL and takes advisory
locks, so it must not share the session database the rest of the suite uses).
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from alembic import command

from hookrelay import partitions as partitions_cli
from hookrelay.core.config import Settings
from hookrelay.db.partitioning import DELIVERY_ATTEMPTS, day_bounds
from hookrelay.observability.gauges import refresh_partition_gauges
from hookrelay.observability.metrics import SERVICE_API, build_metrics, render_metrics
from hookrelay.services.partitions import (
    PartitionMaintainer,
    PartitionPolicy,
    PartitionReport,
    advisory_key,
    connect_kwargs,
)
from tests.conftest import make_settings
from tests.integration.conftest import (
    _asyncpg_dsn,
    alembic_config,
    create_scratch_database,
    drop_scratch_database,
)
from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant
from tests.integration.test_m2_schema import _event

PARENT = DELIVERY_ATTEMPTS.name
DEFAULT = DELIVERY_ATTEMPTS.default_partition

_ATTEMPT = (
    "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
    "status_code, error_class, request_url, worker_id, started_at, finished_at, created_at) "
    "VALUES ($1, $2, $3, 'succeeded', 200, NULL, 'https://a.example.com/', 'w', $4, $4, $4) "
    "RETURNING id, tableoid::regclass::text AS partition"
)


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


async def _maintain(conn: asyncpg.Connection, **overrides: Any) -> PartitionReport:
    return await PartitionMaintainer(conn, _policy(**overrides), sleep=_no_sleep).run()


async def _today(conn: asyncpg.Connection) -> date:
    now: datetime = await conn.fetchval("SELECT now()")
    return now.astimezone(UTC).date()


async def _delivery(conn: asyncpg.Connection) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    tenant = await _tenant(conn, f"p-{uuid.uuid4().hex[:8]}")
    await _event_type(conn, tenant, "x.y")
    endpoint = await _endpoint(conn, tenant, f"https://{uuid.uuid4().hex[:8]}.example.com/")
    delivery: uuid.UUID = await conn.fetchval(
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
        "VALUES ($1, $2, $3, now() + interval '1 day') RETURNING id",
        tenant,
        await _event(conn, tenant, "x.y"),
        endpoint,
    )
    return tenant, delivery, endpoint


async def _attempt(
    conn: asyncpg.Connection, tenant: uuid.UUID, delivery: uuid.UUID, n: int, at: datetime
) -> str:
    row = await conn.fetchrow(_ATTEMPT, tenant, delivery, n, at)
    assert row is not None
    return str(row["partition"])


async def _partitions(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch(
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = 'delivery_attempts'::regclass ORDER BY 1"
    )
    return [r["relname"] for r in rows]


async def _exists(conn: asyncpg.Connection, table: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", table))


async def _create_partition(conn: asyncpg.Connection, day: date) -> str:
    """A partition for a past day, as an earlier run would have premade it."""
    name = DELIVERY_ATTEMPTS.partition_name(day)
    lower, upper = day_bounds(day)
    await conn.execute(
        f"CREATE TABLE {name} PARTITION OF delivery_attempts "
        f"FOR VALUES FROM ('{lower.isoformat()}') TO ('{upper.isoformat()}')"
    )
    return name


# --- layout -------------------------------------------------------------------------------


async def test_migration_creates_daily_partitions_and_a_default(conn: asyncpg.Connection) -> None:
    today = await _today(conn)
    expected = {DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=k)) for k in range(8)}
    assert set(await _partitions(conn)) == {*expected, DEFAULT}
    assert (
        await conn.fetchval("SELECT relkind::text FROM pg_class WHERE relname = $1", PARENT) == "p"
    )
    pk = await conn.fetchval(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'pk_delivery_attempts'"
    )
    assert pk == "PRIMARY KEY (id, created_at)"
    uq = await conn.fetchval(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conname = 'uq_delivery_attempts_number' AND conrelid = 'delivery_attempts'::regclass"
    )
    assert uq == "UNIQUE (delivery_id, attempt_number, created_at)"
    # Every partition carries the FKs, the indexes and the endpoint trigger.
    part = DELIVERY_ATTEMPTS.partition_name(today)
    fks = await conn.fetchval(
        "SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'f'", part
    )
    assert fks == 3
    indexes = await conn.fetchval("SELECT count(*) FROM pg_indexes WHERE tablename = $1", part)
    assert indexes == 6  # pk + unique + 4 secondary
    trigger = await conn.fetchval(
        "SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal", part
    )
    assert trigger == 1


async def test_inserts_land_in_their_day_or_the_default_partition(
    conn: asyncpg.Connection,
) -> None:
    tenant, delivery, endpoint = await _delivery(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    today = now.astimezone(UTC).date()
    assert await _attempt(conn, tenant, delivery, 1, now) == DELIVERY_ATTEMPTS.partition_name(today)
    tomorrow = day_bounds(today + timedelta(days=1))[0]
    assert await _attempt(conn, tenant, delivery, 2, tomorrow) == DELIVERY_ATTEMPTS.partition_name(
        today + timedelta(days=1)
    )
    # Beyond the premade horizon (or long ago): the DEFAULT partition, never an error.
    assert await _attempt(conn, tenant, delivery, 3, now + timedelta(days=40)) == DEFAULT
    assert await _attempt(conn, tenant, delivery, 4, now - timedelta(days=400)) == DEFAULT
    # The BEFORE INSERT trigger still fills endpoint_id on a partitioned table.
    endpoints = await conn.fetch("SELECT DISTINCT endpoint_id FROM delivery_attempts")
    assert [r["endpoint_id"] for r in endpoints] == [endpoint]
    # Server default created_at routes to today's partition.
    row = await conn.fetchrow(
        "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
        "status_code, request_url, worker_id, started_at, finished_at) "
        "VALUES ($1, $2, 5, 'succeeded', 204, 'https://a.example.com/', 'w', now(), now()) "
        "RETURNING tableoid::regclass::text AS partition",
        tenant,
        delivery,
    )
    assert row is not None
    assert row["partition"] == DELIVERY_ATTEMPTS.partition_name(today)


async def test_unknown_delivery_is_reported_by_the_delivery_fk(conn: asyncpg.Connection) -> None:
    tenant, _, _ = await _delivery(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    with pytest.raises(asyncpg.ForeignKeyViolationError) as exc:
        await _attempt(conn, tenant, uuid.uuid4(), 1, now)
    assert exc.value.constraint_name == "fk_delivery_attempts_delivery_same_tenant"


async def test_deleting_a_delivery_cascades_into_every_partition(conn: asyncpg.Connection) -> None:
    tenant, delivery, _ = await _delivery(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    for n, at in enumerate((now, now + timedelta(days=1), now + timedelta(days=60)), start=1):
        await _attempt(conn, tenant, delivery, n, at)
    await conn.execute("DELETE FROM deliveries WHERE id = $1", delivery)
    assert await conn.fetchval("SELECT count(*) FROM delivery_attempts") == 0


async def test_time_windowed_reads_prune_to_their_partitions(conn: asyncpg.Connection) -> None:
    today = await _today(conn)
    lower, upper = day_bounds(today)
    window = f"created_at >= '{lower.isoformat()}' AND created_at < '{upper.isoformat()}'"
    sql = f"EXPLAIN SELECT count(*) FROM delivery_attempts WHERE {window}"  # noqa: S608
    plan = "\n".join(r[0] for r in await conn.fetch(sql))
    assert DELIVERY_ATTEMPTS.partition_name(today) in plan
    assert DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=1)) not in plan
    assert DEFAULT not in plan  # the default holds nothing inside a covered range
    # now()-relative windows (endpoint health, dashboard) prune at executor start-up: older
    # partitions are skipped (the future premade ones are empty and cost one index probe).
    old = await _create_partition(conn, today - timedelta(days=3))
    await _create_partition(conn, today - timedelta(days=2))
    stable = "\n".join(
        r[0]
        for r in await conn.fetch(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) SELECT count(*) "
            "FROM delivery_attempts WHERE endpoint_id = $1 "
            "AND created_at >= now() - interval '1 hour'",
            uuid.uuid4(),
        )
    )
    assert "Subplans Removed: 2" in stable
    assert old not in stable
    # A per-delivery lookup cannot prune by time: one index probe per partition (documented).
    per_delivery = "\n".join(
        r[0]
        for r in await conn.fetch(
            "EXPLAIN SELECT * FROM delivery_attempts WHERE delivery_id = $1", uuid.uuid4()
        )
    )
    assert per_delivery.count("ix_delivery_attempts") + per_delivery.count("_idx") >= 9


async def test_attempt_numbers_are_unique_per_partition_only(conn: asyncpg.Connection) -> None:
    """The documented trade-off: Postgres needs created_at in the unique key, so the same
    (delivery, attempt number) is rejected within a day but not across two days."""
    tenant, delivery, _ = await _delivery(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    await _attempt(conn, tenant, delivery, 1, now)
    with pytest.raises(asyncpg.UniqueViolationError):
        await _attempt(conn, tenant, delivery, 1, now)
    other_day = now + timedelta(days=1)
    assert await _attempt(conn, tenant, delivery, 1, other_day)  # not caught by the database


# --- maintenance --------------------------------------------------------------------------


async def test_premake_is_idempotent_and_extends_the_horizon(conn: asyncpg.Connection) -> None:
    first = await _maintain(conn)
    assert first.created == []
    assert first.complete
    assert first.partitions == 8
    today = await _today(conn)
    assert first.premade_until == day_bounds(today + timedelta(days=7))[1]
    longer = await _maintain(conn, premake_days=10)
    assert longer.created == [
        DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=k)) for k in (8, 9, 10)
    ]
    again = await _maintain(conn, premake_days=10)
    assert again.created == []
    assert again.partitions == 11
    # The partitions it creates are complete: indexes, FKs and the trigger came with ATTACH.
    new = DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=10))
    assert await conn.fetchval("SELECT count(*) FROM pg_indexes WHERE tablename = $1", new) == 6
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'f'",
            new,
        )
        == 3
    )
    # ... and the temporary bound CHECK is gone.
    checks = await conn.fetch(
        "SELECT conname FROM pg_constraint WHERE conrelid = $1::regclass AND conname LIKE '%bound'",
        new,
    )
    assert checks == []


async def test_rows_in_the_default_partition_move_into_a_new_partition(
    conn: asyncpg.Connection,
) -> None:
    tenant, delivery, _ = await _delivery(conn)
    today = await _today(conn)
    far = day_bounds(today + timedelta(days=20))[0] + timedelta(hours=3)
    assert await _attempt(conn, tenant, delivery, 1, far) == DEFAULT
    assert await _attempt(conn, tenant, delivery, 2, far + timedelta(minutes=1)) == DEFAULT
    report = await _maintain(conn)
    target = DELIVERY_ATTEMPTS.partition_name(today + timedelta(days=20))
    assert report.created == [target]
    assert report.moved_from_default == 2
    assert report.default_rows == 0
    rows = await conn.fetch("SELECT tableoid::regclass::text AS p FROM delivery_attempts")
    assert {r["p"] for r in rows} == {target}


async def test_retention_drops_old_partitions_and_sweeps_the_default(
    conn: asyncpg.Connection,
) -> None:
    tenant, delivery, _ = await _delivery(conn)
    today = await _today(conn)
    old_day, kept_day = today - timedelta(days=31), today - timedelta(days=29)
    old, kept = await _create_partition(conn, old_day), await _create_partition(conn, kept_day)
    assert await _attempt(conn, tenant, delivery, 1, day_bounds(old_day)[0]) == old
    assert await _attempt(conn, tenant, delivery, 2, day_bounds(kept_day)[0]) == kept
    now: datetime = await conn.fetchval("SELECT now()")
    for n in range(3, 8):  # 5 stale rows in DEFAULT: swept in batches of 2
        assert await _attempt(conn, tenant, delivery, n, now - timedelta(days=90 + n)) == DEFAULT
    # A table a crashed run detached but never dropped.
    orphan = DELIVERY_ATTEMPTS.partition_name(today - timedelta(days=45))
    await conn.execute(f"CREATE TABLE {orphan} (LIKE delivery_attempts)")

    dry = await _maintain(conn, dry_run=True)
    assert dry.detached == [old]
    assert set(dry.dropped) == {old, orphan}
    assert await _exists(conn, old)

    report = await _maintain(conn)
    assert report.detached == [old]
    assert report.detach_mode == "lock"  # a DEFAULT partition rules out CONCURRENTLY
    assert report.dropped == [old, orphan]
    assert report.default_rows_deleted == 5
    assert report.complete
    assert report.cutoff == day_bounds(today - timedelta(days=30))[0]
    assert not await _exists(conn, old)
    assert not await _exists(conn, orphan)
    assert await _exists(conn, kept)
    left = await conn.fetch("SELECT attempt_number FROM delivery_attempts ORDER BY 1")
    assert [r["attempt_number"] for r in left] == [2]


async def test_without_a_default_partition_detach_is_concurrent(conn: asyncpg.Connection) -> None:
    await conn.execute(f"DROP TABLE {DEFAULT}")
    today = await _today(conn)
    old = await _create_partition(conn, today - timedelta(days=40))
    report = await _maintain(conn)
    assert report.detached == [old]
    assert report.dropped == [old]
    assert report.detach_mode == "concurrently"
    assert report.default_rows == 0


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
        report = await _maintain(conn, premake_days=12)
        assert report.skipped_locked
        assert report.created == []
        assert report.complete
    finally:
        await holder.close()
    after = await _maintain(conn, premake_days=12)
    assert len(after.created) == 5


async def test_partition_gauges(db_url: str, conn: asyncpg.Connection) -> None:
    from hookrelay.db.engine import Database  # noqa: PLC0415

    tenant, delivery, _ = await _delivery(conn)
    now: datetime = await conn.fetchval("SELECT now()")
    await _attempt(conn, tenant, delivery, 1, now + timedelta(days=50))
    metrics = build_metrics(SERVICE_API)
    database = Database.from_settings(make_settings(database_url=db_url))
    try:
        async with database.session() as session:
            snap = await refresh_partition_gauges(session, metrics)
    finally:
        await database.dispose()
    assert snap.partitions == 8
    assert snap.default_rows == 1
    assert 6 * 86_400 < snap.premade_seconds <= 8 * 86_400
    body = render_metrics(metrics)[0].decode()
    assert 'hookrelay_partitions{service="api",table="delivery_attempts"} 8.0' in body
    assert 'hookrelay_partition_default_rows{service="api",table="delivery_attempts"} 1.0' in body
    assert "hookrelay_partition_premade_seconds" in body


# --- entrypoint ---------------------------------------------------------------------------


def _settings(db_url: str, **overrides: Any) -> Settings:
    return make_settings(database_url=db_url, **overrides)


async def test_cli_run_and_exit_codes(db_url: str, monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _settings(db_url, partition_premake_days=9)
    report = await partitions_cli.run(settings, dry_run=True)
    assert len(report.created) == 2
    assert report.dry_run
    monkeypatch.setattr(partitions_cli, "get_settings", lambda: settings)
    # main() owns its event loop (asyncio.run), so run it off this loop's thread.
    assert await asyncio.to_thread(partitions_cli.main, []) == 0
    assert (await partitions_cli.run(settings)).created == []

    # Not migrated to 0011 yet: a clear exit code instead of a traceback.
    await asyncio.to_thread(command.downgrade, alembic_config(db_url), "0010")
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
