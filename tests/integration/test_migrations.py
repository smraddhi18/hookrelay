from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay.db.base import Base
from hookrelay.db.partitioning import include_name, include_object
from tests.integration.conftest import _asyncpg_dsn, alembic_config

M1_TABLES = {"api_keys", "endpoints", "event_types", "subscriptions"}
M2_TABLES = {"events", "event_outbox", "idempotency_keys", "deliveries"}
M3_TABLES = {"delivery_attempts"}
ALL_TABLES = {"tenants", *M1_TABLES, *M2_TABLES, *M3_TABLES}


async def _fetch(url: str, sql: str, *args: object) -> list[asyncpg.Record]:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        return await conn.fetch(sql, *args)
    finally:
        await conn.close()


def _tables(url: str) -> set[str]:
    rows = asyncio.run(_fetch(url, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"))
    return {r["tablename"] for r in rows}


def _version(url: str) -> str | None:
    if "alembic_version" not in _tables(url):
        return None
    rows = asyncio.run(_fetch(url, "SELECT version_num FROM alembic_version"))
    return rows[0]["version_num"] if rows else None


def _has_function(url: str, name: str) -> bool:
    rows = asyncio.run(_fetch(url, "SELECT 1 FROM pg_proc WHERE proname = $1", name))
    return bool(rows)


def _schema_diff(url: str) -> list[Any]:
    async def run() -> list[Any]:
        engine = create_async_engine(url, poolclass=pool.NullPool)
        try:
            async with engine.connect() as conn:

                def compare(sync_conn: Connection) -> list[Any]:
                    ctx = MigrationContext.configure(
                        sync_conn,
                        opts={
                            "compare_type": True,
                            "compare_server_default": True,
                            "include_name": include_name,  # as alembic/env.py
                            "include_object": include_object,
                        },
                    )
                    return list(compare_metadata(ctx, Base.metadata))

                return await conn.run_sync(compare)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def test_single_linear_history() -> None:
    script = ScriptDirectory.from_config(alembic_config("postgresql://x@localhost/x"))
    assert len(script.get_heads()) == 1
    assert len(script.get_bases()) == 1


def test_upgrade_is_idempotent_and_matches_models(scratch_database_url: str) -> None:
    cfg = alembic_config(scratch_database_url)
    head = ScriptDirectory.from_config(cfg).get_current_head()
    command.upgrade(cfg, "head")
    command.upgrade(cfg, "head")  # second run must be a no-op
    assert _version(scratch_database_url) == head
    assert {"alembic_version", *ALL_TABLES} <= _tables(scratch_database_url)
    assert _schema_diff(scratch_database_url) == []


def test_downgrade_to_base_and_back(scratch_database_url: str) -> None:
    cfg = alembic_config(scratch_database_url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")
    assert _version(scratch_database_url) is None
    assert not (ALL_TABLES & _tables(scratch_database_url))
    assert not _has_function(scratch_database_url, "hookrelay_set_updated_at")
    command.upgrade(cfg, "head")
    assert _tables(scratch_database_url) >= ALL_TABLES
    assert _has_function(scratch_database_url, "hookrelay_set_updated_at")
    assert _schema_diff(scratch_database_url) == []


def test_offline_sql_generation(scratch_database_url: str, capsys) -> None:
    command.upgrade(alembic_config(scratch_database_url), "head", sql=True)
    sql = capsys.readouterr().out
    assert "CREATE TABLE tenants" in sql
    assert "CREATE TRIGGER trg_tenants_updated_at" in sql
    for table in sorted(M1_TABLES | M2_TABLES | M3_TABLES):
        assert f"CREATE TABLE {table}" in sql
    for partial_index in (
        "uq_api_keys_rotated_from_id",
        "uq_endpoints_tenant_url_live",
        "uq_events_tenant_external_id",
    ):
        assert f"CREATE UNIQUE INDEX {partial_index}" in sql
    assert "fk_subscriptions_endpoint_same_tenant" in sql
    # M2: same-tenant composite FKs, the outbox's partial claim index and the delivery trigger.
    for name in (
        "fk_events_event_type_same_tenant",
        "fk_event_outbox_event_same_tenant",
        "fk_idempotency_keys_event_same_tenant",
        "fk_deliveries_event_same_tenant",
        "fk_deliveries_endpoint_same_tenant",
        "uq_deliveries_event_endpoint",
    ):
        assert name in sql
    assert "CREATE INDEX ix_event_outbox_pending" in sql
    assert "WHERE processed_at IS NULL" in sql
    assert "CREATE TRIGGER trg_deliveries_updated_at" in sql
    # M3: lease columns + fencing CHECK, the lease-expiry index and the attempt log's FK.
    for name in (
        "ck_deliveries_lease_iff_delivering",
        "uq_deliveries_tenant_id_id",
        "fk_delivery_attempts_delivery_same_tenant",
        "uq_delivery_attempts_number",
    ):
        assert name in sql
    assert "CREATE INDEX ix_deliveries_lease_expiry" in sql
    assert "ADD COLUMN lease_token UUID" in sql


def test_each_revision_downgrades_one_step(scratch_database_url: str) -> None:
    """Walk head -> base one revision at a time; every step must succeed and remove its tables."""
    cfg = alembic_config(scratch_database_url)
    script = ScriptDirectory.from_config(cfg)
    command.upgrade(cfg, "head")
    expected_removed = {
        "0012": set(),  # events/deliveries/delivery_attempts swapped for partitioned copies
        "0011": set(),  # delivery_attempts swapped for an unpartitioned copy (same name)
        "0010": set(),  # traceparent/tracestate columns only
        "0009": set(),  # columns, indexes and triggers only
        "0008": {"endpoint_states", "notifications", "replay_requests"},
        "0007": {"delivery_attempts"},
        "0006": {"deliveries"},
        "0005": {"events", "event_outbox", "idempotency_keys"},
        "0004": {"event_types", "subscriptions"},
        "0003": {"endpoints"},
        "0002": {"api_keys"},
        "0001": {"tenants"},
    }
    for revision in script.walk_revisions():  # newest first
        before = _tables(scratch_database_url)
        command.downgrade(cfg, "-1")
        removed = before - _tables(scratch_database_url)
        assert expected_removed[revision.revision] <= removed, revision.revision
        assert _version(scratch_database_url) == revision.down_revision
    command.upgrade(cfg, "head")
    assert _schema_diff(scratch_database_url) == []


def test_m2_downgrade_drops_delivery_trigger(scratch_database_url: str) -> None:
    cfg = alembic_config(scratch_database_url)
    command.upgrade(cfg, "head")
    triggers = asyncio.run(
        _fetch(
            scratch_database_url,
            "SELECT tgname FROM pg_trigger WHERE tgname = 'trg_deliveries_updated_at' "
            "AND tgparentid = 0",  # partitions carry cloned copies
        )
    )
    assert len(triggers) == 1
    command.downgrade(cfg, "0004")
    assert not (M2_TABLES & _tables(scratch_database_url))
    assert _has_function(scratch_database_url, "hookrelay_set_updated_at")  # owned by 0001
    command.upgrade(cfg, "head")
    assert _schema_diff(scratch_database_url) == []


async def _seed_m3_rows(url: str) -> None:
    from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant  # noqa: PLC0415
    from tests.integration.test_m2_schema import _event  # noqa: PLC0415

    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        tenant = await _tenant(conn, "m3-downgrade")
        await _event_type(conn, tenant, "x.y")
        insert = (
            "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, status, "
            "completed_at, lease_token, leased_until, leased_by, event_created_at) "
            "VALUES ($1, $2, $3, now() + interval '1 day', $4, $5, $6, $7, $8, "
            "COALESCE((SELECT created_at FROM events WHERE id = $2), now()))"
        )
        endpoint = await _endpoint(conn, tenant, "https://a.example.com/")
        failed = (await _event(conn, tenant, "x.y"), "failed", "now", None, None, None)
        leased = (await _event(conn, tenant, "x.y"), "delivering", None, "lease", "now", "w")
        for event_id, status, completed, token, until, by in (failed, leased):
            await conn.execute(
                insert.replace("$5", "now()" if completed else "NULL::timestamptz")
                .replace("$6", "gen_random_uuid()" if token else "NULL::uuid")
                .replace("$7", "now()" if until else "NULL::timestamptz")
                .replace("$8", "$5"),
                tenant,
                event_id,
                endpoint,
                status,
                by,
            )
    finally:
        await conn.close()


def test_m3_downgrade_maps_failed_and_in_flight_rows(scratch_database_url: str) -> None:
    cfg = alembic_config(scratch_database_url)
    command.upgrade(cfg, "head")
    asyncio.run(_seed_m3_rows(scratch_database_url))
    command.downgrade(cfg, "0006")
    rows = asyncio.run(_fetch(scratch_database_url, "SELECT status FROM deliveries ORDER BY 1"))
    assert [r["status"] for r in rows] == ["dead", "pending"]
    assert "delivery_attempts" not in _tables(scratch_database_url)
    command.upgrade(cfg, "head")
    assert _schema_diff(scratch_database_url) == []


async def _seed_m4_rows(url: str) -> None:
    """Terminal ``failed`` rows as 0007 left them: two exhausted retries, one permanent 4xx."""
    from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant  # noqa: PLC0415
    from tests.integration.test_m2_schema import _event  # noqa: PLC0415

    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        tenant = await _tenant(conn, "m4-upgrade")
        await _event_type(conn, tenant, "x.y")
        endpoint = await _endpoint(conn, tenant, "https://a.example.com/")
        for error_class, status_code in (
            ("http_status", 503),
            ("timeout", None),
            ("http_status", 410),
        ):
            await conn.execute(
                "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, status, "
                "completed_at, attempt_count, last_error_class, last_status_code) "
                "VALUES ($1, $2, $3, now() + interval '1 day', 'failed', now(), 1, $4, $5)",
                tenant,
                await _event(conn, tenant, "x.y"),
                endpoint,
                error_class,
                status_code,
            )
    finally:
        await conn.close()


def test_m4_upgrade_dead_letters_exhausted_rows_and_backfills_state(
    scratch_database_url: str,
) -> None:
    cfg = alembic_config(scratch_database_url)
    command.upgrade(cfg, "0007")
    asyncio.run(_seed_m4_rows(scratch_database_url))
    command.upgrade(cfg, "head")
    rows = asyncio.run(
        _fetch(
            scratch_database_url,
            "SELECT status, last_error_class, run_started_at = created_at AS backfilled "
            "FROM deliveries ORDER BY last_status_code NULLS LAST",
        )
    )
    assert [(r["status"], r["last_error_class"]) for r in rows] == [
        ("failed", "http_status"),  # 410: permanent, not a dead letter
        ("dead", "http_status"),
        ("dead", "timeout"),
    ]
    assert all(r["backfilled"] for r in rows)
    states = asyncio.run(_fetch(scratch_database_url, "SELECT breaker_state FROM endpoint_states"))
    assert [s["breaker_state"] for s in states] == ["closed"]

    command.downgrade(cfg, "0007")
    rows = asyncio.run(_fetch(scratch_database_url, "SELECT DISTINCT status FROM deliveries"))
    assert [r["status"] for r in rows] == ["failed"]
    command.upgrade(cfg, "head")
    assert _schema_diff(scratch_database_url) == []


async def _seed_attempts_across_days(url: str) -> list[tuple[int, str]]:
    """Attempts on three UTC days (one far in the past) as 0010 stored them."""
    from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant  # noqa: PLC0415
    from tests.integration.test_m2_schema import _event  # noqa: PLC0415

    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        tenant = await _tenant(conn, "m9c-partitions")
        await _event_type(conn, tenant, "x.y")
        endpoint = await _endpoint(conn, tenant, "https://a.example.com/")
        delivery = await conn.fetchval(
            "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, "
            "attempt_count) VALUES ($1, $2, $3, now() + interval '1 day', 3) RETURNING id",
            tenant,
            await _event(conn, tenant, "x.y"),
            endpoint,
        )
        now = await conn.fetchval("SELECT now()")
        old = datetime(2026, 1, 2, 23, 59, 59, 500_000, tzinfo=UTC)
        for n, at in enumerate((old, now - timedelta(days=1), now), start=1):
            await conn.execute(
                "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
                "status_code, error_class, request_url, worker_id, started_at, finished_at, "
                "created_at) VALUES ($1, $2, $3, 'retrying', 503, 'http_status', "
                "'https://a.example.com/', 'w', $4, $4, $4)",
                tenant,
                delivery,
                n,
                at,
            )
        rows = await conn.fetch(
            "SELECT attempt_number, to_char(created_at AT TIME ZONE 'UTC', 'YYYYMMDD') AS day "
            "FROM delivery_attempts ORDER BY attempt_number"
        )
        return [(r["attempt_number"], r["day"]) for r in rows]
    finally:
        await conn.close()


def test_m9c_partitioning_roundtrip_keeps_every_attempt(scratch_database_url: str) -> None:
    url = scratch_database_url
    cfg = alembic_config(url)
    command.upgrade(cfg, "0010")
    seeded = asyncio.run(_seed_attempts_across_days(url))
    command.upgrade(cfg, "0011")
    rows = asyncio.run(
        _fetch(
            url,
            "SELECT attempt_number, tableoid::regclass::text AS partition "
            "FROM delivery_attempts ORDER BY attempt_number",
        )
    )
    # Each row sits in the daily partition of its UTC day (one was created for the old day).
    assert [(r["attempt_number"], r["partition"]) for r in rows] == [
        (n, f"delivery_attempts_p{day}") for n, day in seeded
    ]
    kind = asyncio.run(
        _fetch(url, "SELECT relkind::text AS k FROM pg_class WHERE relname = 'delivery_attempts'")
    )
    assert kind[0]["k"] == "p"
    assert "delivery_attempts_m0010" not in _tables(url)

    command.downgrade(cfg, "0010")
    back = asyncio.run(
        _fetch(
            url,
            "SELECT attempt_number, to_char(created_at AT TIME ZONE 'UTC', 'YYYYMMDD') AS day "
            "FROM delivery_attempts ORDER BY attempt_number",
        )
    )
    assert [(r["attempt_number"], r["day"]) for r in back] == seeded
    kind = asyncio.run(
        _fetch(url, "SELECT relkind::text AS k FROM pg_class WHERE relname = 'delivery_attempts'")
    )
    assert kind[0]["k"] == "r"
    leftovers = asyncio.run(
        _fetch(url, "SELECT relname FROM pg_class WHERE relname LIKE 'delivery_attempts_%'")
    )
    assert [r["relname"] for r in leftovers if "_p" in r["relname"] or "_m0" in r["relname"]] == []
    pk = asyncio.run(
        _fetch(
            url,
            "SELECT pg_get_constraintdef(oid) AS d FROM pg_constraint "
            "WHERE conname = 'pk_delivery_attempts'",
        )
    )
    assert pk[0]["d"] == "PRIMARY KEY (id)"
    command.upgrade(cfg, "head")
    assert _schema_diff(url) == []


async def _seed_event_graph_0011(url: str) -> dict[str, Any]:
    """Three events on three UTC days (one 40 days old) with external ids, an idempotency key,
    outbox rows, deliveries and attempts, in the 0011 shape (attempts keyed by their own day)."""
    from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant  # noqa: PLC0415

    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        tenant = await _tenant(conn, "m9d-roundtrip")
        await _event_type(conn, tenant, "x.y")
        endpoint = await _endpoint(conn, tenant, "https://a.example.com/")
        now = await conn.fetchval("SELECT now()")
        events: list[dict[str, Any]] = []
        for label, age_days, external in (
            ("old", 40, "ext-old"),
            ("mid", 1, "ext-mid"),
            ("new", 0, None),
        ):
            created = now - timedelta(days=age_days)
            event_id = await conn.fetchval(
                "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
                "created_at, retain_until, external_id) "
                "VALUES ($1, 'x.y', '{}', 2, $2::timestamptz, $2::timestamptz, "
                "$2::timestamptz + interval '90 days', $3) RETURNING id",
                tenant,
                created,
                external,
            )
            delivery_id = await conn.fetchval(
                "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, "
                "attempt_count) VALUES ($1, $2, $3, $4, 2) RETURNING id",
                tenant,
                event_id,
                endpoint,
                created + timedelta(days=90),
            )
            for n in (1, 2):
                # The second attempt happens two days after the event (a retry / replay).
                at = created + timedelta(days=2 * (n - 1))
                await conn.execute(
                    "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, "
                    "outcome, status_code, error_class, request_url, worker_id, started_at, "
                    "finished_at, created_at) VALUES ($1, $2, $3, 'retrying', 503, "
                    "'http_status', 'https://a.example.com/', 'w', $4, $4, $4)",
                    tenant,
                    delivery_id,
                    n,
                    at,
                )
            await conn.execute(
                "INSERT INTO event_outbox (tenant_id, event_id, processed_at) "
                "VALUES ($1, $2, CASE WHEN $3 THEN now() END)",
                tenant,
                event_id,
                label != "new",
            )
            events.append({"label": label, "id": event_id, "created": created, "ext": external})
        await conn.execute(
            "INSERT INTO idempotency_keys (tenant_id, key, request_method, request_path, "
            "fingerprint, expires_at, event_id, response_status, response_body) "
            "VALUES ($1, 'k-old', 'POST', '/v1/events', $2, now() + interval '1 day', $3, 202, '{}')",
            tenant,
            b"\x00" * 32,
            events[0]["id"],
        )
        return {"tenant": tenant, "events": events}
    finally:
        await conn.close()


def _assert_graph_0012(url: str, seeded: dict[str, Any]) -> None:
    """Every row is there and sits in the partition of its EVENT's UTC day."""
    events = {str(e["id"]): e for e in seeded["events"]}

    def day(moment: datetime) -> str:
        return moment.astimezone(UTC).strftime("%Y%m%d")

    for r in asyncio.run(
        _fetch(url, "SELECT id, created_at, tableoid::regclass::text AS p FROM events")
    ):
        assert r["p"] == f"events_p{day(events[str(r['id'])]['created'])}"
    rows = asyncio.run(
        _fetch(
            url,
            "SELECT d.event_id, d.event_created_at, e.created_at, "
            "d.tableoid::regclass::text AS p FROM deliveries d "
            "JOIN events e ON e.id = d.event_id AND e.created_at = d.event_created_at",
        )
    )
    assert len(rows) == 3
    for r in rows:
        assert r["p"] == f"deliveries_p{day(events[str(r['event_id'])]['created'])}"
    attempts = asyncio.run(
        _fetch(
            url,
            "SELECT d.event_id, a.attempt_number, a.created_at, "
            "a.tableoid::regclass::text AS p FROM delivery_attempts a "
            "JOIN deliveries d ON d.id = a.delivery_id AND d.event_created_at = a.event_created_at",
        )
    )
    assert len(attempts) == 6
    for r in attempts:  # the retry made two days later still lives with its event's day
        assert r["p"] == f"delivery_attempts_p{day(events[str(r['event_id'])]['created'])}"
    outbox = asyncio.run(
        _fetch(
            url,
            "SELECT count(*) AS n FROM event_outbox o JOIN events e "
            "ON e.id = o.event_id AND e.created_at = o.event_created_at",
        )
    )
    assert outbox[0]["n"] == 3
    claims = asyncio.run(
        _fetch(
            url,
            "SELECT x.external_id, x.expires_at = e.retain_until AS same FROM event_external_ids x "
            "JOIN events e ON e.id = x.event_id AND e.created_at = x.event_created_at "
            "ORDER BY x.external_id",
        )
    )
    assert [(c["external_id"], c["same"]) for c in claims] == [("ext-mid", True), ("ext-old", True)]
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM idempotency_keys"))) == 1


def test_m9d_partitioned_event_graph_roundtrip_keeps_every_row(scratch_database_url: str) -> None:
    url = scratch_database_url
    cfg = alembic_config(url)
    command.upgrade(cfg, "0011")
    seeded = asyncio.run(_seed_event_graph_0011(url))

    command.upgrade(cfg, "0012")
    _assert_graph_0012(url, seeded)
    assert _schema_diff(url) == []
    kinds = asyncio.run(
        _fetch(
            url,
            "SELECT relname, relkind::text AS k FROM pg_class WHERE relname IN "
            "('events', 'deliveries', 'delivery_attempts', 'events_m0011', 'deliveries_m0011')",
        )
    )
    assert {r["relname"]: r["k"] for r in kinds} == {
        "events": "p",
        "deliveries": "p",
        "delivery_attempts": "p",
    }
    # idempotency_keys no longer reference events: expiring the event leaves the record alone,
    # and the downgrade drops such dangling records instead of failing on the old FK.
    old_id = seeded["events"][0]["id"]
    asyncio.run(_fetch(url, "DELETE FROM events WHERE id = $1", old_id))
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM idempotency_keys"))) == 1

    command.downgrade(cfg, "0011")
    assert (
        asyncio.run(
            _fetch(url, "SELECT relkind::text AS k FROM pg_class WHERE relname = 'events'")
        )[0]["k"]
        == "r"
    )
    assert (
        asyncio.run(
            _fetch(
                url, "SELECT relkind::text AS k FROM pg_class WHERE relname = 'delivery_attempts'"
            )
        )[0]["k"]
        == "p"
    )
    assert "event_external_ids" not in _tables(url)
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM events"))) == 2
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM deliveries"))) == 2
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM delivery_attempts"))) == 4
    assert asyncio.run(_fetch(url, "SELECT 1 FROM idempotency_keys")) == []  # dangling: dropped
    ext = asyncio.run(_fetch(url, "SELECT external_id FROM events ORDER BY created_at"))
    assert [r["external_id"] for r in ext] == ["ext-mid", None]
    # The 0011 unique index is back and enforces uniqueness again.
    with pytest.raises(asyncpg.UniqueViolationError):
        asyncio.run(
            _fetch(
                url,
                "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
                "retain_until, external_id) SELECT tenant_id, 'x.y', '{}', 2, now(), "
                "now() + interval '1 day', 'ext-mid' FROM events LIMIT 1",
            )
        )

    command.upgrade(cfg, "0012")
    survivors = [e for e in seeded["events"] if e["id"] != old_id]
    _assert_survivors_0012(url, survivors)
    assert _schema_diff(url) == []


def _assert_survivors_0012(url: str, survivors: list[dict[str, Any]]) -> None:
    ids = {str(e["id"]) for e in survivors}
    assert {str(r["id"]) for r in asyncio.run(_fetch(url, "SELECT id FROM events"))} == ids
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM deliveries"))) == 2
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM delivery_attempts"))) == 4
    claims = asyncio.run(_fetch(url, "SELECT external_id FROM event_external_ids"))
    assert [c["external_id"] for c in claims] == ["ext-mid"]
    mismatched = asyncio.run(
        _fetch(
            url,
            "SELECT 1 FROM delivery_attempts a JOIN deliveries d ON d.id = a.delivery_id "
            "WHERE a.event_created_at <> d.event_created_at",
        )
    )
    assert mismatched == []


def test_m9d_downgrade_refuses_to_lose_a_duplicate_external_id(
    scratch_database_url: str,
) -> None:
    """An external id reclaimed while the expired event still waits for its partition drop
    exists twice; the 0011 unique index cannot hold both, so the downgrade stops."""
    url = scratch_database_url
    cfg = alembic_config(url)
    command.upgrade(cfg, "0012")
    seeded = asyncio.run(_seed_event_graph_0012(url))
    assert seeded
    with pytest.raises(Exception, match=r"external_id|unique|duplicate"):
        command.downgrade(cfg, "0011")
    assert _version(url) == "0012"  # the failed downgrade rolled back completely
    assert len(asyncio.run(_fetch(url, "SELECT 1 FROM events WHERE external_id = 'dup'"))) == 2


async def _seed_event_graph_0012(url: str) -> bool:
    from tests.integration.test_m1_schema import _event_type, _tenant  # noqa: PLC0415

    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        tenant = await _tenant(conn, "m9d-dup")
        await _event_type(conn, tenant, "x.y")
        for age in (3, 0):
            await conn.execute(
                "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
                "created_at, retain_until, external_id) VALUES ($1, 'x.y', '{}', 2, now(), "
                "now() - make_interval(days => $2), now() + interval '1 day', 'dup')",
                tenant,
                age,
            )
        return True
    finally:
        await conn.close()
