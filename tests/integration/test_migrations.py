from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay.db.base import Base
from tests.integration.conftest import _asyncpg_dsn, alembic_config

M1_TABLES = {"api_keys", "endpoints", "event_types", "subscriptions"}
M2_TABLES = {"events", "event_outbox", "idempotency_keys", "deliveries"}
ALL_TABLES = {"tenants", *M1_TABLES, *M2_TABLES}


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
                        sync_conn, opts={"compare_type": True, "compare_server_default": True}
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
    for table in sorted(M1_TABLES | M2_TABLES):
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


def test_each_revision_downgrades_one_step(scratch_database_url: str) -> None:
    """Walk head -> base one revision at a time; every step must succeed and remove its tables."""
    cfg = alembic_config(scratch_database_url)
    script = ScriptDirectory.from_config(cfg)
    command.upgrade(cfg, "head")
    expected_removed = {
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
            "SELECT tgname FROM pg_trigger WHERE tgname = 'trg_deliveries_updated_at'",
        )
    )
    assert len(triggers) == 1
    command.downgrade(cfg, "0004")
    assert not (M2_TABLES & _tables(scratch_database_url))
    assert _has_function(scratch_database_url, "hookrelay_set_updated_at")  # owned by 0001
    command.upgrade(cfg, "head")
    assert _schema_diff(scratch_database_url) == []
