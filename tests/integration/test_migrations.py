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
    assert {"tenants", "alembic_version"} <= _tables(scratch_database_url)
    assert _schema_diff(scratch_database_url) == []


def test_downgrade_to_base_and_back(scratch_database_url: str) -> None:
    cfg = alembic_config(scratch_database_url)
    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")
    assert _version(scratch_database_url) is None
    assert "tenants" not in _tables(scratch_database_url)
    assert not _has_function(scratch_database_url, "hookrelay_set_updated_at")
    command.upgrade(cfg, "head")
    assert "tenants" in _tables(scratch_database_url)
    assert _has_function(scratch_database_url, "hookrelay_set_updated_at")
    assert _schema_diff(scratch_database_url) == []


def test_offline_sql_generation(scratch_database_url: str, capsys) -> None:
    command.upgrade(alembic_config(scratch_database_url), "head", sql=True)
    sql = capsys.readouterr().out
    assert "CREATE TABLE tenants" in sql
    assert "CREATE TRIGGER trg_tenants_updated_at" in sql
