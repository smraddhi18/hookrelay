from __future__ import annotations

import time

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import AsyncAdaptedQueuePool

from hookrelay.db.engine import Database
from hookrelay.db.models import Tenant
from hookrelay.main import create_app
from tests.conftest import make_settings, serve


async def test_ping(database: Database) -> None:
    await database.ping(timeout_s=2)


async def test_connection_settings_applied(database: Database) -> None:
    async with database.engine.connect() as conn:
        timeout = (await conn.execute(text("SHOW statement_timeout"))).scalar_one()
        app_name = (await conn.execute(text("SHOW application_name"))).scalar_one()
    assert timeout == "15s"
    assert app_name == "hookrelay"


async def test_statement_timeout_cancels_runaway_query(migrated_database_url: str) -> None:
    db = Database.from_settings(
        make_settings(database_url=migrated_database_url, db_statement_timeout_ms=200)
    )
    try:
        started = time.perf_counter()
        with pytest.raises(DBAPIError, match="statement timeout"):
            async with db.engine.connect() as conn:
                await conn.execute(text("SELECT pg_sleep(5)"))
        assert time.perf_counter() - started < 3
    finally:
        await db.dispose()


async def test_session_commits_on_success(database: Database) -> None:
    async with database.session() as session:
        session.add(Tenant(slug="commit-ok", name="Commit OK"))
    async with database.session() as session:
        tenant = (
            await session.execute(select(Tenant).where(Tenant.slug == "commit-ok"))
        ).scalar_one()
        assert tenant.status == "active"
        await session.delete(tenant)


async def test_session_rolls_back_on_error(database: Database) -> None:
    class BoomError(Exception):
        pass

    async def failing_unit_of_work() -> None:
        async with database.session() as session:
            session.add(Tenant(slug="rolled-back", name="Nope"))
            await session.flush()
            raise BoomError

    with pytest.raises(BoomError):
        await failing_unit_of_work()
    async with database.session() as session:
        found = await session.execute(select(Tenant).where(Tenant.slug == "rolled-back"))
        assert found.scalar_one_or_none() is None


async def test_orm_loads_trigger_maintained_updated_at(database: Database) -> None:
    async with database.session() as session:
        tenant = Tenant(slug="orm-touch", name="Before")
        session.add(tenant)
        await session.flush()
        # eager_defaults: id/timestamps come back via RETURNING, no lazy load (which would
        # raise MissingGreenlet in an async session).
        assert tenant.id is not None
        assert tenant.created_at is not None
        tenant.name = "After"
        await session.flush()
        db_value = (
            await session.execute(
                text("SELECT updated_at FROM tenants WHERE id = :id"), {"id": tenant.id}
            )
        ).scalar_one()
        assert tenant.updated_at == db_value
        await session.delete(tenant)


async def test_ready_endpoint_against_real_postgres(migrated_database_url: str) -> None:
    app = create_app(make_settings(database_url=migrated_database_url))
    async with serve(app) as client:
        resp = await client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["checks"]["postgres"]["status"] == "up"


async def test_ready_503_when_postgres_unreachable() -> None:
    app = create_app(
        make_settings(
            database_url="postgresql+asyncpg://nobody:nothing@127.0.0.1:1/none",
            readiness_timeout_s=1.0,
            db_connect_timeout_s=0.5,
        )
    )
    async with serve(app) as client:
        started = time.perf_counter()
        resp = await client.get("/ready")
        elapsed = time.perf_counter() - started
    assert resp.status_code == 503
    assert resp.json()["checks"]["postgres"]["status"] == "down"
    assert elapsed < 3


async def test_lifespan_builds_and_disposes_engine(migrated_database_url: str) -> None:
    app = create_app(make_settings(database_url=migrated_database_url))
    async with serve(app) as client:
        db: Database = app.state.database
        pool = db.engine.pool
        assert isinstance(pool, AsyncAdaptedQueuePool)
        assert (await client.get("/ready")).status_code == 200
        assert pool.checkedin() >= 1
    assert pool.checkedin() == 0
