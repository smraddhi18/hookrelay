"""Integration fixtures against a real Postgres.

Set ``TEST_DATABASE_URL`` (any postgres:// / postgresql+asyncpg:// URL whose role may
``CREATE DATABASE``). Every test session creates a throwaway database, migrates it with the
real Alembic scripts, and drops it at the end, so tests never touch a developer's data and
migration tests can freely downgrade/upgrade their own scratch databases.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url

from hookrelay.core.config import normalize_database_url
from hookrelay.db.engine import Database
from tests.conftest import make_settings

ROOT = Path(__file__).resolve().parents[2]
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        if "tests/integration/" in item.nodeid.replace("\\", "/"):
            item.add_marker(pytest.mark.integration)
            if not TEST_DATABASE_URL:
                item.add_marker(pytest.mark.skip(reason="TEST_DATABASE_URL is not set"))


def _url_for(database: str) -> str:
    assert TEST_DATABASE_URL
    url = make_url(normalize_database_url(TEST_DATABASE_URL)).set(database=database)
    return url.render_as_string(hide_password=False)


def _asyncpg_dsn(sqlalchemy_url: str) -> str:
    return sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def _admin(sql: str) -> None:
    conn = await asyncpg.connect(_asyncpg_dsn(_url_for("postgres")))
    try:
        await conn.execute(sql)
    finally:
        await conn.close()


def create_scratch_database(prefix: str) -> tuple[str, str]:
    name = f"{prefix}_{uuid.uuid4().hex[:10]}"
    asyncio.run(_admin(f'CREATE DATABASE "{name}"'))
    return name, _url_for(name)


def drop_scratch_database(name: str) -> None:
    asyncio.run(_admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))


def alembic_config(url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "alembic"))
    # ConfigParser treats '%' as interpolation; URL-encoded passwords contain it.
    cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    cfg.attributes["configure_logger"] = False
    return cfg


@pytest.fixture(scope="session")
def migrated_database_url() -> Iterator[str]:
    name, url = create_scratch_database("hookrelay_it")
    try:
        command.upgrade(alembic_config(url), "head")
        yield url
    finally:
        drop_scratch_database(name)


@pytest.fixture
def scratch_database_url() -> Iterator[str]:
    """An empty database (no migrations applied) for migration tests."""
    name, url = create_scratch_database("hookrelay_mig")
    try:
        yield url
    finally:
        drop_scratch_database(name)


@pytest.fixture
async def database(migrated_database_url: str) -> AsyncIterator[Database]:
    db = Database.from_settings(make_settings(database_url=migrated_database_url))
    try:
        yield db
    finally:
        await db.dispose()


@pytest.fixture
async def pg(migrated_database_url: str) -> AsyncIterator[asyncpg.Connection]:
    conn = await asyncpg.connect(_asyncpg_dsn(migrated_database_url))
    tx = conn.transaction()
    await tx.start()
    try:
        yield conn
    finally:
        await tx.rollback()
        await conn.close()
