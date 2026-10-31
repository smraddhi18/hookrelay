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
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg
import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import make_url

from hookrelay.core.config import Settings, normalize_database_url
from hookrelay.db.engine import Database
from hookrelay.main import create_app
from tests.conftest import TEST_ENCRYPTION_KEY, TEST_OPERATOR_TOKEN, make_settings, serve

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


# --- HTTP-level fixtures for the tenant / operator APIs -----------------------------------


@dataclass(frozen=True)
class TenantCtx:
    id: str
    slug: str
    admin_key: str

    @property
    def headers(self) -> dict[str, str]:
        return bearer(self.admin_key)


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


OPERATOR = bearer(TEST_OPERATOR_TOKEN)


def api_settings(database_url: str, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "database_url": database_url,
        "operator_token": TEST_OPERATOR_TOKEN,
        "secrets_encryption_key": TEST_ENCRYPTION_KEY,
    }
    base.update(overrides)
    return make_settings(**base)


@asynccontextmanager
async def api_client(database_url: str, **overrides: Any) -> AsyncIterator[httpx.AsyncClient]:
    settings = api_settings(database_url, **overrides)
    app = create_app(settings, database=Database.from_settings(settings))
    async with serve(app) as client:
        yield client


@pytest.fixture
async def client(migrated_database_url: str) -> AsyncIterator[httpx.AsyncClient]:
    async with api_client(migrated_database_url) as c:
        yield c


async def create_tenant(client: httpx.AsyncClient, *, slug: str | None = None) -> TenantCtx:
    slug = slug or f"t-{uuid.uuid4().hex[:12]}"
    created = await client.post(
        "/admin/tenants", json={"slug": slug, "name": slug.title()}, headers=OPERATOR
    )
    assert created.status_code == 201, created.text
    tenant_id = created.json()["id"]
    issued = await client.post(
        f"/admin/tenants/{tenant_id}/api-keys",
        json={"name": "bootstrap", "role": "admin"},
        headers=OPERATOR,
    )
    assert issued.status_code == 201, issued.text
    return TenantCtx(id=tenant_id, slug=slug, admin_key=issued.json()["key"])


async def issue_key(client: httpx.AsyncClient, tenant: TenantCtx, role: str, **extra: Any) -> str:
    response = await client.post(
        "/v1/api-keys", json={"name": f"{role}-key", "role": role, **extra}, headers=tenant.headers
    )
    assert response.status_code == 201, response.text
    return str(response.json()["key"])


@pytest.fixture
async def tenant(client: httpx.AsyncClient) -> TenantCtx:
    return await create_tenant(client)
