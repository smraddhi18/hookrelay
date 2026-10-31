"""Shared fixtures: settings factory, a fake database, and a lifespan-aware HTTP client."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, cast

import httpx
import pytest
from fastapi import FastAPI

from hookrelay.core.config import Settings
from hookrelay.main import create_app

if TYPE_CHECKING:
    from hookrelay.db.engine import Database

# Fixed 32-byte AES key for tests (base64url). Never use outside tests.
TEST_ENCRYPTION_KEY = "dGVzdC1rZXktdGVzdC1rZXktdGVzdC1rZXktMzJieXQ"
TEST_OPERATOR_TOKEN = "op-test-token-0123456789abcdefghijklmnop"

# Never let a developer's .env or shell leak into unit tests.
_ISOLATED_ENV = {"_env_file": None}


def make_settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "app_env": "test",
        "log_format": "json",
        "log_level": "WARNING",
        "database_url": os.environ.get(
            "TEST_DATABASE_URL", "postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay"
        ),
    }
    base.update(overrides)
    return Settings(**_ISOLATED_ENV, **base)  # type: ignore[arg-type]


@pytest.fixture
def settings_factory() -> Callable[..., Settings]:
    return make_settings


class FakeDatabase:
    """Stands in for ``hookrelay.db.engine.Database`` where only ping/dispose matter."""

    def __init__(self, ping_error: BaseException | None = None, ping_delay_s: float = 0) -> None:
        self.ping_error = ping_error
        self.ping_delay_s = ping_delay_s
        self.ping_calls = 0
        self.disposed = False

    async def ping(self, timeout_s: float) -> None:
        self.ping_calls += 1
        async with asyncio.timeout(timeout_s):
            if self.ping_delay_s:
                await asyncio.sleep(self.ping_delay_s)
        if self.ping_error is not None:
            raise self.ping_error

    async def dispose(self) -> None:
        self.disposed = True

    def as_database(self) -> Database:
        """Duck-typed stand-in: the app only calls ping()/dispose() on it in these tests."""
        return cast("Database", self)


@pytest.fixture
def fake_db() -> FakeDatabase:
    return FakeDatabase()


@asynccontextmanager
async def serve(
    app: FastAPI, *, raise_app_exceptions: bool = True
) -> AsyncIterator[httpx.AsyncClient]:
    """Run the app's lifespan and yield a client bound to it (ASGITransport skips lifespan)."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            yield client


@pytest.fixture
def app_factory(fake_db: FakeDatabase) -> Callable[..., FastAPI]:
    def _factory(db: Any = None, **overrides: Any) -> FastAPI:
        return create_app(make_settings(**overrides), database=(db or fake_db).as_database())

    return _factory
