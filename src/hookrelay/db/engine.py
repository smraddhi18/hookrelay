"""Async engine + session factory, owned by the app lifespan (never created at import time)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from hookrelay.core.config import Settings

logger = structlog.stdlib.get_logger("hookrelay.db")


def build_engine(settings: Settings) -> AsyncEngine:
    """Create the pooled asyncpg engine.

    * ``pool_pre_ping`` turns a dead pooled connection (Postgres restart, failover) into a
      transparent reconnect instead of a 500.
    * ``statement_timeout`` is set server-side per connection so a runaway query cannot hold a
      pool slot forever; individual queries can still lower it with ``SET LOCAL``.
    * ``application_name`` makes our sessions identifiable in ``pg_stat_activity``.
    """
    return create_async_engine(
        settings.database_dsn,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_s,
        pool_recycle=settings.db_pool_recycle_s,
        pool_pre_ping=True,
        connect_args={
            "timeout": settings.db_connect_timeout_s,
            "command_timeout": settings.db_statement_timeout_ms / 1000 + 5,
            "server_settings": {
                "application_name": settings.app_name,
                "statement_timeout": str(settings.db_statement_timeout_ms),
                "idle_in_transaction_session_timeout": "60000",
            },
        },
    )


class Database:
    """Holds the engine and session factory for one process."""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        self.sessionmaker: async_sessionmaker[AsyncSession] = async_sessionmaker(
            engine, expire_on_commit=False, autoflush=False
        )

    @classmethod
    def from_settings(cls, settings: Settings) -> Database:
        return cls(build_engine(settings))

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Unit of work: commit on success, roll back on any exception."""
        async with self.sessionmaker() as session:
            try:
                yield session
                await session.commit()
            except BaseException:
                await session.rollback()
                raise

    async def ping(self, timeout_s: float) -> None:
        """Round-trip ``SELECT 1``; raises ``TimeoutError`` or a driver/SQLAlchemy error."""
        async with asyncio.timeout(timeout_s), self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def dispose(self) -> None:
        await self.engine.dispose()
        logger.info("db.engine.disposed")
