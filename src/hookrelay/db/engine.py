"""Async engine + session factory, owned by the app lifespan (never created at import time)."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

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


def prepared_statement_name() -> str:
    """A process- and connection-independent statement name (PgBouncer transaction pooling).

    asyncpg names statements ``__asyncpg_stmt_<n>__`` per client connection; behind a
    transaction pooler two clients can land on the same server connection and collide
    (``prepared statement "__asyncpg_stmt_1__" already exists``). SQLAlchemy documents a UUID
    name function as the fix.
    """
    return f"__asyncpg_{uuid.uuid4().hex}__"


def connect_args(settings: Settings) -> dict[str, Any]:
    """asyncpg ``connect()`` arguments for a direct Postgres or a PgBouncer ``DATABASE_URL``.

    Direct: ``statement_timeout`` / ``idle_in_transaction_session_timeout`` are startup
    parameters, so every server session carries them. PgBouncer (``DB_PGBOUNCER=true``): it
    rejects startup parameters it does not track (only ``application_name`` and a few
    others are), and a session ``SET`` would leak to whichever client gets the server
    connection next, so the server-side timeouts are configured on the pooler
    (``connect_query``) or as role defaults instead; asyncpg's ``command_timeout`` still bounds
    every query client-side (its cancel request is forwarded by PgBouncer). Prepared statements
    stay on (the extended protocol needs them) but are never cached and get unique names.
    """
    args: dict[str, Any] = {
        "timeout": settings.db_connect_timeout_s,
        "command_timeout": settings.db_statement_timeout_ms / 1000 + 5,
    }
    if settings.db_pgbouncer:
        args["server_settings"] = {"application_name": settings.app_name}
        args["statement_cache_size"] = 0
        args["prepared_statement_cache_size"] = 0
        args["prepared_statement_name_func"] = prepared_statement_name
    else:
        args["server_settings"] = {
            "application_name": settings.app_name,
            "statement_timeout": str(settings.db_statement_timeout_ms),
            "idle_in_transaction_session_timeout": "60000",
        }
    return args


def build_engine(settings: Settings) -> AsyncEngine:
    """Create the pooled asyncpg engine.

    * ``pool_pre_ping`` turns a dead pooled connection (Postgres restart, failover) into a
      transparent reconnect instead of a 500.
    * ``statement_timeout`` is set server-side per connection so a runaway query cannot hold a
      pool slot forever; individual queries can still lower it with ``SET LOCAL``.
    * ``application_name`` makes our sessions identifiable in ``pg_stat_activity``.
    * ``DB_PGBOUNCER=true`` adapts the driver to transaction pooling (see :func:`connect_args`).
      The SQLAlchemy pool stays: it holds cheap client connections to PgBouncer, which maps
      them onto far fewer server connections only for the duration of a transaction.
    """
    return create_async_engine(
        settings.database_dsn,
        echo=settings.db_echo,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout_s,
        pool_recycle=settings.db_pool_recycle_s,
        pool_pre_ping=True,
        connect_args=connect_args(settings),
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
