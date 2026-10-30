"""Async Alembic environment.

The URL comes from ``hookrelay.core.config.Settings`` unless a caller (tests) sets
``sqlalchemy.url`` on the Alembic config explicitly. Each migration runs in its own transaction
on Postgres (transactional DDL), guarded by short lock and statement timeouts so a migration
waiting on a busy table fails fast instead of queueing every query behind its lock.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from hookrelay.core.config import Settings, normalize_database_url
from hookrelay.db import models  # noqa: F401 - registers every model on Base.metadata
from hookrelay.db.base import Base

config = context.config

if config.config_file_name is not None and config.attributes.get("configure_logger", True):
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata

_LOCK_TIMEOUT = "5s"
_STATEMENT_TIMEOUT = "60s"


def _database_url() -> str:
    explicit = config.get_main_option("sqlalchemy.url")
    if explicit:
        return normalize_database_url(explicit)
    return Settings().database_dsn


def run_migrations_offline() -> None:
    """Emit SQL to stdout (``alembic upgrade head --sql``) without a live database."""
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        transaction_per_migration=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    connection.exec_driver_sql(f"SET lock_timeout = '{_LOCK_TIMEOUT}'")
    connection.exec_driver_sql(f"SET statement_timeout = '{_STATEMENT_TIMEOUT}'")
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        compare_server_default=True,
        transaction_per_migration=True,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = async_engine_from_config(
        {"sqlalchemy.url": _database_url()},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_do_run_migrations)
            await connection.commit()
    finally:
        await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
