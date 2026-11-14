"""``python -m hookrelay.partitions`` / ``hookrelay-partitions``: one partition maintenance run.

Premakes daily ``delivery_attempts`` partitions ``PARTITION_PREMAKE_DAYS`` ahead, moves rows
out of the ``DEFAULT`` partition, detaches + drops partitions past ``PARTITION_RETENTION_DAYS``
(see :mod:`hookrelay.services.partitions`). Meant for an hourly CronJob: every step is
idempotent, and concurrent runs are safe (an advisory lock makes the second one a no-op).

Connects to Postgres directly (``Settings.migration_dsn``: ``DATABASE_DIRECT_URL`` when
``DB_PGBOUNCER=true``), because the advisory lock is session-level and
``DETACH ... CONCURRENTLY`` cannot run inside a transaction.

Exit codes: 0 done (or another run holds the lock), 3 incomplete (a lock timeout outlasted
the retries, SIGTERM, or the default-partition sweep hit its batch budget: run it again),
1 the table is not partitioned yet (run migrations first).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys

import asyncpg
import structlog

from hookrelay.core.config import Settings, get_settings
from hookrelay.core.logging import configure_logging
from hookrelay.services.partitions import (
    NotPartitionedError,
    PartitionMaintainer,
    PartitionPolicy,
    PartitionReport,
    connect_kwargs,
)

logger = structlog.stdlib.get_logger("hookrelay.partitions")

EXIT_INCOMPLETE = 3
EXIT_NOT_PARTITIONED = 1


def asyncpg_dsn(sqlalchemy_url: str) -> str:
    """asyncpg takes ``postgresql://``; settings hold the SQLAlchemy ``+asyncpg`` form."""
    return sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def run(settings: Settings, *, dry_run: bool = False) -> PartitionReport:
    policy = PartitionPolicy.from_settings(settings, dry_run=dry_run)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
            installed.append(sig)
    conn = await asyncpg.connect(
        asyncpg_dsn(settings.migration_dsn),
        timeout=settings.db_connect_timeout_s,
        **connect_kwargs(policy, application_name=f"{settings.app_name}-partitions"),
    )
    try:
        logger.info(
            "partitions.started",
            premake_days=policy.premake_days,
            retention_days=policy.retention_days,
            lock_timeout_ms=policy.lock_timeout_ms,
            dry_run=dry_run,
        )
        report = await PartitionMaintainer(conn, policy, should_stop=stop.is_set).run()
        logger.info("partitions.finished", **report.as_log())
        return report
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)
        await conn.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hookrelay-partitions",
        description="hookrelay partition maintenance (premake, retire, default sweep)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="log what would be created / detached / dropped without changing anything",
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings)
    try:
        report = asyncio.run(run(settings, dry_run=args.dry_run))
    except NotPartitionedError as exc:
        logger.error("partitions.not_partitioned", error=str(exc))  # noqa: TRY400 - expected
        return EXIT_NOT_PARTITIONED
    return 0 if report.complete else EXIT_INCOMPLETE


if __name__ == "__main__":
    sys.exit(main())
