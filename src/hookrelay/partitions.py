"""``python -m hookrelay.partitions`` / ``hookrelay-partitions``: one partition maintenance run.

For ``events``, ``deliveries`` and ``delivery_attempts`` (the three tables of the event graph,
all partitioned by the event's UTC day): premakes daily partitions ``PARTITION_PREMAKE_DAYS``
ahead (parents first), moves stray ``delivery_attempts`` rows out of the ``DEFAULT`` partition,
detaches + drops partitions past ``PARTITION_RETENTION_DAYS`` (children first, so one day leaves
as a unit; see :mod:`hookrelay.services.partitions`). Meant for an hourly CronJob: every step
is idempotent, and concurrent runs are safe (an advisory lock per table makes the second one a
no-op). The relay runs the premake part as a safety net.

Connects to Postgres directly (``Settings.migration_dsn``: ``DATABASE_DIRECT_URL`` when
``DB_PGBOUNCER=true``), because the advisory locks are session-level and
``DETACH ... CONCURRENTLY`` cannot run inside a transaction.

With ``--repair-default`` (or ``PARTITION_REPAIR_DEFAULT=true``) the run first re-homes rows
stranded in the ``events`` / ``deliveries`` DEFAULT partitions: one day's event graph per
transaction, cascade-safe (:mod:`hookrelay.services.partition_repair`), bounded by
``PARTITION_REPAIR_MAX_DAYS`` / ``PARTITION_REPAIR_MAX_ROWS``; with ``--dry-run`` it only
counts what it would move. ``--metrics-textfile PATH`` writes the run's counters (e.g.
``hookrelay_partition_repaired_rows_total``) in the Prometheus text format for a node-exporter
textfile collector, since a CronJob is never scraped.

Exit codes: 0 done (or another run holds the lock; a repaired day counts as done), 3 incomplete
(a lock timeout outlasted the retries, SIGTERM, a day blocked by rows in a DEFAULT partition
or a repair skipped (too large, unsupported, deferred, failed), a drop blocked by referencing
rows, or the default-partition sweep hit its batch budget: look at the log, run it again), 1 a
table is not partitioned yet (run migrations first).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys

import asyncpg
import structlog
from prometheus_client import write_to_textfile

from hookrelay.core.config import Settings, get_settings
from hookrelay.core.logging import configure_logging
from hookrelay.observability.metrics import SERVICE_PARTITIONS, build_metrics
from hookrelay.services.partitions import (
    NotPartitionedError,
    PartitionPolicy,
    PartitionReport,
    connect_kwargs,
    maintain,
)

logger = structlog.stdlib.get_logger("hookrelay.partitions")

EXIT_INCOMPLETE = 3
EXIT_NOT_PARTITIONED = 1


def asyncpg_dsn(sqlalchemy_url: str) -> str:
    """asyncpg takes ``postgresql://``; settings hold the SQLAlchemy ``+asyncpg`` form."""
    return sqlalchemy_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def run(
    settings: Settings, *, dry_run: bool = False, repair_default: bool | None = None
) -> list[PartitionReport]:
    policy = PartitionPolicy.from_settings(settings, dry_run=dry_run, repair_default=repair_default)
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
            repair_default=policy.repair_default,
        )
        reports = await maintain(conn, policy, should_stop=stop.is_set)
        for report in reports:
            logger.info("partitions.finished", **report.as_log())
        return reports
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
    parser.add_argument(
        "--repair-default",
        action="store_true",
        default=None,
        help="re-home rows stranded in the events / deliveries DEFAULT partitions "
        "(cascade-safe, one day per transaction; default: PARTITION_REPAIR_DEFAULT)",
    )
    parser.add_argument(
        "--metrics-textfile",
        metavar="PATH",
        help="write this run's counters in the Prometheus text format (textfile collector)",
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings)
    try:
        reports = asyncio.run(
            run(settings, dry_run=args.dry_run, repair_default=args.repair_default)
        )
    except NotPartitionedError as exc:
        logger.error("partitions.not_partitioned", error=str(exc))  # noqa: TRY400 - expected
        return EXIT_NOT_PARTITIONED
    if args.metrics_textfile:
        write_metrics_textfile(args.metrics_textfile, reports)
    return 0 if all(r.complete for r in reports) else EXIT_INCOMPLETE


def write_metrics_textfile(path: str, reports: list[PartitionReport]) -> None:
    """The run's counters for a node-exporter textfile collector (atomic rename)."""
    metrics = build_metrics(SERVICE_PARTITIONS)
    for report in reports:
        metrics.record_partition_repair(report.table, report.repaired_rows)
    write_to_textfile(path, metrics.registry)


if __name__ == "__main__":
    sys.exit(main())
