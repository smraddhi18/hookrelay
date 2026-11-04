"""``python -m hookrelay.retention`` / ``hookrelay-retention``: one bounded retention run.

Meant for a cron job or Kubernetes CronJob next to (or instead of) the relay's housekeeping
pass. Exits 0 when everything due was processed, 3 when the batch budget
(``RETENTION_MAX_BATCHES``) or SIGTERM cut the run short (run it again, or raise the budget).
Concurrent runs are safe: every batch takes its rows with ``SKIP LOCKED``.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
from dataclasses import asdict

import structlog

from hookrelay.core.config import Settings, get_settings
from hookrelay.core.logging import configure_logging
from hookrelay.db.engine import Database
from hookrelay.services.retention import RetentionPolicy, RetentionReport, RetentionRunner

logger = structlog.stdlib.get_logger("hookrelay.retention")

EXIT_INCOMPLETE = 3


async def run(settings: Settings, *, max_batches: int | None = None) -> RetentionReport:
    policy = RetentionPolicy.from_settings(settings)
    if max_batches is not None:
        policy = RetentionPolicy(
            batch_size=policy.batch_size,
            max_batches=max_batches,
            pause_s=policy.pause_s,
            payload_retention_s=policy.payload_retention_s,
        )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError, RuntimeError):
            loop.add_signal_handler(sig, stop.set)
            installed.append(sig)
    database = Database.from_settings(settings)
    try:
        logger.info(
            "retention.started",
            batch_size=policy.batch_size,
            max_batches=policy.max_batches,
            payload_retention_s=policy.payload_retention_s,
        )
        report = await RetentionRunner(database, policy, should_stop=stop.is_set).run()
        logger.info("retention.finished", **asdict(report))
        return report
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)
        await database.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hookrelay-retention", description="hookrelay data retention (one bounded run)"
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="override RETENTION_MAX_BATCHES (batches per task) for this run",
    )
    args = parser.parse_args(argv)
    if args.max_batches is not None and args.max_batches < 1:
        parser.error("--max-batches must be >= 1")
    settings = get_settings()
    configure_logging(settings)
    report = asyncio.run(run(settings, max_batches=args.max_batches))
    return 0 if report.complete else EXIT_INCOMPLETE


if __name__ == "__main__":
    sys.exit(main())
