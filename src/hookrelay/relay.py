"""Fan-out relay: ``python -m hookrelay.relay`` (or ``hookrelay-relay``).

A separate process that drains the transactional outbox:

1. claim a batch of due outbox rows with ``FOR UPDATE SKIP LOCKED`` (any number of relays can
   run side by side; they take disjoint batches);
2. insert one ``deliveries`` row per matching (event, endpoint) with ``ON CONFLICT DO
   NOTHING`` and mark the rows processed, all in one transaction;
3. if the set-based batch fails, retry row by row inside savepoints so one poison event is
   parked with exponential backoff instead of blocking the batch.

Polling backs off exponentially from ``RELAY_POLL_MIN_MS`` to ``RELAY_POLL_MAX_MS`` while the
outbox is empty and drops back to the minimum (or does not sleep at all when a batch was
full) as soon as there is work. ``SIGTERM``/``SIGINT`` finish the current batch and exit.

On its own interval the relay also dispatches operator notifications (Milestone 4: e.g.
``endpoint.disabled``) through a pluggable :class:`~hookrelay.services.notifications.Notifier`
and runs batched housekeeping.
A heartbeat file is touched every loop; ``--healthcheck`` checks its age (container probe).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import structlog
from opentelemetry import trace
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings, get_settings
from hookrelay.core.logging import configure_logging, redact_text
from hookrelay.db.engine import Database
from hookrelay.db.integrity import is_connection_lost
from hookrelay.observability.gauges import refresh_gauges
from hookrelay.observability.metrics import SERVICE_RELAY, Metrics
from hookrelay.observability.runtime import (
    build_observability,
    metrics_http_server,
    shutdown_observability,
)
from hookrelay.observability.tracing import start_span
from hookrelay.redis_ import RedisBundle, WakeupPublisher
from hookrelay.redis_.runtime import build_redis_bundle
from hookrelay.services import fanout, housekeeping, notifications
from hookrelay.services.retention import RetentionPolicy, RetentionRunner

logger = structlog.stdlib.get_logger("hookrelay.relay")

HOUSEKEEPING_BATCH_SIZE = 1_000
HOUSEKEEPING_MAX_ROUNDS = 10
NOTIFICATION_BATCH_SIZE = 100


@dataclass(slots=True)
class RelayStats:
    """Monotonic counters (the shape a Prometheus exporter will publish in a later milestone)."""

    batches: int = 0
    outbox_rows_processed: int = 0
    deliveries_created: int = 0
    outbox_rows_failed: int = 0
    batch_fallbacks: int = 0
    loop_errors: int = 0
    housekeeping_runs: int = 0
    purged_idempotency_keys: int = 0
    purged_outbox_rows: int = 0
    purged_events: int = 0
    purged_notifications: int = 0
    purged_replay_requests: int = 0
    purged_payloads: int = 0
    cleared_previous_secrets: int = 0
    notifications_sent: int = 0
    notifications_failed: int = 0
    notifications_abandoned: int = 0


@dataclass(frozen=True, slots=True)
class BatchResult:
    claimed: int
    processed: int
    failed: int
    deliveries_created: int


class Relay:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        *,
        notifier: notifications.Notifier | None = None,
        redis: RedisBundle | None = None,
        publisher: WakeupPublisher | None = None,
        metrics: Metrics | None = None,
    ) -> None:
        self.database = database
        self.settings = settings
        self.notifier: notifications.Notifier = notifier or notifications.LoggingNotifier()
        self.redis = redis
        self.publisher = publisher or (None if redis is None else redis.publisher)
        self.cache = None if redis is None else redis.cache
        self.metrics = metrics
        self.stats = RelayStats()
        self._stop = asyncio.Event()
        self._heartbeat = Path(settings.relay_heartbeat_file)
        self._heartbeat_warned = False

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def stop(self) -> None:
        if not self._stop.is_set():
            logger.info("relay.stopping")
        self._stop.set()

    async def run_once(self) -> BatchResult:
        """Claim and fan out one batch in one transaction."""
        with start_span(
            "hookrelay.fanout",
            kind=trace.SpanKind.INTERNAL,
            attributes={"hookrelay.batch_size": self.settings.relay_batch_size},
        ) as span:
            async with self.database.session() as session:
                rows = await fanout.claim_batch(session, batch_size=self.settings.relay_batch_size)
                if not rows:
                    return BatchResult(claimed=0, processed=0, failed=0, deliveries_created=0)
                span.set_attribute("hookrelay.claimed", len(rows))
                try:
                    async with session.begin_nested():
                        created = await fanout.fan_out(
                            session, [r.event_id for r in rows], cache=self.cache
                        )
                        await fanout.mark_processed(session, rows)
                    result = BatchResult(
                        claimed=len(rows),
                        processed=len(rows),
                        failed=0,
                        deliveries_created=created,
                    )
                except SQLAlchemyError as exc:
                    if is_connection_lost(exc):
                        # The connection died (Postgres restart / failover): the transaction
                        # and its claims are gone, so isolating a "poison" row is pointless and
                        # would only fail again on the dead session. The loop retries the
                        # batch on a fresh connection.
                        raise
                    logger.warning(
                        "relay.batch_failed",
                        rows=len(rows),
                        error=redact_text(type(exc).__name__),
                    )
                    self.stats.batch_fallbacks += 1
                    result = await self._one_by_one(session, rows)
            self._record(result)
            span.set_attribute("hookrelay.deliveries_created", result.deliveries_created)
            if result.deliveries_created > 0:
                self._notify_workers()
            return result

    def _notify_workers(self) -> None:
        if self.publisher is not None:
            self.publisher.notify()

    async def _one_by_one(
        self, session: AsyncSession, rows: list[fanout.ClaimedRow]
    ) -> BatchResult:
        processed = failed = created = 0
        for row in rows:
            try:
                async with session.begin_nested():
                    created += await fanout.fan_out(session, [row.event_id], cache=self.cache)
                    await fanout.mark_processed(session, [row])
                processed += 1
            except SQLAlchemyError as exc:
                failed += 1
                error = redact_text(f"{type(exc).__name__}: {exc}")
                delay = await fanout.mark_failed(
                    session, row, error, retry_max_s=self.settings.relay_retry_max_s
                )
                logger.warning(
                    "relay.outbox_row_failed",
                    outbox_id=row.outbox_id,
                    event_id=str(row.event_id),
                    tenant_id=str(row.tenant_id),
                    attempts=row.attempts + 1,
                    retry_in_s=delay,
                    error=error[:200],
                )
        return BatchResult(
            claimed=len(rows), processed=processed, failed=failed, deliveries_created=created
        )

    def _record(self, result: BatchResult) -> None:
        self.stats.batches += 1
        self.stats.outbox_rows_processed += result.processed
        self.stats.outbox_rows_failed += result.failed
        self.stats.deliveries_created += result.deliveries_created
        if self.metrics is not None:
            self.metrics.record_relay_batch()
            self.metrics.record_outbox_processed(
                processed=result.processed,
                deliveries=result.deliveries_created,
                failed=result.failed,
            )
        logger.info(
            "relay.batch",
            claimed=result.claimed,
            processed=result.processed,
            failed=result.failed,
            deliveries_created=result.deliveries_created,
        )

    async def refresh_gauges_once(self) -> None:
        """Push queue-depth / breaker gauges to Prometheus (best-effort)."""
        if self.metrics is None:
            return
        try:
            async with self.database.session() as session:
                await refresh_gauges(session, self.metrics)
        except Exception as exc:
            logger.warning("relay.gauges_failed", error=type(exc).__name__)

    async def housekeep(self) -> housekeeping.SweepResult:
        """Purge expired idempotency keys, old processed outbox rows, expired events, old
        dispatched notifications and old replay audit rows; then one bounded retention pass
        (payloads past EVENT_PAYLOAD_RETENTION_DAYS, ended secret overlaps)."""
        totals = dict.fromkeys(
            ("idempotency_keys", "outbox_rows", "events", "notifications", "replay_requests"), 0
        )
        for _ in range(HOUSEKEEPING_MAX_ROUNDS):
            async with self.database.session() as session:
                swept = await housekeeping.sweep(
                    session,
                    outbox_retention_s=self.settings.relay_outbox_retention_s,
                    batch_size=HOUSEKEEPING_BATCH_SIZE,
                    notification_retention_s=self.settings.notification_retention_s,
                    replay_retention_s=self.settings.event_retention_days * 86_400,
                )
            for name in totals:
                totals[name] += getattr(swept, name)
            if self.stopping or max(asdict(swept).values()) < HOUSEKEEPING_BATCH_SIZE:
                break
        total = housekeeping.SweepResult(**totals)
        self.stats.housekeeping_runs += 1
        self.stats.purged_idempotency_keys += total.idempotency_keys
        self.stats.purged_outbox_rows += total.outbox_rows
        self.stats.purged_events += total.events
        self.stats.purged_notifications += total.notifications
        self.stats.purged_replay_requests += total.replay_requests
        retention = await RetentionRunner(
            self.database,
            RetentionPolicy(
                batch_size=self.settings.retention_batch_size,
                max_batches=HOUSEKEEPING_MAX_ROUNDS,
                payload_retention_s=self.settings.event_payload_retention_s,
                expire_events=False,  # the sweep above already did
            ),
            should_stop=lambda: self.stopping,
        ).run()
        self.stats.purged_payloads += retention.payloads
        self.stats.cleared_previous_secrets += retention.previous_secrets
        if total.total or retention.total:
            logger.info(
                "relay.housekeeping",
                **asdict(total),
                payloads=retention.payloads,
                responses_scrubbed=retention.responses_scrubbed,
                previous_secrets=retention.previous_secrets,
            )
        return total

    async def notify_once(self) -> notifications.DispatchResult:
        """Dispatch one batch of due operator notifications."""
        async with self.database.session() as session:
            result = await notifications.dispatch_due(
                session,
                self.notifier,
                batch_size=NOTIFICATION_BATCH_SIZE,
                max_attempts=self.settings.notification_max_attempts,
            )
        self.stats.notifications_sent += result.sent
        self.stats.notifications_failed += result.failed
        self.stats.notifications_abandoned += result.abandoned
        if result.total:
            logger.info("relay.notifications", **asdict(result))
        return result

    def touch_heartbeat(self) -> None:
        try:
            self._heartbeat.touch()
        except OSError as exc:
            if not self._heartbeat_warned:
                logger.warning(
                    "relay.heartbeat_unwritable", path=str(self._heartbeat), error=str(exc)
                )
                self._heartbeat_warned = True

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def run(self) -> None:  # noqa: PLR0912, PLR0915 - main loop with notify/housekeep
        """Poll until :meth:`stop` (or SIGTERM/SIGINT); the in-flight batch always completes."""
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop)
                installed.append(sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - non-Unix / thread
                pass
        min_s = self.settings.relay_poll_min_ms / 1000
        max_s = self.settings.relay_poll_max_ms / 1000
        delay = min_s
        next_housekeeping = next_notify = time.monotonic()
        logger.info(
            "relay.started",
            batch_size=self.settings.relay_batch_size,
            poll_min_ms=self.settings.relay_poll_min_ms,
            poll_max_ms=self.settings.relay_poll_max_ms,
            wakeup=self.publisher is not None,
            subscription_cache=self.cache is not None,
        )
        try:
            while not self.stopping:
                claimed = 0
                try:
                    claimed = (await self.run_once()).claimed
                except Exception as exc:
                    self.stats.loop_errors += 1
                    if self.metrics is not None:
                        self.metrics.record_relay_loop_error()
                    logger.exception("relay.iteration_failed", error=type(exc).__name__)
                self.touch_heartbeat()
                if not self.stopping and time.monotonic() >= next_notify:
                    try:
                        await self.notify_once()
                    except Exception as exc:
                        self.stats.loop_errors += 1
                        if self.metrics is not None:
                            self.metrics.record_relay_loop_error()
                        logger.exception("relay.notify_failed", error=type(exc).__name__)
                    next_notify = time.monotonic() + self.settings.notification_poll_interval_s
                if not self.stopping and time.monotonic() >= next_housekeeping:
                    try:
                        await self.housekeep()
                        await self.refresh_gauges_once()
                    except Exception as exc:
                        self.stats.loop_errors += 1
                        if self.metrics is not None:
                            self.metrics.record_relay_loop_error()
                        logger.exception("relay.housekeeping_failed", error=type(exc).__name__)
                    next_housekeeping = (
                        time.monotonic() + self.settings.relay_housekeeping_interval_s
                    )
                if claimed >= self.settings.relay_batch_size:
                    delay = min_s
                    continue  # a full batch: there is probably more, do not sleep
                delay = min_s if claimed else min(delay * 2, max_s)
                await self._sleep(delay)
        finally:
            for sig in installed:
                loop.remove_signal_handler(sig)
            logger.info("relay.stopped", **asdict(self.stats))


def heartbeat_is_fresh(path: str, max_age_s: float, *, now: float | None = None) -> bool:
    try:
        modified = Path(path).stat().st_mtime
    except OSError:
        return False
    return ((time.time() if now is None else now) - modified) <= max_age_s


async def serve(settings: Settings, *, once: bool = False) -> RelayStats:
    database = Database.from_settings(settings)
    redis = build_redis_bundle(settings, role="relay", source="relay")
    obs = build_observability(settings, service=SERVICE_RELAY)
    metrics_http_server(settings, obs.metrics)
    relay = Relay(database, settings, redis=redis, metrics=obs.metrics)
    try:
        if once:
            await relay.run_once()
            await relay.notify_once()
            await relay.refresh_gauges_once()
        else:
            await relay.run()
    finally:
        await redis.close()
        await database.dispose()
        shutdown_observability()
    return relay.stats


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hookrelay-relay", description="hookrelay fan-out relay")
    parser.add_argument("--once", action="store_true", help="process one batch and exit")
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="exit 0 if the heartbeat file is fresh (container health probe)",
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    if args.healthcheck:
        fresh = heartbeat_is_fresh(
            settings.relay_heartbeat_file, settings.relay_heartbeat_max_age_s
        )
        return 0 if fresh else 1
    configure_logging(settings)
    asyncio.run(serve(settings, once=args.once))
    return 0


if __name__ == "__main__":
    sys.exit(main())
