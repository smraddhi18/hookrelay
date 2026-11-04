"""Data retention: expired events, event payloads past their retention, ended secret overlaps.

Three tiers, cheapest to keep first:

1. **Event expiry** (``EVENT_RETENTION_DAYS``): the event row and everything hanging off it
   (outbox row, deliveries, attempts) is deleted at ``retain_until``.
2. **Payload purge** (``EVENT_PAYLOAD_RETENTION_DAYS``, optional): the payload (and the receiver
   response bodies stored on its attempts) is dropped earlier, while delivery metadata (status,
   attempts, latency, error classes) stays searchable until tier 1. Only events that were fanned
   out and whose deliveries are **all terminal** qualify; a busy event is skipped and retried by
   a later run. A purged event cannot be replayed (409 ``payload_purged``).
3. **Previous signing secrets**: the replaced secret of a rotation is cleared once its overlap
   window ended (the lease query already ignores it after expiry; this removes the ciphertext).

Every task works in batches of at most ``batch_size`` rows, **one short transaction per
batch**, and takes rows with ``SKIP LOCKED``, so concurrent runs (several relays, a CronJob
next to them) split the work instead of blocking each other or ingestion. A run stops after
``max_batches`` batches per task (the report says whether it finished) and can pause between
batches to keep replication lag and I/O bursts down.

Payload purge vs replay: the purge locks the candidate event (``FOR NO KEY UPDATE SKIP
LOCKED``, which does not conflict with the ``KEY SHARE`` lock fan-out's FK check takes) and
then share-locks **all** of its deliveries. ``FOR SHARE`` waits for a replay (or worker) that
holds a delivery row and re-reads the newest version, so a delivery requeued by a concurrent
replay is seen as busy; a replay that comes second waits for the purge to commit and then sees
``payload_purged_at``. As a last line of defence the worker fails a delivery whose payload is
gone with the terminal error class ``payload_purged`` instead of sending an empty body.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings
from hookrelay.db.engine import Database
from hookrelay.services.housekeeping import purge_expired_events

logger = structlog.stdlib.get_logger("hookrelay.retention")

Cursor = tuple[datetime, uuid.UUID]

_PAYLOAD_CANDIDATES_SQL = text(
    """
    SELECT ev.id, ev.created_at, ev.fanned_out_at IS NOT NULL AS fanned_out
    FROM events AS ev
    WHERE ev.payload_purged_at IS NULL
      AND ev.created_at <= now() - make_interval(secs => :older_than_s)
      AND (CAST(:after_ts AS timestamptz) IS NULL
           OR (ev.created_at, ev.id) > (CAST(:after_ts AS timestamptz), CAST(:after_id AS uuid)))
    ORDER BY ev.created_at, ev.id
    LIMIT :batch_size
    FOR NO KEY UPDATE SKIP LOCKED
    """
)

# Share-locks every delivery of the candidates (waiting for current holders) and reports the
# events that still have one on its way. The CTE must stay MATERIALIZED: written as a plain
# subquery, the planner pushes the outer status filter down into the locking scan, so a
# delivery that a replay is about to make pending again (still 'dead' when scanned) would be
# neither locked nor waited for.
_BUSY_EVENTS_SQL = text(
    """
    WITH locked AS MATERIALIZED (
        SELECT d.event_id, d.status
        FROM deliveries AS d
        WHERE d.event_id = ANY(CAST(:ids AS uuid[]))
        FOR SHARE
    )
    SELECT DISTINCT event_id FROM locked
    WHERE status IN ('pending', 'delivering')
    """
)

_PURGE_PAYLOADS_SQL = text(
    """
    UPDATE events
    SET payload = NULL, payload_purged_at = now()
    WHERE id = ANY(CAST(:ids AS uuid[])) AND payload_purged_at IS NULL
    RETURNING id
    """
)

_SCRUB_RESPONSES_SQL = text(
    """
    UPDATE delivery_attempts AS a
    SET response_body = NULL, response_truncated = false
    FROM deliveries AS d
    WHERE d.event_id = ANY(CAST(:ids AS uuid[]))
      AND a.tenant_id = d.tenant_id AND a.delivery_id = d.id
      AND a.response_body IS NOT NULL
    RETURNING a.id
    """
)

_EXPIRE_SECRETS_SQL = text(
    """
    UPDATE endpoints AS e
    SET previous_secret_ciphertext = NULL, previous_secret_expires_at = NULL
    FROM (
        SELECT id FROM endpoints
        WHERE previous_secret_expires_at <= now()
        ORDER BY previous_secret_expires_at
        LIMIT :batch_size
        FOR NO KEY UPDATE SKIP LOCKED
    ) AS doomed
    WHERE e.id = doomed.id
    RETURNING e.id
    """
)


@dataclass(frozen=True, slots=True)
class PayloadBatch:
    scanned: int
    purged: int
    responses_scrubbed: int
    skipped_busy: int
    last: Cursor | None


async def purge_payloads(
    session: AsyncSession,
    *,
    older_than_s: int,
    batch_size: int,
    after: Cursor | None = None,
) -> PayloadBatch:
    """Purge the payloads of up to ``batch_size`` unpurged events older than ``older_than_s``
    (oldest first, strictly after ``after``). Busy events are skipped, and the returned cursor
    lets the caller move past them instead of rescanning them every batch."""
    rows = (
        await session.execute(
            _PAYLOAD_CANDIDATES_SQL,
            {
                "older_than_s": float(older_than_s),
                "after_ts": None if after is None else after[0],
                "after_id": None if after is None else after[1],
                "batch_size": batch_size,
            },
        )
    ).all()
    if not rows:
        return PayloadBatch(0, 0, 0, 0, None)
    last: Cursor = (rows[-1].created_at, rows[-1].id)
    fanned = [r.id for r in rows if r.fanned_out]
    busy: set[uuid.UUID] = set()
    if fanned:
        busy = set((await session.scalars(_BUSY_EVENTS_SQL, {"ids": fanned})).all())
    ready = [event_id for event_id in fanned if event_id not in busy]
    purged: list[uuid.UUID] = []
    scrubbed = 0
    if ready:
        purged = list((await session.scalars(_PURGE_PAYLOADS_SQL, {"ids": ready})).all())
        if purged:
            scrubbed = len((await session.execute(_SCRUB_RESPONSES_SQL, {"ids": purged})).all())
    return PayloadBatch(
        scanned=len(rows),
        purged=len(purged),
        responses_scrubbed=scrubbed,
        skipped_busy=len(rows) - len(ready),
        last=last,
    )


async def expire_previous_secrets(session: AsyncSession, *, batch_size: int) -> int:
    """Clear previous signing secrets whose overlap window ended. Not a representation change
    worth a version bump: an expired previous secret already signs nothing."""
    return len((await session.execute(_EXPIRE_SECRETS_SQL, {"batch_size": batch_size})).all())


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    batch_size: int
    max_batches: int
    pause_s: float = 0.0
    payload_retention_s: int | None = None
    expire_events: bool = True

    @classmethod
    def from_settings(cls, settings: Settings) -> RetentionPolicy:
        return cls(
            batch_size=settings.retention_batch_size,
            max_batches=settings.retention_max_batches,
            pause_s=settings.retention_batch_pause_ms / 1000,
            payload_retention_s=settings.event_payload_retention_s,
        )


@dataclass(slots=True)
class RetentionReport:
    events: int = 0
    payloads: int = 0
    responses_scrubbed: int = 0
    payloads_skipped_busy: int = 0
    previous_secrets: int = 0
    batches: int = 0
    complete: bool = True
    elapsed_ms: int = 0

    @property
    def total(self) -> int:
        return self.events + self.payloads + self.previous_secrets


class RetentionRunner:
    """Runs every retention task to completion or to its batch budget."""

    def __init__(
        self,
        database: Database,
        policy: RetentionPolicy,
        *,
        should_stop: Callable[[], bool] = lambda: False,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.database = database
        self.policy = policy
        self._should_stop = should_stop
        self._sleep = sleep

    async def _pause(self) -> None:
        if self.policy.pause_s > 0:
            await self._sleep(self.policy.pause_s)

    async def _run_counting(
        self, report: RetentionReport, step: Callable[[AsyncSession], Awaitable[int]]
    ) -> int:
        """Repeat a delete/update batch until a short batch, the budget, or stop."""
        total = 0
        for done in range(self.policy.max_batches):
            if self._should_stop():
                report.complete = False
                return total
            if done:
                await self._pause()
            async with self.database.session() as session:
                n = await step(session)
            report.batches += 1
            total += n
            if n < self.policy.batch_size:
                return total
        report.complete = False
        return total

    async def _run_payloads(self, report: RetentionReport, older_than_s: int) -> None:
        after: Cursor | None = None
        for done in range(self.policy.max_batches):
            if self._should_stop():
                report.complete = False
                return
            if done:
                await self._pause()
            async with self.database.session() as session:
                batch = await purge_payloads(
                    session,
                    older_than_s=older_than_s,
                    batch_size=self.policy.batch_size,
                    after=after,
                )
            report.batches += 1
            report.payloads += batch.purged
            report.responses_scrubbed += batch.responses_scrubbed
            report.payloads_skipped_busy += batch.skipped_busy
            if batch.scanned < self.policy.batch_size:
                return
            after = batch.last
        report.complete = False

    async def run(self) -> RetentionReport:
        started = time.monotonic()
        report = RetentionReport()
        size = self.policy.batch_size
        if self.policy.expire_events:
            report.events = await self._run_counting(
                report, lambda s: purge_expired_events(s, batch_size=size)
            )
        if self.policy.payload_retention_s is not None:
            await self._run_payloads(report, self.policy.payload_retention_s)
        report.previous_secrets = await self._run_counting(
            report, lambda s: expire_previous_secrets(s, batch_size=size)
        )
        report.elapsed_ms = int((time.monotonic() - started) * 1000)
        return report
