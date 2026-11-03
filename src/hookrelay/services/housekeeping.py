"""Periodic, batched cleanup run by the relay.

Every sweep deletes at most ``batch_size`` rows per table per call and skips rows locked by
someone else, so it never blocks ingestion or fan-out and never holds a long transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.models import Event, OutboxEntry, ReplayRequest
from hookrelay.services import idempotency, notifications


@dataclass(frozen=True, slots=True)
class SweepResult:
    idempotency_keys: int
    outbox_rows: int
    events: int
    notifications: int = 0
    replay_requests: int = 0

    @property
    def total(self) -> int:
        return (
            self.idempotency_keys
            + self.outbox_rows
            + self.events
            + self.notifications
            + self.replay_requests
        )


async def purge_processed_outbox(
    session: AsyncSession, *, older_than_s: int, batch_size: int
) -> int:
    doomed = (
        select(OutboxEntry.id)
        .where(
            OutboxEntry.processed_at.is_not(None),
            OutboxEntry.processed_at <= func.now() - timedelta(seconds=older_than_s),
        )
        .order_by(OutboxEntry.processed_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(
        delete(OutboxEntry)
        .where(OutboxEntry.id.in_(doomed))
        .returning(OutboxEntry.id)
        .execution_options(synchronize_session=False)
    )
    return len(result.all())


async def purge_expired_events(session: AsyncSession, *, batch_size: int) -> int:
    """Delete events past ``retain_until``; their outbox rows, deliveries and idempotency
    records go with them (ON DELETE CASCADE)."""
    doomed = (
        select(Event.id)
        .where(Event.retain_until <= func.now())
        .order_by(Event.retain_until)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(
        delete(Event)
        .where(Event.id.in_(doomed))
        .returning(Event.id)
        .execution_options(synchronize_session=False)
    )
    return len(result.all())


async def purge_old_replay_requests(
    session: AsyncSession, *, older_than_s: int, batch_size: int
) -> int:
    """Replay audit rows older than the retention window (the rate limiter only needs the last
    minute; the rest is history)."""
    doomed = (
        select(ReplayRequest.id)
        .where(ReplayRequest.created_at <= func.now() - timedelta(seconds=older_than_s))
        .order_by(ReplayRequest.created_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(
        delete(ReplayRequest)
        .where(ReplayRequest.id.in_(doomed))
        .returning(ReplayRequest.id)
        .execution_options(synchronize_session=False)
    )
    return len(result.all())


async def sweep(
    session: AsyncSession,
    *,
    outbox_retention_s: int,
    batch_size: int,
    notification_retention_s: int | None = None,
    replay_retention_s: int | None = None,
) -> SweepResult:
    return SweepResult(
        idempotency_keys=await idempotency.purge_expired(session, batch_size=batch_size),
        outbox_rows=await purge_processed_outbox(
            session, older_than_s=outbox_retention_s, batch_size=batch_size
        ),
        events=await purge_expired_events(session, batch_size=batch_size),
        notifications=(
            0
            if notification_retention_s is None
            else await notifications.purge_completed(
                session, older_than_s=notification_retention_s, batch_size=batch_size
            )
        ),
        replay_requests=(
            0
            if replay_retention_s is None
            else await purge_old_replay_requests(
                session, older_than_s=replay_retention_s, batch_size=batch_size
            )
        ),
    )
