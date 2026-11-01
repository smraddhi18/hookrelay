"""Periodic, batched cleanup run by the relay.

Every sweep deletes at most ``batch_size`` rows per table per call and skips rows locked by
someone else, so it never blocks ingestion or fan-out and never holds a long transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.models import Event, OutboxEntry
from hookrelay.services import idempotency


@dataclass(frozen=True, slots=True)
class SweepResult:
    idempotency_keys: int
    outbox_rows: int
    events: int

    @property
    def total(self) -> int:
        return self.idempotency_keys + self.outbox_rows + self.events


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


async def sweep(session: AsyncSession, *, outbox_retention_s: int, batch_size: int) -> SweepResult:
    return SweepResult(
        idempotency_keys=await idempotency.purge_expired(session, batch_size=batch_size),
        outbox_rows=await purge_processed_outbox(
            session, older_than_s=outbox_retention_s, batch_size=batch_size
        ),
        events=await purge_expired_events(session, batch_size=batch_size),
    )
