"""Refresh queue-depth and circuit-breaker gauges from Postgres (no tenant payloads)."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.observability.metrics import Metrics

_QUEUE_SQL = text(
    """
    SELECT
      (SELECT count(*) FROM event_outbox WHERE processed_at IS NULL) AS outbox_pending,
      (SELECT coalesce(
           extract(epoch FROM (now() - min(created_at))), 0
         ) FROM event_outbox WHERE processed_at IS NULL) AS outbox_lag_seconds,
      (SELECT count(*) FROM deliveries WHERE status = 'pending') AS deliveries_pending,
      (SELECT count(*) FROM deliveries WHERE status = 'delivering') AS deliveries_delivering
    """
)

_BREAKER_SQL = text(
    """
    SELECT breaker_state, count(*) AS n
    FROM endpoint_states
    GROUP BY breaker_state
    """
)


@dataclass(frozen=True, slots=True)
class QueueSnapshot:
    outbox_pending: int
    outbox_lag_seconds: float
    deliveries_pending: int
    deliveries_delivering: int
    breaker_closed: int
    breaker_open: int
    breaker_half_open: int


async def snapshot_queue(session: AsyncSession) -> QueueSnapshot:
    row = (await session.execute(_QUEUE_SQL)).one()
    counts = {r.breaker_state: int(r.n) for r in (await session.execute(_BREAKER_SQL)).all()}
    return QueueSnapshot(
        outbox_pending=int(row.outbox_pending or 0),
        outbox_lag_seconds=float(row.outbox_lag_seconds or 0),
        deliveries_pending=int(row.deliveries_pending or 0),
        deliveries_delivering=int(row.deliveries_delivering or 0),
        breaker_closed=counts.get("closed", 0),
        breaker_open=counts.get("open", 0),
        breaker_half_open=counts.get("half_open", 0),
    )


async def refresh_gauges(session: AsyncSession, metrics: Metrics) -> QueueSnapshot:
    snap = await snapshot_queue(session)
    metrics.set_queue_depth(
        outbox_pending=snap.outbox_pending,
        outbox_lag_seconds=snap.outbox_lag_seconds,
        deliveries_pending=snap.deliveries_pending,
        deliveries_delivering=snap.deliveries_delivering,
    )
    metrics.set_breaker_counts(
        closed=snap.breaker_closed, open_=snap.breaker_open, half_open=snap.breaker_half_open
    )
    return snap
