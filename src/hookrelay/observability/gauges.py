"""Refresh queue-depth, circuit-breaker and partition gauges from Postgres (no tenant payloads)."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.partitioning import DELIVERY_ATTEMPTS, PartitionedTable
from hookrelay.observability.metrics import Metrics

_QUEUE_SQL = text(
    """
    SELECT
      (SELECT count(*) FROM event_outbox WHERE processed_at IS NULL) AS outbox_pending,
      (SELECT coalesce(
           extract(epoch FROM (now() - min(created_at))), 0
         ) FROM event_outbox WHERE processed_at IS NULL) AS outbox_lag_seconds,
      (SELECT count(*) FROM deliveries WHERE status = 'pending') AS deliveries_pending,
      (SELECT count(*) FROM deliveries d
        WHERE d.status = 'pending' AND d.next_attempt_at <= now()
          AND NOT EXISTS (
            SELECT 1 FROM endpoint_states s
            WHERE s.endpoint_id = d.endpoint_id
              AND s.breaker_state = 'open' AND s.open_until > now()
          )) AS deliveries_due,
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


# Catalog-only (no table scan): daily partitions, the newest upper bound, and whether a DEFAULT
# partition exists. pg_get_expr renders bounds with an explicit offset, so the cast is exact.
_PARTITIONS_SQL = text(
    r"""
    SELECT count(*) FILTER (WHERE b.expr <> 'DEFAULT') AS partitions,
           extract(epoch FROM (
               max((regexp_match(b.expr, 'TO \(''([^'']+)''\)'))[1]::timestamptz) - now()
           )) AS premade_seconds,
           bool_or(b.expr = 'DEFAULT') AS has_default
    FROM pg_inherits AS i
    JOIN pg_class AS c ON c.oid = i.inhrelid
    CROSS JOIN LATERAL (SELECT pg_get_expr(c.relpartbound, c.oid) AS expr) AS b
    WHERE i.inhparent = to_regclass(:table) AND NOT i.inhdetachpending
    """
)
# Rows routed to DEFAULT mean premaking fell behind; bounded so a huge one stays cheap.
DEFAULT_ROWS_CAP = 10_000


@dataclass(frozen=True, slots=True)
class PartitionSnapshot:
    table: str
    partitions: int
    premade_seconds: float
    default_rows: int


async def snapshot_partitions(
    session: AsyncSession, table: PartitionedTable = DELIVERY_ATTEMPTS
) -> PartitionSnapshot:
    row = (await session.execute(_PARTITIONS_SQL, {"table": table.name})).one()
    default_rows = 0
    if row.has_default:
        default_rows = int(
            await session.scalar(
                text(
                    f'SELECT count(*) FROM (SELECT 1 FROM ONLY "{table.default_partition}" '  # noqa: S608 - our own identifier
                    f"LIMIT {DEFAULT_ROWS_CAP}) AS s"
                )
            )
            or 0
        )
    return PartitionSnapshot(
        table=table.name,
        partitions=int(row.partitions or 0),
        premade_seconds=float(row.premade_seconds or 0),
        default_rows=default_rows,
    )


async def refresh_partition_gauges(session: AsyncSession, metrics: Metrics) -> PartitionSnapshot:
    snap = await snapshot_partitions(session)
    metrics.set_partition_state(
        table=snap.table,
        partitions=snap.partitions,
        premade_seconds=snap.premade_seconds,
        default_rows=snap.default_rows,
    )
    return snap


@dataclass(frozen=True, slots=True)
class QueueSnapshot:
    outbox_pending: int
    outbox_lag_seconds: float
    deliveries_pending: int
    deliveries_due: int
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
        deliveries_due=int(row.deliveries_due or 0),
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
        deliveries_due=snap.deliveries_due,
        deliveries_delivering=snap.deliveries_delivering,
    )
    metrics.set_breaker_counts(
        closed=snap.breaker_closed, open_=snap.breaker_open, half_open=snap.breaker_half_open
    )
    await refresh_partition_gauges(session, metrics)
    return snap
