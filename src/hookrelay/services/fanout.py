"""Fan-out: turn claimed outbox rows into one ``deliveries`` row per (event, endpoint).

Exactly-once fan-out rests on two independent guarantees:

* **Claiming** uses ``FOR UPDATE SKIP LOCKED``, so concurrent relays take disjoint batches and
  never wait on each other.
* **Inserting** uses ``ON CONFLICT (event_id, endpoint_id) DO NOTHING`` against
  ``uq_deliveries_event_endpoint``. If a relay crashes after inserting but before its commit,
  everything rolls back; if a row is ever re-processed (manual replay, a bug, a restored
  backup), the constraint absorbs the duplicates. The relay can therefore be at-least-once
  internally while the observable result is exactly one delivery per endpoint.

Subscriptions are evaluated when the relay processes the event (normally well under a second
after ingestion), against endpoints that are enabled and not deleted at that moment.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_, func, literal, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.models import Delivery, Endpoint, Event, OutboxEntry, Subscription
from hookrelay.redis_.subscription_cache import SubscriptionCache
from hookrelay.services.subscriptions import pattern_matches_sql

MAX_ERROR_LENGTH = 1_000


@dataclass(frozen=True, slots=True)
class ClaimedRow:
    outbox_id: int
    event_id: uuid.UUID
    tenant_id: uuid.UUID
    attempts: int


async def claim_batch(session: AsyncSession, *, batch_size: int) -> list[ClaimedRow]:
    """Lock up to ``batch_size`` due, unprocessed outbox rows (held until commit/rollback)."""
    rows = (
        await session.execute(
            select(
                OutboxEntry.id, OutboxEntry.event_id, OutboxEntry.tenant_id, OutboxEntry.attempts
            )
            .where(OutboxEntry.processed_at.is_(None), OutboxEntry.available_at <= func.now())
            .order_by(OutboxEntry.available_at, OutboxEntry.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    ).all()
    return [
        ClaimedRow(outbox_id=r.id, event_id=r.event_id, tenant_id=r.tenant_id, attempts=r.attempts)
        for r in rows
    ]


async def matching_endpoint_ids(
    session: AsyncSession, tenant_id: uuid.UUID, event_type: str
) -> list[uuid.UUID]:
    """Enabled, non-deleted endpoint IDs subscribed to ``event_type`` (SQL source of truth)."""
    rows = (
        await session.execute(
            select(Endpoint.id)
            .distinct()
            .join(
                Subscription,
                and_(
                    Subscription.tenant_id == Endpoint.tenant_id,
                    Subscription.endpoint_id == Endpoint.id,
                ),
            )
            .where(
                Endpoint.tenant_id == tenant_id,
                Endpoint.deleted_at.is_(None),
                Endpoint.enabled.is_(True),
                pattern_matches_sql(Subscription.event_type_pattern, literal(event_type)),
            )
            .order_by(Endpoint.id)
        )
    ).all()
    return [r.id for r in rows]


async def fan_out_sql(session: AsyncSession, event_ids: Sequence[uuid.UUID]) -> int:
    """Create the missing deliveries for ``event_ids`` in one set-based statement.

    Returns how many delivery rows were inserted (rows that already existed are not counted).
    """
    matches = (
        select(
            Event.tenant_id,
            Event.id,
            Endpoint.id,
            Event.retain_until,
        )
        .distinct()
        .select_from(Event)
        .join(Subscription, Subscription.tenant_id == Event.tenant_id)
        .join(
            Endpoint,
            and_(
                Endpoint.tenant_id == Subscription.tenant_id,
                Endpoint.id == Subscription.endpoint_id,
            ),
        )
        .where(
            Event.id.in_(event_ids),
            Endpoint.deleted_at.is_(None),
            Endpoint.enabled.is_(True),
            pattern_matches_sql(Subscription.event_type_pattern, Event.event_type),
        )
    )
    stmt = (
        pg_insert(Delivery)
        .from_select(["tenant_id", "event_id", "endpoint_id", "retain_until"], matches)
        .on_conflict_do_nothing(constraint="uq_deliveries_event_endpoint")
        .returning(Delivery.id)
    )
    result = await session.execute(stmt)
    return len(result.all())


async def fan_out_cached(
    session: AsyncSession, event_ids: Sequence[uuid.UUID], cache: SubscriptionCache
) -> int:
    """Cache-aside fan-out: membership from Redis (SQL on miss), insert with ON CONFLICT.

    Never invents matches. ON CONFLICT keeps the exactly-once insert invariant.
    """
    events = (
        await session.execute(
            select(Event.id, Event.tenant_id, Event.event_type, Event.retain_until).where(
                Event.id.in_(list(event_ids))
            )
        )
    ).all()
    if not events:
        return 0

    values: list[dict[str, object]] = []
    seen: set[tuple[uuid.UUID, uuid.UUID]] = set()

    async def _loader(tenant_id: uuid.UUID, event_type: str) -> list[uuid.UUID]:
        return await matching_endpoint_ids(session, tenant_id, event_type)

    for event in events:
        endpoint_ids = await cache.get_or_load(event.tenant_id, event.event_type, _loader)
        for endpoint_id in endpoint_ids:
            key = (event.id, endpoint_id)
            if key in seen:
                continue
            seen.add(key)
            values.append(
                {
                    "tenant_id": event.tenant_id,
                    "event_id": event.id,
                    "endpoint_id": endpoint_id,
                    "retain_until": event.retain_until,
                }
            )
    if not values:
        return 0
    stmt = (
        pg_insert(Delivery)
        .values(values)
        .on_conflict_do_nothing(constraint="uq_deliveries_event_endpoint")
        .returning(Delivery.id)
    )
    result = await session.execute(stmt)
    return len(result.all())


async def fan_out(
    session: AsyncSession,
    event_ids: Sequence[uuid.UUID],
    *,
    cache: SubscriptionCache | None = None,
) -> int:
    """Create the missing deliveries for ``event_ids``.

    Prefer the subscription cache when it is available; fall back to the set-based SQL join
    on miss/error/disabled so correctness never depends on Redis.
    """
    if not event_ids:
        return 0
    if cache is not None and cache.is_available():
        try:
            return await fan_out_cached(session, event_ids, cache)
        except Exception:
            return await fan_out_sql(session, event_ids)
    return await fan_out_sql(session, event_ids)


async def mark_processed(session: AsyncSession, rows: Sequence[ClaimedRow]) -> None:
    outbox_ids = [r.outbox_id for r in rows]
    event_ids = [r.event_id for r in rows]
    await session.execute(
        update(OutboxEntry)
        .where(OutboxEntry.id.in_(outbox_ids))
        .values(processed_at=func.now(), attempts=OutboxEntry.attempts + 1, last_error=None)
        .execution_options(synchronize_session=False)
    )
    await session.execute(
        update(Event)
        .where(Event.id.in_(event_ids), Event.fanned_out_at.is_(None))
        .values(fanned_out_at=func.now())
        .execution_options(synchronize_session=False)
    )


def retry_delay_s(attempts: int, *, max_s: int) -> int:
    """Exponential backoff for an outbox row that failed ``attempts`` times: 1, 2, 4 ... max."""
    return int(min(max_s, 2 ** min(max(attempts - 1, 0), 30)))


async def mark_failed(
    session: AsyncSession, row: ClaimedRow, error: str, *, retry_max_s: int
) -> int:
    """Record a failure and push the row back by the backoff; returns the delay in seconds."""
    attempts = row.attempts + 1
    delay = retry_delay_s(attempts, max_s=retry_max_s)
    await session.execute(
        update(OutboxEntry)
        .where(OutboxEntry.id == row.outbox_id)
        .values(
            attempts=attempts,
            last_error=error[:MAX_ERROR_LENGTH],
            available_at=func.now() + timedelta(seconds=delay),
        )
        .execution_options(synchronize_session=False)
    )
    return delay
