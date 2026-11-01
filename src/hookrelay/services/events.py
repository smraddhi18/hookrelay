"""Event ingestion (transactional outbox) and the event/delivery read side.

``ingest_event`` inserts the event and its outbox row in the caller's transaction. The caller
(the route) also writes the idempotency record in that same transaction, so after commit
either all three exist or none do: there is no window where an event is accepted but never
fanned out, or fanned out but unknown to the idempotency store.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import structlog
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import defer

from hookrelay.core.errors import ConflictError, NotFoundError, UnprocessableEntityError
from hookrelay.db.integrity import violated_constraint
from hookrelay.db.models import Delivery, Event, EventType, OutboxEntry
from hookrelay.services.clock import db_now
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.events")


@dataclass(frozen=True, slots=True)
class NewEvent:
    event_type: str
    payload: dict[str, Any]
    payload_size: int
    external_id: str | None = None
    occurred_at: datetime | None = None
    idempotency_key: str | None = None


async def _require_publishable_type(session: AsyncSession, tenant_id: uuid.UUID, name: str) -> None:
    row = (
        await session.execute(
            select(EventType.archived_at).where(
                EventType.tenant_id == tenant_id, EventType.name == name
            )
        )
    ).first()
    if row is None:
        raise UnprocessableEntityError(
            f"Event type {name!r} is not registered",
            code="unknown_event_type",
            details={"event_type": name},
        )
    if row.archived_at is not None:
        raise UnprocessableEntityError(
            f"Event type {name!r} is archived and accepts no new events",
            code="event_type_archived",
            details={"event_type": name},
        )


async def ingest_event(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    new: NewEvent,
    retention_days: int,
    max_future_skew_s: int,
) -> Event:
    await _require_publishable_type(session, tenant_id, new.event_type)
    now = await db_now(session)
    if new.occurred_at is not None and new.occurred_at > now + timedelta(seconds=max_future_skew_s):
        raise UnprocessableEntityError(
            "occurred_at is too far in the future",
            code="occurred_at_in_future",
            details={"max_future_skew_s": max_future_skew_s},
        )
    values: dict[str, Any] = {
        "tenant_id": tenant_id,
        "event_type": new.event_type,
        "external_id": new.external_id,
        "payload": new.payload,
        "payload_size": new.payload_size,
        "idempotency_key": new.idempotency_key,
        # now() is fixed for the transaction, so this is exactly created_at + retention.
        "retain_until": now + timedelta(days=retention_days),
    }
    if new.occurred_at is not None:
        values["occurred_at"] = new.occurred_at
    try:
        # Savepoint: a duplicate external_id must not poison the transaction we report from.
        async with session.begin_nested():
            event = (await session.scalars(insert(Event).values(**values).returning(Event))).one()
    except IntegrityError as exc:
        if violated_constraint(exc) == "uq_events_tenant_external_id":
            existing = await session.scalar(
                select(Event.id).where(
                    Event.tenant_id == tenant_id, Event.external_id == new.external_id
                )
            )
            raise ConflictError(
                f"An event with external_id {new.external_id!r} already exists",
                code="event_exists",
                details={"event_id": str(existing) if existing else None},
            ) from exc
        raise
    await session.execute(insert(OutboxEntry).values(tenant_id=tenant_id, event_id=event.id))
    logger.info(
        "event.accepted",
        tenant_id=str(tenant_id),
        event_id=str(event.id),
        event_type=event.event_type,
        payload_size=event.payload_size,
    )
    return event


async def get_event(session: AsyncSession, tenant_id: uuid.UUID, event_id: uuid.UUID) -> Event:
    event = await session.scalar(
        select(Event).where(Event.tenant_id == tenant_id, Event.id == event_id)
    )
    if event is None:
        raise NotFoundError("Event not found", code="event_not_found")
    return event


async def list_events(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    event_type: str | None,
    created_after: datetime | None,
    created_before: datetime | None,
    cursor: str | None,
    limit: int,
) -> Page[Event]:
    # The payload is never loaded for lists (raiseload makes an accidental access loud).
    stmt = (
        select(Event)
        .options(defer(Event.payload, raiseload=True))
        .where(Event.tenant_id == tenant_id)
    )
    if event_type is not None:
        stmt = stmt.where(Event.event_type == event_type)
    if created_after is not None:
        stmt = stmt.where(Event.created_at >= created_after)
    if created_before is not None:
        stmt = stmt.where(Event.created_at < created_before)
    return await paginate(
        session, stmt, created_col=Event.created_at, id_col=Event.id, cursor=cursor, limit=limit
    )


async def list_event_deliveries(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    event_id: uuid.UUID,
    *,
    cursor: str | None,
    limit: int,
) -> Page[Delivery]:
    exists = await session.scalar(
        select(Event.id).where(Event.tenant_id == tenant_id, Event.id == event_id)
    )
    if exists is None:
        raise NotFoundError("Event not found", code="event_not_found")
    stmt = select(Delivery).where(Delivery.tenant_id == tenant_id, Delivery.event_id == event_id)
    return await paginate(
        session,
        stmt,
        created_col=Delivery.created_at,
        id_col=Delivery.id,
        cursor=cursor,
        limit=limit,
    )
