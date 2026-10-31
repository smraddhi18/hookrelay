"""Event type registry: the vocabulary of ``event_type`` names a tenant publishes.

Registering types up front lets the API reject typos in exact subscriptions
(``invoice.payed``) instead of silently never delivering. Types are archived, never deleted:
historical subscriptions and (from M2) messages keep referring to them.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog
from sqlalchemy import case, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import ConflictError, NotFoundError, PreconditionFailedError
from hookrelay.db.integrity import violated_constraint
from hookrelay.db.models import EventType
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.event_types")


async def create_event_type(
    session: AsyncSession, *, tenant_id: uuid.UUID, name: str, description: str | None
) -> EventType:
    event_type = EventType(tenant_id=tenant_id, name=name, description=description)
    session.add(event_type)
    try:
        await session.flush()
    except IntegrityError as exc:
        if violated_constraint(exc) == "uq_event_types_tenant_id_name":
            raise ConflictError(
                f"Event type {name!r} is already registered", code="event_type_exists"
            ) from exc
        raise
    logger.info("event_type.created", tenant_id=str(tenant_id), event_type=name)
    return event_type


async def get_event_type(session: AsyncSession, tenant_id: uuid.UUID, name: str) -> EventType:
    event_type = await session.scalar(
        select(EventType).where(EventType.tenant_id == tenant_id, EventType.name == name)
    )
    if event_type is None:
        raise NotFoundError(f"Event type {name!r} not found", code="event_type_not_found")
    return event_type


async def list_event_types(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    include_archived: bool,
    cursor: str | None,
    limit: int,
) -> Page[EventType]:
    stmt = select(EventType).where(EventType.tenant_id == tenant_id)
    if not include_archived:
        stmt = stmt.where(EventType.archived_at.is_(None))
    return await paginate(
        session,
        stmt,
        created_col=EventType.created_at,
        id_col=EventType.id,
        cursor=cursor,
        limit=limit,
    )


async def update_event_type(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    name: str,
    *,
    expected_version: int | None,
    changes: dict[str, Any],
) -> EventType:
    values: dict[str, Any] = {}
    if "description" in changes:
        values["description"] = changes["description"]
    if "archived" in changes:
        # Keep the original archived_at when archiving twice.
        values["archived_at"] = (
            case((EventType.archived_at.is_(None), func.now()), else_=EventType.archived_at)
            if changes["archived"]
            else None
        )
    conditions = [EventType.tenant_id == tenant_id, EventType.name == name]
    if expected_version is not None:
        conditions.append(EventType.version == expected_version)
    event_type = await session.scalar(
        update(EventType)
        .where(*conditions)
        .values(**values, version=EventType.version + 1)
        .returning(EventType)
        .execution_options(populate_existing=True)
    )
    if event_type is None:
        current = await get_event_type(session, tenant_id, name)  # 404 when it does not exist
        raise PreconditionFailedError(
            "Event type was modified by another request; re-fetch and retry",
            details={"current_version": current.version},
            headers={"ETag": f'"{current.version}"'},
        )
    logger.info(
        "event_type.updated",
        tenant_id=str(tenant_id),
        event_type=name,
        fields=sorted(changes),
        version=event_type.version,
    )
    return event_type
