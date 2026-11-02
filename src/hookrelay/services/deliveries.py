"""Delivery read API: one delivery and its attempt log, always scoped to the caller's tenant."""

from __future__ import annotations

import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import NotFoundError
from hookrelay.db.models import Delivery, DeliveryAttempt
from hookrelay.services.pagination import Page, paginate


async def get_delivery(
    session: AsyncSession, tenant_id: uuid.UUID, delivery_id: uuid.UUID
) -> Delivery:
    delivery = await session.scalar(
        select(Delivery).where(Delivery.tenant_id == tenant_id, Delivery.id == delivery_id)
    )
    if delivery is None:
        raise NotFoundError("Delivery not found", code="delivery_not_found")
    return delivery


async def list_attempts(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    delivery_id: uuid.UUID,
    *,
    cursor: str | None,
    limit: int,
) -> Page[DeliveryAttempt]:
    """Attempts newest first (keyset on ``(created_at, id)``); 404 for another tenant's id."""
    await get_delivery(session, tenant_id, delivery_id)
    stmt = select(DeliveryAttempt).where(
        DeliveryAttempt.tenant_id == tenant_id, DeliveryAttempt.delivery_id == delivery_id
    )
    return await paginate(
        session,
        stmt,
        created_col=DeliveryAttempt.created_at,
        id_col=DeliveryAttempt.id,
        cursor=cursor,
        limit=limit,
    )
