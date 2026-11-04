"""Delivery read API: search, one delivery and its attempt log, always tenant-scoped.

Search (``GET /v1/deliveries``) is a keyset page newest first on ``(created_at, id)``. Each
filter combination has an index whose leading columns match it, followed by
``(created_at, id)``, so a page is a bounded index range scan rather than a sort:

* status: ``ix_deliveries_tenant_status_created``; several statuses become a merge of ranges;
* event type: ``ix_deliveries_tenant_type_created`` (``deliveries.event_type`` is a copy of
  the event's type maintained by a trigger, so no join to ``events`` is needed);
* endpoint: ``ix_deliveries_endpoint_created``;
* no filter / time range only: ``ix_deliveries_tenant_created``.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import NotFoundError, UnprocessableEntityError
from hookrelay.db.models import Delivery, DeliveryAttempt, DeliveryStatus
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


@dataclass(frozen=True, slots=True)
class DeliverySearch:
    statuses: Sequence[DeliveryStatus] = ()
    event_type: str | None = None
    endpoint_id: uuid.UUID | None = None
    created_after: datetime | None = None  # inclusive
    created_before: datetime | None = None  # exclusive

    def __post_init__(self) -> None:
        if (
            self.created_after is not None
            and self.created_before is not None
            and self.created_before <= self.created_after
        ):
            raise UnprocessableEntityError(
                "created_before must be later than created_after",
                code="invalid_time_range",
                details=[{"loc": ["query", "created_before"]}],
            )


async def search_deliveries(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    search: DeliverySearch,
    *,
    cursor: str | None,
    limit: int,
) -> Page[Delivery]:
    """Deliveries matching every given filter, newest first.

    An endpoint id of another tenant simply matches nothing (the tenant predicate is always
    applied), which is indistinguishable from an endpoint without deliveries.
    """
    stmt = select(Delivery).where(Delivery.tenant_id == tenant_id)
    statuses = sorted({DeliveryStatus(s).value for s in search.statuses})
    if statuses:
        stmt = stmt.where(Delivery.status.in_(statuses))
    if search.event_type is not None:
        stmt = stmt.where(Delivery.event_type == search.event_type)
    if search.endpoint_id is not None:
        stmt = stmt.where(Delivery.endpoint_id == search.endpoint_id)
    if search.created_after is not None:
        stmt = stmt.where(Delivery.created_at >= search.created_after)
    if search.created_before is not None:
        stmt = stmt.where(Delivery.created_at < search.created_before)
    return await paginate(
        session,
        stmt,
        created_col=Delivery.created_at,
        id_col=Delivery.id,
        cursor=cursor,
        limit=limit,
    )
