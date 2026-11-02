"""Delivery read API: delivery state and its attempt log (``reader`` role, tenant-scoped).

Another tenant's delivery id answers ``404`` exactly like an unknown id, so ids cannot be probed.
"""

from __future__ import annotations

import uuid

from fastapi import APIRouter

from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.deliveries import DeliveryAttemptOut
from hookrelay.api.contracts.events import DeliveryOut
from hookrelay.api.deps import ReaderDep
from hookrelay.db.session import SessionDep
from hookrelay.services import deliveries as service

router = APIRouter(prefix="/deliveries", tags=["deliveries"])


@router.get(
    "/{delivery_id}",
    response_model=DeliveryOut,
    summary="Get a delivery",
    responses=error_responses(404),
)
async def get_delivery(
    delivery_id: uuid.UUID, principal: ReaderDep, session: SessionDep
) -> DeliveryOut:
    return DeliveryOut.from_model(
        await service.get_delivery(session, principal.tenant_id, delivery_id)
    )


@router.get(
    "/{delivery_id}/attempts",
    response_model=PageResponse[DeliveryAttemptOut],
    summary="List a delivery's attempts, newest first",
    responses=error_responses(404),
)
async def list_delivery_attempts(
    delivery_id: uuid.UUID,
    principal: ReaderDep,
    session: SessionDep,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[DeliveryAttemptOut]:
    page = await service.list_attempts(
        session, principal.tenant_id, delivery_id, cursor=cursor, limit=limit
    )
    return PageResponse[DeliveryAttemptOut](
        data=[DeliveryAttemptOut.from_model(a) for a in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )
