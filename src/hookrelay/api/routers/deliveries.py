"""Delivery read API: search, delivery state and its attempt log (``reader``, tenant-scoped).

Another tenant's delivery id answers ``404`` exactly like an unknown id, so ids cannot be probed.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query
from pydantic import AwareDatetime

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
from hookrelay.db.models import DeliveryStatus
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH
from hookrelay.db.session import SessionDep
from hookrelay.services import deliveries as service

router = APIRouter(prefix="/deliveries", tags=["deliveries"])


@router.get(
    "",
    response_model=PageResponse[DeliveryOut],
    summary="Search deliveries (newest first) by status, event type, endpoint and time range",
    responses=error_responses(422),
)
async def search_deliveries(
    *,
    principal: ReaderDep,
    session: SessionDep,
    status: Annotated[
        list[DeliveryStatus] | None,
        Query(
            max_length=len(DeliveryStatus),
            description="Repeat to match any of several statuses, e.g. "
            "`?status=failed&status=dead`.",
        ),
    ] = None,
    event_type: Annotated[
        str | None,
        Query(max_length=MAX_EVENT_TYPE_LENGTH, pattern=EVENT_TYPE_PATTERN),
    ] = None,
    endpoint_id: Annotated[uuid.UUID | None, Query()] = None,
    created_after: Annotated[
        AwareDatetime | None, Query(description="Inclusive lower bound on created_at.")
    ] = None,
    created_before: Annotated[
        AwareDatetime | None,
        Query(description="Exclusive upper bound on created_at; must be after created_after."),
    ] = None,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[DeliveryOut]:
    search = service.DeliverySearch(
        statuses=status or (),
        event_type=event_type,
        endpoint_id=endpoint_id,
        created_after=created_after,
        created_before=created_before,
    )
    page = await service.search_deliveries(
        session, principal.tenant_id, search, cursor=cursor, limit=limit
    )
    return PageResponse[DeliveryOut](
        data=[DeliveryOut.from_model(d) for d in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


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
