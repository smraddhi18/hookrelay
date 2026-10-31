"""Event type registry. Reads need ``reader``; registering/updating needs ``publisher``."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Path, Query, Response, status

from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.event_types import EventTypeCreate, EventTypeOut, EventTypeUpdate
from hookrelay.api.deps import PublisherDep, ReaderDep
from hookrelay.api.etag import IfMatchHeader, parse_if_match, set_etag
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH
from hookrelay.db.session import SessionDep
from hookrelay.services import event_types as service

router = APIRouter(prefix="/event-types", tags=["event-types"])

EventTypeName = Annotated[
    str,
    Path(max_length=MAX_EVENT_TYPE_LENGTH, pattern=EVENT_TYPE_PATTERN, examples=["invoice.paid"]),
]


@router.get("", response_model=PageResponse[EventTypeOut], summary="List event types")
async def list_event_types(
    principal: ReaderDep,
    session: SessionDep,
    include_archived: Annotated[bool, Query(description="Also return archived types.")] = False,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[EventTypeOut]:
    page = await service.list_event_types(
        session,
        principal.tenant_id,
        include_archived=include_archived,
        cursor=cursor,
        limit=limit,
    )
    return PageResponse[EventTypeOut](
        data=[EventTypeOut.from_model(e) for e in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=EventTypeOut,
    summary="Register an event type",
    responses=error_responses(409),
)
async def create_event_type(
    body: EventTypeCreate, principal: PublisherDep, session: SessionDep, response: Response
) -> EventTypeOut:
    event_type = await service.create_event_type(
        session, tenant_id=principal.tenant_id, name=body.name, description=body.description
    )
    response.headers["Location"] = f"/v1/event-types/{event_type.name}"
    set_etag(response, event_type.version)
    return EventTypeOut.from_model(event_type)


@router.get(
    "/{name}",
    response_model=EventTypeOut,
    summary="Get an event type by name",
    responses=error_responses(404),
)
async def get_event_type(
    name: EventTypeName, principal: ReaderDep, session: SessionDep, response: Response
) -> EventTypeOut:
    event_type = await service.get_event_type(session, principal.tenant_id, name)
    set_etag(response, event_type.version)
    return EventTypeOut.from_model(event_type)


@router.patch(
    "/{name}",
    response_model=EventTypeOut,
    summary="Update description / archive or unarchive (requires If-Match)",
    responses=error_responses(404, 412, 428),
)
async def update_event_type(
    *,
    name: EventTypeName,
    body: EventTypeUpdate,
    principal: PublisherDep,
    session: SessionDep,
    response: Response,
    if_match: IfMatchHeader = None,
) -> EventTypeOut:
    expected = parse_if_match(if_match, required=True)
    event_type = await service.update_event_type(
        session, principal.tenant_id, name, expected_version=expected, changes=body.changes()
    )
    set_etag(response, event_type.version)
    return EventTypeOut.from_model(event_type)
