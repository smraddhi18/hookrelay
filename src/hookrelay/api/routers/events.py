"""Event ingestion and the event read API.

``POST /v1/events`` needs ``publisher``; reads need ``reader``. Ingestion answers ``202
Accepted``: the event is durably stored with its outbox row, and the relay creates deliveries
asynchronously.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Header, Query, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, ValidationError

from hookrelay.api.body import canonical_json, parse_json, read_body
from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.events import (
    DeliveryOut,
    EventAccepted,
    EventCreate,
    EventOut,
    EventSummary,
)
from hookrelay.api.deps import PublisherDep, ReaderDep, SettingsDep
from hookrelay.core.errors import PayloadTooLargeError
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH
from hookrelay.db.session import SessionDep
from hookrelay.services import events as service
from hookrelay.services import idempotency

router = APIRouter(prefix="/events", tags=["events"])

IdempotencyKeyHeader = Annotated[
    str | None,
    Header(
        alias=idempotency.IDEMPOTENCY_HEADER,
        description="Optional; 1 to 255 printable ASCII characters. Retrying with the same key "
        "and body replays the original response (`Idempotent-Replayed: true`) instead of "
        "creating a second event.",
    ),
]

_ACCEPTED_HEADERS = {
    "Location": {"description": "URL of the accepted event", "schema": {"type": "string"}},
    idempotency.REPLAYED_HEADER: {
        "description": "`true` when this is a stored response replayed for an Idempotency-Key",
        "schema": {"type": "string"},
    },
}


def _validation_error(exc: ValidationError) -> RequestValidationError:
    return RequestValidationError(
        [
            {"loc": ("body", *err["loc"]), "msg": err["msg"], "type": err["type"]}
            for err in exc.errors(include_url=False)
        ]
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=EventAccepted,
    summary="Publish an event (queued for fan-out to subscribed endpoints)",
    responses={
        202: {"headers": _ACCEPTED_HEADERS},
        **error_responses(400, 409, 413, 415),
    },
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": EventCreate.model_json_schema()}},
        }
    },
)
async def publish_event(
    request: Request,
    principal: PublisherDep,
    session: SessionDep,
    settings: SettingsDep,
    idempotency_key: IdempotencyKeyHeader = None,
) -> JSONResponse:
    key = idempotency.validate_key(idempotency_key) if idempotency_key is not None else None
    raw = await read_body(request, max_bytes=settings.event_max_request_bytes)
    document: Any = parse_json(raw)
    try:
        body = EventCreate.model_validate(document)
    except ValidationError as exc:
        raise _validation_error(exc) from None

    canonical_payload = canonical_json(body.payload)
    payload_size = len(canonical_payload)
    if payload_size > settings.event_max_payload_bytes:
        raise PayloadTooLargeError(
            f"Event payload exceeds the {settings.event_max_payload_bytes}-byte limit",
            code="payload_too_large",
            details={"max_bytes": settings.event_max_payload_bytes, "payload_bytes": payload_size},
        )

    path = request.url.path
    if key is not None:
        request_fingerprint = idempotency.fingerprint(
            request.method, path, canonical_json(body.model_dump(mode="json"))
        )
        stored = await idempotency.claim(
            session,
            tenant_id=principal.tenant_id,
            key=key,
            method=request.method,
            path=path,
            request_fingerprint=request_fingerprint,
            ttl_s=settings.idempotency_key_ttl_s,
            lock_timeout_ms=settings.idempotency_lock_timeout_ms,
        )
        if stored is not None:
            return JSONResponse(
                stored.body,
                status_code=stored.status_code,
                headers={
                    idempotency.REPLAYED_HEADER: "true",
                    "Location": f"{path}/{stored.body.get('id')}",
                },
            )

    event = await service.ingest_event(
        session,
        tenant_id=principal.tenant_id,
        new=service.NewEvent(
            event_type=body.event_type,
            payload=body.payload,
            payload_size=payload_size,
            external_id=body.external_id,
            occurred_at=body.occurred_at,
            idempotency_key=key,
        ),
        retention_days=settings.event_retention_days,
        max_future_skew_s=settings.event_occurred_at_max_future_s,
    )
    accepted = EventAccepted.from_model(event).model_dump(mode="json")
    if key is not None:
        await idempotency.complete(
            session,
            tenant_id=principal.tenant_id,
            key=key,
            event_id=event.id,
            response=idempotency.StoredResponse(
                status_code=status.HTTP_202_ACCEPTED, body=accepted
            ),
        )
    return JSONResponse(
        accepted,
        status_code=status.HTTP_202_ACCEPTED,
        headers={"Location": f"{path}/{event.id}"},
    )


@router.get("", response_model=PageResponse[EventSummary], summary="List events (newest first)")
async def list_events(
    *,
    principal: ReaderDep,
    session: SessionDep,
    event_type: Annotated[
        str | None,
        Query(max_length=MAX_EVENT_TYPE_LENGTH, pattern=EVENT_TYPE_PATTERN),
    ] = None,
    created_after: Annotated[
        AwareDatetime | None, Query(description="Inclusive lower bound on created_at.")
    ] = None,
    created_before: Annotated[
        AwareDatetime | None, Query(description="Exclusive upper bound on created_at.")
    ] = None,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[EventSummary]:
    page = await service.list_events(
        session,
        principal.tenant_id,
        event_type=event_type,
        created_after=created_after,
        created_before=created_before,
        cursor=cursor,
        limit=limit,
    )
    return PageResponse[EventSummary](
        data=[EventSummary.from_model(e) for e in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.get(
    "/{event_id}",
    response_model=EventOut,
    summary="Get an event, including its payload",
    responses=error_responses(404),
)
async def get_event(event_id: uuid.UUID, principal: ReaderDep, session: SessionDep) -> EventOut:
    return EventOut.from_model(await service.get_event(session, principal.tenant_id, event_id))


@router.get(
    "/{event_id}/deliveries",
    response_model=PageResponse[DeliveryOut],
    summary="List the deliveries created for an event",
    responses=error_responses(404),
)
async def list_event_deliveries(
    event_id: uuid.UUID,
    principal: ReaderDep,
    session: SessionDep,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[DeliveryOut]:
    page = await service.list_event_deliveries(
        session, principal.tenant_id, event_id, cursor=cursor, limit=limit
    )
    return PageResponse[DeliveryOut](
        data=[DeliveryOut.from_model(d) for d in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )
