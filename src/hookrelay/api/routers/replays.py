"""Replay API: re-send finished deliveries (``202 Accepted``; workers do the sending).

* ``POST /v1/deliveries/{id}/replay`` (``publisher``): one delivery.
* ``POST /v1/events/{id}/replay`` (``publisher``): an event's deliveries.
* ``POST /v1/replays`` (``admin``): everything matching a time window and filters.
* ``GET /v1/replays`` (``reader``): the audit trail.

Every POST honours ``Idempotency-Key`` (a retried request replays the stored response instead
of replaying the deliveries twice; an idempotent replay costs no rate-limit budget) and is
rate-limited per tenant (``429 replay_rate_limited`` with ``Retry-After``).
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

from fastapi import APIRouter, Body, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.api.body import canonical_json
from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.events import DeliveryOut
from hookrelay.api.contracts.replays import BulkReplay, EventReplay, ReplayOut
from hookrelay.api.deps import AdminDep, PublisherDep, ReaderDep, SettingsDep, notify_workers
from hookrelay.api.routers.events import IdempotencyKeyHeader
from hookrelay.core.config import Settings
from hookrelay.db.session import SessionDep
from hookrelay.services import idempotency
from hookrelay.services import replays as service
from hookrelay.services.api_keys import Principal

router = APIRouter(tags=["replays"])

_REPLAY_HEADERS = {
    idempotency.REPLAYED_HEADER: {
        "description": "`true` when this is a stored response replayed for an Idempotency-Key",
        "schema": {"type": "string"},
    },
}


def _limits(settings: Settings) -> service.ReplayLimits:
    return service.ReplayLimits(
        per_minute=settings.replay_rate_limit_per_minute,
        max_deliveries=settings.replay_max_deliveries,
        max_window_s=settings.replay_max_window_s,
    )


async def _idempotent(
    *,
    request: Request,
    session: AsyncSession,
    settings: Settings,
    principal: Principal,
    key: str | None,
    body: dict[str, Any],
    action: Callable[[], Awaitable[dict[str, Any]]],
) -> JSONResponse:
    """Run ``action`` at most once per ``Idempotency-Key`` (see :mod:`services.idempotency`)."""
    validated = idempotency.validate_key(key) if key is not None else None
    path = request.url.path
    if validated is not None:
        stored = await idempotency.claim(
            session,
            tenant_id=principal.tenant_id,
            key=validated,
            method=request.method,
            path=path,
            request_fingerprint=idempotency.fingerprint(request.method, path, canonical_json(body)),
            ttl_s=settings.idempotency_key_ttl_s,
            lock_timeout_ms=settings.idempotency_lock_timeout_ms,
        )
        if stored is not None:
            return JSONResponse(
                stored.body,
                status_code=stored.status_code,
                headers={idempotency.REPLAYED_HEADER: "true"},
            )
    result = await action()
    if validated is not None:
        await idempotency.complete(
            session,
            tenant_id=principal.tenant_id,
            key=validated,
            response=idempotency.StoredResponse(status_code=status.HTTP_202_ACCEPTED, body=result),
        )
    # Wake delivery workers: replay put rows back to pending. Harmless no-op when Redis is off.
    notify_workers(request)
    return JSONResponse(result, status_code=status.HTTP_202_ACCEPTED)


@router.post(
    "/deliveries/{delivery_id}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=DeliveryOut,
    summary="Replay one finished delivery (succeeded, failed or dead)",
    responses={202: {"headers": _REPLAY_HEADERS}, **error_responses(400, 404, 409, 422, 429)},
)
async def replay_delivery(
    *,
    delivery_id: uuid.UUID,
    request: Request,
    principal: PublisherDep,
    session: SessionDep,
    settings: SettingsDep,
    idempotency_key: IdempotencyKeyHeader = None,
) -> JSONResponse:
    async def action() -> dict[str, Any]:
        result = await service.replay_delivery(
            session,
            tenant_id=principal.tenant_id,
            api_key_id=principal.api_key_id,
            delivery_id=delivery_id,
            limits=_limits(settings),
        )
        [delivery] = result.deliveries
        return DeliveryOut.from_model(delivery).model_dump(mode="json")

    return await _idempotent(
        request=request,
        session=session,
        settings=settings,
        principal=principal,
        key=idempotency_key,
        body={},
        action=action,
    )


@router.post(
    "/events/{event_id}/replay",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReplayOut,
    summary="Replay an event's finished deliveries (default: failed and dead)",
    responses={202: {"headers": _REPLAY_HEADERS}, **error_responses(400, 404, 409, 422, 429)},
)
async def replay_event(
    *,
    event_id: uuid.UUID,
    request: Request,
    principal: PublisherDep,
    session: SessionDep,
    settings: SettingsDep,
    body: Annotated[EventReplay | None, Body()] = None,
    idempotency_key: IdempotencyKeyHeader = None,
) -> JSONResponse:
    options = body or EventReplay()

    async def action() -> dict[str, Any]:
        result = await service.replay_event(
            session,
            tenant_id=principal.tenant_id,
            api_key_id=principal.api_key_id,
            event_id=event_id,
            statuses=options.statuses,
            endpoint_id=options.endpoint_id,
            limits=_limits(settings),
        )
        return ReplayOut.from_model(result.request).model_dump(mode="json")

    return await _idempotent(
        request=request,
        session=session,
        settings=settings,
        principal=principal,
        key=idempotency_key,
        body=options.model_dump(mode="json"),
        action=action,
    )


@router.post(
    "/replays",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=ReplayOut,
    summary="Bulk replay by time window and filters (admin)",
    responses={202: {"headers": _REPLAY_HEADERS}, **error_responses(400, 409, 422, 429)},
)
async def replay_filtered(
    *,
    body: BulkReplay,
    request: Request,
    principal: AdminDep,
    session: SessionDep,
    settings: SettingsDep,
    idempotency_key: IdempotencyKeyHeader = None,
) -> JSONResponse:
    async def action() -> dict[str, Any]:
        result = await service.replay_filtered(
            session,
            tenant_id=principal.tenant_id,
            api_key_id=principal.api_key_id,
            flt=service.ReplayFilter(
                statuses=body.statuses,
                created_after=body.created_after,
                created_before=body.created_before,
                endpoint_id=body.endpoint_id,
                event_type=body.event_type,
            ),
            limits=_limits(settings),
        )
        return ReplayOut.from_model(result.request).model_dump(mode="json")

    return await _idempotent(
        request=request,
        session=session,
        settings=settings,
        principal=principal,
        key=idempotency_key,
        body=body.model_dump(mode="json"),
        action=action,
    )


@router.get(
    "/replays",
    response_model=PageResponse[ReplayOut],
    summary="List replay requests (audit trail, newest first)",
)
async def list_replays(
    principal: ReaderDep,
    session: SessionDep,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[ReplayOut]:
    page = await service.list_replays(session, principal.tenant_id, cursor=cursor, limit=limit)
    return PageResponse[ReplayOut](
        data=[ReplayOut.from_model(r) for r in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )
