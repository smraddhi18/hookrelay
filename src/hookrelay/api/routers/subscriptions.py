"""Subscriptions (nested under endpoints) and the fan-out preview."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Response, status

from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.subscriptions import (
    MatchedEndpoint,
    MatchResponse,
    SubscriptionCreate,
    SubscriptionOut,
)
from hookrelay.api.deps import AdminDep, ReaderDep
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH
from hookrelay.db.session import SessionDep
from hookrelay.services import subscriptions as service

router = APIRouter(tags=["subscriptions"])


@router.get(
    "/endpoints/{endpoint_id}/subscriptions",
    response_model=PageResponse[SubscriptionOut],
    summary="List an endpoint's subscriptions",
    responses=error_responses(404),
)
async def list_subscriptions(
    endpoint_id: uuid.UUID,
    principal: ReaderDep,
    session: SessionDep,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[SubscriptionOut]:
    page = await service.list_subscriptions(
        session, principal.tenant_id, endpoint_id, cursor=cursor, limit=limit
    )
    return PageResponse[SubscriptionOut](
        data=[SubscriptionOut.from_model(s) for s in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.post(
    "/endpoints/{endpoint_id}/subscriptions",
    status_code=status.HTTP_201_CREATED,
    response_model=SubscriptionOut,
    summary="Subscribe an endpoint to an event type or wildcard pattern",
    responses=error_responses(404, 409, 422),
)
async def create_subscription(
    endpoint_id: uuid.UUID,
    body: SubscriptionCreate,
    principal: AdminDep,
    session: SessionDep,
    response: Response,
) -> SubscriptionOut:
    subscription = await service.create_subscription(
        session, tenant_id=principal.tenant_id, endpoint_id=endpoint_id, pattern=body.event_type
    )
    response.headers["Location"] = f"/v1/endpoints/{endpoint_id}/subscriptions/{subscription.id}"
    return SubscriptionOut.from_model(subscription)


@router.delete(
    "/endpoints/{endpoint_id}/subscriptions/{subscription_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a subscription",
    responses=error_responses(404),
)
async def delete_subscription(
    endpoint_id: uuid.UUID,
    subscription_id: uuid.UUID,
    principal: AdminDep,
    session: SessionDep,
) -> Response:
    await service.delete_subscription(session, principal.tenant_id, endpoint_id, subscription_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/subscriptions/match",
    response_model=MatchResponse,
    summary="Preview fan-out: which enabled endpoints would receive this event type",
)
async def match_subscriptions(
    principal: ReaderDep,
    session: SessionDep,
    event_type: Annotated[
        str,
        Query(
            max_length=MAX_EVENT_TYPE_LENGTH,
            pattern=EVENT_TYPE_PATTERN,
            examples=["invoice.paid"],
        ),
    ],
) -> MatchResponse:
    matches = await service.match_endpoints(session, principal.tenant_id, event_type)
    return MatchResponse(
        event_type=event_type,
        endpoints=[
            MatchedEndpoint(id=endpoint.id, url=endpoint.url, matched_patterns=patterns)
            for endpoint, patterns in matches
        ],
    )
