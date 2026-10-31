"""Endpoint CRUD. Reads need ``reader``; writes and the signing secret need ``admin``."""

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
from hookrelay.api.contracts.endpoints import (
    EndpointCreate,
    EndpointCreated,
    EndpointOut,
    EndpointSecret,
    EndpointUpdate,
)
from hookrelay.api.deps import AdminDep, EndpointPolicyDep, ReaderDep, SecretBoxDep
from hookrelay.api.etag import IfMatchHeader, parse_if_match, set_etag
from hookrelay.db.models import ApiKeyRole
from hookrelay.db.session import SessionDep
from hookrelay.services import endpoints as service

router = APIRouter(prefix="/endpoints", tags=["endpoints"])


@router.get("", response_model=PageResponse[EndpointOut], summary="List endpoints")
async def list_endpoints(
    principal: ReaderDep,
    session: SessionDep,
    enabled: Annotated[bool | None, Query(description="Filter by enabled flag.")] = None,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[EndpointOut]:
    page = await service.list_endpoints(
        session, principal.tenant_id, enabled=enabled, cursor=cursor, limit=limit
    )
    reveal = principal.can(ApiKeyRole.ADMIN)
    return PageResponse[EndpointOut](
        data=[EndpointOut.from_model(e, reveal_headers=reveal) for e in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=EndpointCreated,
    summary="Create an endpoint (returns its signing secret once)",
    responses=error_responses(409, 422),
)
async def create_endpoint(
    *,
    body: EndpointCreate,
    principal: AdminDep,
    session: SessionDep,
    policy: EndpointPolicyDep,
    box: SecretBoxDep,
    response: Response,
) -> EndpointCreated:
    created = await service.create_endpoint(
        session,
        tenant_id=principal.tenant_id,
        policy=policy,
        box=box,
        url=body.url,
        description=body.description,
        enabled=body.enabled,
        headers=body.headers,
        timeout_ms=body.timeout_ms,
    )
    endpoint = created.endpoint
    response.headers["Location"] = f"/v1/endpoints/{endpoint.id}"
    response.headers["Cache-Control"] = "no-store"
    set_etag(response, endpoint.version)
    return EndpointCreated(
        **EndpointOut.from_model(endpoint, reveal_headers=True).model_dump(),
        signing_secret=created.signing_secret,
    )


@router.get(
    "/{endpoint_id}",
    response_model=EndpointOut,
    summary="Get an endpoint (ETag header = version)",
    responses=error_responses(404),
)
async def get_endpoint(
    endpoint_id: uuid.UUID, principal: ReaderDep, session: SessionDep, response: Response
) -> EndpointOut:
    endpoint = await service.get_endpoint(session, principal.tenant_id, endpoint_id)
    set_etag(response, endpoint.version)
    return EndpointOut.from_model(endpoint, reveal_headers=principal.can(ApiKeyRole.ADMIN))


@router.patch(
    "/{endpoint_id}",
    response_model=EndpointOut,
    summary="Update an endpoint (requires If-Match)",
    responses=error_responses(404, 409, 412, 422, 428),
)
async def update_endpoint(
    *,
    endpoint_id: uuid.UUID,
    body: EndpointUpdate,
    principal: AdminDep,
    session: SessionDep,
    policy: EndpointPolicyDep,
    response: Response,
    if_match: IfMatchHeader = None,
) -> EndpointOut:
    expected = parse_if_match(if_match, required=True)
    endpoint = await service.update_endpoint(
        session,
        principal.tenant_id,
        endpoint_id,
        policy=policy,
        expected_version=expected,
        changes=body.changes(),
    )
    set_etag(response, endpoint.version)
    return EndpointOut.from_model(endpoint, reveal_headers=True)


@router.delete(
    "/{endpoint_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an endpoint and its subscriptions (If-Match optional)",
    responses=error_responses(404, 412),
)
async def delete_endpoint(
    endpoint_id: uuid.UUID,
    principal: AdminDep,
    session: SessionDep,
    if_match: IfMatchHeader = None,
) -> Response:
    expected = parse_if_match(if_match, required=False)
    await service.delete_endpoint(
        session, principal.tenant_id, endpoint_id, expected_version=expected
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/{endpoint_id}/secret",
    response_model=EndpointSecret,
    summary="Reveal the endpoint's signing secret (admin only, audited)",
    responses=error_responses(404),
)
async def get_endpoint_secret(
    endpoint_id: uuid.UUID,
    principal: AdminDep,
    session: SessionDep,
    box: SecretBoxDep,
    response: Response,
) -> EndpointSecret:
    secret = await service.reveal_signing_secret(session, principal.tenant_id, endpoint_id, box=box)
    response.headers["Cache-Control"] = "no-store"
    return EndpointSecret(endpoint_id=endpoint_id, signing_secret=secret)
