"""Endpoint CRUD. Reads need ``reader``; writes and the signing secret need ``admin``."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Request, Response, status

from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.endpoints import (
    CircuitOut,
    EndpointCreate,
    EndpointCreated,
    EndpointHealthOut,
    EndpointOut,
    EndpointSecret,
    EndpointUpdate,
    SecretRotate,
    SecretRotated,
)
from hookrelay.api.deps import (
    AdminDep,
    EndpointPolicyDep,
    ReaderDep,
    SecretBoxDep,
    SettingsDep,
    invalidate_subscription_cache,
)
from hookrelay.api.etag import IfMatchHeader, parse_if_match, set_etag
from hookrelay.db.models import ApiKeyRole
from hookrelay.db.session import SessionDep
from hookrelay.services import endpoint_health, endpoint_secrets
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
        rate_limit_per_s=body.rate_limit_per_s,
        max_in_flight=body.max_in_flight,
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
    summary="Update an endpoint (requires If-Match); re-enabling resets its circuit breaker",
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
    request: Request,
    if_match: IfMatchHeader = None,
) -> EndpointOut:
    expected = parse_if_match(if_match, required=True)
    changes = body.changes()
    endpoint = await service.update_endpoint(
        session,
        principal.tenant_id,
        endpoint_id,
        policy=policy,
        expected_version=expected,
        changes=changes,
    )
    # enabled/disabled changes fan-out membership; invalidate the whole tenant.
    if "enabled" in changes:
        await invalidate_subscription_cache(request, principal.tenant_id)
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
    request: Request,
    if_match: IfMatchHeader = None,
) -> Response:
    expected = parse_if_match(if_match, required=False)
    await service.delete_endpoint(
        session, principal.tenant_id, endpoint_id, expected_version=expected
    )
    await invalidate_subscription_cache(request, principal.tenant_id)
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


@router.get(
    "/{endpoint_id}/circuit",
    response_model=CircuitOut,
    summary="Circuit-breaker state, in-flight load and limits of an endpoint",
    responses=error_responses(404),
)
async def get_endpoint_circuit(
    endpoint_id: uuid.UUID, principal: ReaderDep, session: SessionDep, settings: SettingsDep
) -> CircuitOut:
    status_ = await service.get_circuit(
        session,
        principal.tenant_id,
        endpoint_id,
        default_max_in_flight=settings.endpoint_default_max_in_flight,
    )
    return CircuitOut.from_status(status_)


@router.post(
    "/{endpoint_id}/secret/rotate",
    response_model=SecretRotated,
    summary="Rotate the signing secret (requires If-Match); the old one keeps signing during "
    "the overlap window",
    responses=error_responses(404, 412, 422, 428),
)
async def rotate_endpoint_secret(
    *,
    endpoint_id: uuid.UUID,
    principal: AdminDep,
    session: SessionDep,
    box: SecretBoxDep,
    settings: SettingsDep,
    response: Response,
    body: SecretRotate | None = None,
    if_match: IfMatchHeader = None,
) -> SecretRotated:
    expected = parse_if_match(if_match, required=True)
    request = body or SecretRotate()
    rotated = await endpoint_secrets.rotate_signing_secret(
        session,
        principal.tenant_id,
        endpoint_id,
        box=box,
        policy=endpoint_secrets.SecretRotationPolicy.from_settings(settings),
        expected_version=expected,
        overlap_s=request.overlap_s,
        secret=request.secret,
    )
    endpoint = rotated.endpoint
    set_etag(response, endpoint.version)
    response.headers["Cache-Control"] = "no-store"
    return SecretRotated(
        endpoint_id=endpoint.id,
        signing_secret=rotated.signing_secret,
        rotated_at=endpoint.secret_rotated_at or endpoint.updated_at,
        previous_secret_expires_at=endpoint.previous_secret_expires_at,
        version=endpoint.version,
    )


@router.delete(
    "/{endpoint_id}/secret/previous",
    response_model=EndpointOut,
    summary="Stop signing with the previous secret now (idempotent; If-Match optional)",
    responses=error_responses(404, 412),
)
async def end_endpoint_secret_overlap(
    endpoint_id: uuid.UUID,
    principal: AdminDep,
    session: SessionDep,
    response: Response,
    if_match: IfMatchHeader = None,
) -> EndpointOut:
    endpoint = await endpoint_secrets.end_secret_overlap(
        session,
        principal.tenant_id,
        endpoint_id,
        expected_version=parse_if_match(if_match, required=False),
    )
    set_etag(response, endpoint.version)
    return EndpointOut.from_model(endpoint, reveal_headers=True)


@router.get(
    "/{endpoint_id}/health",
    response_model=EndpointHealthOut,
    summary="Delivery health: success rate, latency percentiles, errors, breaker, backlog",
    responses=error_responses(404, 422),
)
async def get_endpoint_health(
    endpoint_id: uuid.UUID,
    principal: ReaderDep,
    session: SessionDep,
    settings: SettingsDep,
    window_s: Annotated[
        int | None,
        Query(
            ge=60,
            description="Look-back window in seconds. Default "
            "ENDPOINT_HEALTH_DEFAULT_WINDOW_S, at most ENDPOINT_HEALTH_MAX_WINDOW_S.",
        ),
    ] = None,
) -> EndpointHealthOut:
    health = await endpoint_health.get_endpoint_health(
        session,
        principal.tenant_id,
        endpoint_id,
        policy=endpoint_health.HealthPolicy.from_settings(settings),
        window_s=window_s,
    )
    return EndpointHealthOut.from_health(health)
