"""Tenant self-service API key management (admin role)."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Query, Response, status

from hookrelay.api.contracts.api_keys import ApiKeyCreate, ApiKeyCreated, ApiKeyOut, RotatedApiKey
from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.deps import AdminDep, SettingsDep
from hookrelay.db.session import SessionDep
from hookrelay.services import api_keys as service

router = APIRouter(prefix="/api-keys", tags=["api-keys"])


def _created(issued: service.IssuedApiKey) -> ApiKeyCreated:
    return ApiKeyCreated(**ApiKeyOut.from_model(issued.key).model_dump(), key=issued.plaintext)


@router.get("", response_model=PageResponse[ApiKeyOut], summary="List API keys")
async def list_api_keys(
    principal: AdminDep,
    session: SessionDep,
    include_inactive: Annotated[
        bool, Query(description="Also return revoked and expired keys.")
    ] = False,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[ApiKeyOut]:
    page = await service.list_api_keys(
        session,
        principal.tenant_id,
        include_inactive=include_inactive,
        cursor=cursor,
        limit=limit,
    )
    return PageResponse[ApiKeyOut](
        data=[ApiKeyOut.from_model(k) for k in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=ApiKeyCreated,
    summary="Create an API key (the secret is shown once)",
    responses=error_responses(422),
)
async def create_api_key(
    body: ApiKeyCreate, principal: AdminDep, session: SessionDep, response: Response
) -> ApiKeyCreated:
    issued = await service.create_api_key(
        session,
        tenant_id=principal.tenant_id,
        name=body.name,
        role=body.role,
        expires_at=body.expires_at,
    )
    response.headers["Location"] = f"/v1/api-keys/{issued.key.id}"
    response.headers["Cache-Control"] = "no-store"
    return _created(issued)


@router.get(
    "/{key_id}", response_model=ApiKeyOut, summary="Get an API key", responses=error_responses(404)
)
async def get_api_key(key_id: uuid.UUID, principal: AdminDep, session: SessionDep) -> ApiKeyOut:
    return ApiKeyOut.from_model(await service.get_api_key(session, principal.tenant_id, key_id))


@router.post(
    "/{key_id}/rotate",
    status_code=status.HTTP_201_CREATED,
    response_model=RotatedApiKey,
    summary="Rotate: mint a successor; the old key keeps working for the overlap window",
    description="The old key's expiry becomes `now + API_KEY_ROTATION_OVERLAP_S` (or stays "
    "earlier). A key can be rotated once; rotate the successor next time.",
    responses=error_responses(404, 409),
)
async def rotate_api_key(
    key_id: uuid.UUID,
    principal: AdminDep,
    session: SessionDep,
    settings: SettingsDep,
    response: Response,
) -> RotatedApiKey:
    issued = await service.rotate_api_key(
        session, principal.tenant_id, key_id, overlap_s=settings.api_key_rotation_overlap_s
    )
    previous = await service.get_api_key(session, principal.tenant_id, key_id)
    response.headers["Location"] = f"/v1/api-keys/{issued.key.id}"
    response.headers["Cache-Control"] = "no-store"
    return RotatedApiKey(new_key=_created(issued), previous_key=ApiKeyOut.from_model(previous))


@router.post(
    "/{key_id}/revoke",
    response_model=ApiKeyOut,
    summary="Revoke an API key immediately (idempotent)",
    responses=error_responses(404),
)
async def revoke_api_key(key_id: uuid.UUID, principal: AdminDep, session: SessionDep) -> ApiKeyOut:
    return ApiKeyOut.from_model(await service.revoke_api_key(session, principal.tenant_id, key_id))
