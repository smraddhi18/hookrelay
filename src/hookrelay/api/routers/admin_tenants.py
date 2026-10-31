"""Operator API: tenant lifecycle and bootstrap API keys. Guarded by ``require_operator``."""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, Response, status

from hookrelay.api.contracts.api_keys import ApiKeyCreate, ApiKeyCreated, ApiKeyOut
from hookrelay.api.contracts.common import (
    DEFAULT_LIMIT,
    CursorQuery,
    LimitQuery,
    PageResponse,
    error_responses,
)
from hookrelay.api.contracts.tenants import TenantCreate, TenantOut
from hookrelay.api.deps import require_operator
from hookrelay.db.models import ApiKeyRole, TenantStatus
from hookrelay.db.session import SessionDep
from hookrelay.services import api_keys as api_key_service
from hookrelay.services import tenants as tenant_service

router = APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[Depends(require_operator)],
    responses=error_responses(401, 403),
)


@router.post(
    "/tenants",
    status_code=status.HTTP_201_CREATED,
    response_model=TenantOut,
    summary="Create a tenant",
    responses=error_responses(409),
)
async def create_tenant(body: TenantCreate, session: SessionDep, response: Response) -> TenantOut:
    tenant = await tenant_service.create_tenant(session, slug=body.slug, name=body.name)
    response.headers["Location"] = f"/admin/tenants/{tenant.id}"
    return TenantOut.model_validate(tenant)


@router.get("/tenants", response_model=PageResponse[TenantOut], summary="List tenants")
async def list_tenants(
    session: SessionDep,
    status_filter: Annotated[TenantStatus | None, Query(alias="status")] = None,
    cursor: CursorQuery = None,
    limit: LimitQuery = DEFAULT_LIMIT,
) -> PageResponse[TenantOut]:
    page = await tenant_service.list_tenants(
        session, status=status_filter, cursor=cursor, limit=limit
    )
    return PageResponse[TenantOut](
        data=[TenantOut.model_validate(t) for t in page.items],
        next_cursor=page.next_cursor,
        has_more=page.has_more,
    )


@router.get(
    "/tenants/{tenant_id}",
    response_model=TenantOut,
    summary="Get a tenant",
    responses=error_responses(404),
)
async def get_tenant(tenant_id: uuid.UUID, session: SessionDep) -> TenantOut:
    return TenantOut.model_validate(await tenant_service.get_tenant(session, tenant_id))


@router.post(
    "/tenants/{tenant_id}/suspend",
    response_model=TenantOut,
    summary="Suspend a tenant (all its API keys get 403 tenant_suspended; idempotent)",
    responses=error_responses(404),
)
async def suspend_tenant(tenant_id: uuid.UUID, session: SessionDep) -> TenantOut:
    tenant = await tenant_service.set_tenant_status(session, tenant_id, TenantStatus.SUSPENDED)
    return TenantOut.model_validate(tenant)


@router.post(
    "/tenants/{tenant_id}/resume",
    response_model=TenantOut,
    summary="Reactivate a suspended tenant (idempotent)",
    responses=error_responses(404),
)
async def resume_tenant(tenant_id: uuid.UUID, session: SessionDep) -> TenantOut:
    tenant = await tenant_service.set_tenant_status(session, tenant_id, TenantStatus.ACTIVE)
    return TenantOut.model_validate(tenant)


@router.post(
    "/tenants/{tenant_id}/api-keys",
    status_code=status.HTTP_201_CREATED,
    response_model=ApiKeyCreated,
    summary="Issue an API key for a tenant (bootstrap the first admin key)",
    description="The plaintext key is returned once and never stored.",
    responses=error_responses(404, 422),
)
async def issue_tenant_api_key(
    tenant_id: uuid.UUID, body: ApiKeyCreate, session: SessionDep, response: Response
) -> ApiKeyCreated:
    await tenant_service.get_tenant(session, tenant_id)
    issued = await api_key_service.create_api_key(
        session,
        tenant_id=tenant_id,
        name=body.name,
        role=ApiKeyRole(body.role),
        expires_at=body.expires_at,
    )
    response.headers["Cache-Control"] = "no-store"
    return ApiKeyCreated(**ApiKeyOut.from_model(issued.key).model_dump(), key=issued.plaintext)
