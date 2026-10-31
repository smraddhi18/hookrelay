"""Tenant lifecycle (operator-only): create, read, list, suspend / resume."""

from __future__ import annotations

import uuid

import structlog
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import ConflictError, NotFoundError
from hookrelay.db.integrity import violated_constraint
from hookrelay.db.models import Tenant, TenantStatus
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.tenants")


async def create_tenant(session: AsyncSession, *, slug: str, name: str) -> Tenant:
    tenant = Tenant(slug=slug, name=name)
    session.add(tenant)
    try:
        await session.flush()
    except IntegrityError as exc:
        if violated_constraint(exc) == "uq_tenants_slug":
            raise ConflictError(
                f"Tenant slug {slug!r} is already taken", code="tenant_slug_taken"
            ) from exc
        raise
    logger.info("tenant.created", tenant_id=str(tenant.id), slug=slug)
    return tenant


async def get_tenant(session: AsyncSession, tenant_id: uuid.UUID) -> Tenant:
    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise NotFoundError("Tenant not found", code="tenant_not_found")
    return tenant


async def list_tenants(
    session: AsyncSession,
    *,
    status: TenantStatus | None,
    cursor: str | None,
    limit: int,
) -> Page[Tenant]:
    stmt = select(Tenant)
    if status is not None:
        stmt = stmt.where(Tenant.status == status.value)
    return await paginate(
        session, stmt, created_col=Tenant.created_at, id_col=Tenant.id, cursor=cursor, limit=limit
    )


async def set_tenant_status(
    session: AsyncSession, tenant_id: uuid.UUID, status: TenantStatus
) -> Tenant:
    """Idempotent: suspending a suspended tenant is a no-op that still returns 200."""
    tenant = await session.get(Tenant, tenant_id, with_for_update=True)
    if tenant is None:
        raise NotFoundError("Tenant not found", code="tenant_not_found")
    if tenant.status != status.value:
        previous = tenant.status
        tenant.status = status.value
        await session.flush()
        logger.info(
            "tenant.status_changed", tenant_id=str(tenant.id), previous=previous, status=status
        )
    return tenant
