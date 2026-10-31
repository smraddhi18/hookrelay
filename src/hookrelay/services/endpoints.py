"""Endpoint CRUD with tenant scoping, optimistic concurrency and an encrypted signing secret.

Every statement filters on ``tenant_id`` explicitly, so another tenant's endpoint id behaves
exactly like a non-existent one (404). Updates are a single conditional
``UPDATE ... WHERE version = :expected RETURNING *``: no read-modify-write race window, and a
stale ``If-Match`` is detected atomically by the database.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import structlog
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings
from hookrelay.core.errors import (
    AppError,
    ConflictError,
    NotFoundError,
    PreconditionFailedError,
    UnprocessableEntityError,
)
from hookrelay.core.security import SecretBox, SecretDecryptionError, generate_signing_secret
from hookrelay.db.integrity import violated_constraint
from hookrelay.db.models import Endpoint, Subscription
from hookrelay.services.endpoint_rules import UrlPolicy, validate_endpoint_url
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.endpoints")


@dataclass(frozen=True, slots=True)
class EndpointPolicy:
    url: UrlPolicy
    default_timeout_ms: int
    max_timeout_ms: int

    @classmethod
    def from_settings(cls, settings: Settings) -> EndpointPolicy:
        return cls(
            url=UrlPolicy(
                allow_http=settings.endpoint_allow_http,
                allow_private_networks=settings.endpoint_allow_private_networks,
            ),
            default_timeout_ms=settings.endpoint_default_timeout_ms,
            max_timeout_ms=settings.endpoint_max_timeout_ms,
        )

    def check_url(self, url: str) -> str:
        try:
            return validate_endpoint_url(url, self.url)
        except ValueError as exc:
            raise UnprocessableEntityError(
                str(exc), code="invalid_endpoint_url", details=[{"loc": ["body", "url"]}]
            ) from exc

    def check_timeout(self, timeout_ms: int | None) -> int:
        if timeout_ms is None:
            return self.default_timeout_ms
        if timeout_ms > self.max_timeout_ms:
            raise UnprocessableEntityError(
                f"timeout_ms must be at most {self.max_timeout_ms} on this deployment",
                code="timeout_too_large",
                details=[{"loc": ["body", "timeout_ms"]}],
            )
        return timeout_ms


@dataclass(frozen=True, slots=True)
class CreatedEndpoint:
    endpoint: Endpoint
    signing_secret: str


def secret_aad(tenant_id: uuid.UUID, endpoint_id: uuid.UUID) -> bytes:
    return f"endpoint-secret:{tenant_id}:{endpoint_id}".encode()


def _url_conflict(exc: IntegrityError, url: str) -> None:
    if violated_constraint(exc) == "uq_endpoints_tenant_url_live":
        raise ConflictError(
            "An endpoint with this URL already exists",
            code="endpoint_url_taken",
            details={"url": url},
        ) from exc


async def create_endpoint(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    policy: EndpointPolicy,
    box: SecretBox,
    url: str,
    description: str | None,
    enabled: bool,
    headers: dict[str, str],
    timeout_ms: int | None,
) -> CreatedEndpoint:
    normalized = policy.check_url(url)
    endpoint_id = uuid.uuid4()  # chosen up front: it is part of the secret's associated data
    secret = generate_signing_secret()
    endpoint = Endpoint(
        id=endpoint_id,
        tenant_id=tenant_id,
        url=normalized,
        description=description,
        enabled=enabled,
        headers=headers,
        timeout_ms=policy.check_timeout(timeout_ms),
        secret_ciphertext=box.encrypt(secret, aad=secret_aad(tenant_id, endpoint_id)),
    )
    session.add(endpoint)
    try:
        await session.flush()
    except IntegrityError as exc:
        _url_conflict(exc, normalized)
        raise
    logger.info("endpoint.created", tenant_id=str(tenant_id), endpoint_id=str(endpoint_id))
    return CreatedEndpoint(endpoint=endpoint, signing_secret=secret)


def _live(tenant_id: uuid.UUID, endpoint_id: uuid.UUID) -> list[Any]:
    return [
        Endpoint.tenant_id == tenant_id,
        Endpoint.id == endpoint_id,
        Endpoint.deleted_at.is_(None),
    ]


async def get_endpoint(
    session: AsyncSession, tenant_id: uuid.UUID, endpoint_id: uuid.UUID
) -> Endpoint:
    endpoint = await session.scalar(select(Endpoint).where(*_live(tenant_id, endpoint_id)))
    if endpoint is None:
        raise NotFoundError("Endpoint not found", code="endpoint_not_found")
    return endpoint


async def list_endpoints(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    enabled: bool | None,
    cursor: str | None,
    limit: int,
) -> Page[Endpoint]:
    stmt = select(Endpoint).where(Endpoint.tenant_id == tenant_id, Endpoint.deleted_at.is_(None))
    if enabled is not None:
        stmt = stmt.where(Endpoint.enabled == enabled)
    return await paginate(
        session,
        stmt,
        created_col=Endpoint.created_at,
        id_col=Endpoint.id,
        cursor=cursor,
        limit=limit,
    )


async def _version_mismatch(
    session: AsyncSession, tenant_id: uuid.UUID, endpoint_id: uuid.UUID
) -> PreconditionFailedError:
    """Called when a conditional write touched 0 rows: tell 404 apart from 412."""
    current = await get_endpoint(session, tenant_id, endpoint_id)  # raises 404 if gone
    return PreconditionFailedError(
        "Endpoint was modified by another request; re-fetch and retry",
        details={"current_version": current.version},
        headers={"ETag": f'"{current.version}"'},
    )


async def update_endpoint(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    *,
    policy: EndpointPolicy,
    expected_version: int | None,
    changes: dict[str, Any],
) -> Endpoint:
    """Apply a partial update. ``expected_version=None`` means ``If-Match: *``."""
    values = dict(changes)
    if "url" in values:
        values["url"] = policy.check_url(values["url"])
    if "timeout_ms" in values:
        values["timeout_ms"] = policy.check_timeout(values["timeout_ms"])
    conditions = _live(tenant_id, endpoint_id)
    if expected_version is not None:
        conditions.append(Endpoint.version == expected_version)
    stmt = (
        update(Endpoint)
        .where(*conditions)
        .values(**values, version=Endpoint.version + 1)
        .returning(Endpoint)
        .execution_options(populate_existing=True)
    )
    try:
        endpoint = await session.scalar(stmt)
    except IntegrityError as exc:
        _url_conflict(exc, values.get("url", ""))
        raise
    if endpoint is None:
        raise await _version_mismatch(session, tenant_id, endpoint_id)
    logger.info(
        "endpoint.updated",
        tenant_id=str(tenant_id),
        endpoint_id=str(endpoint_id),
        fields=sorted(changes),
        version=endpoint.version,
    )
    return endpoint


async def delete_endpoint(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    *,
    expected_version: int | None,
) -> None:
    """Soft delete: keeps the row for delivery history (M2+), frees the URL, drops links."""
    conditions = _live(tenant_id, endpoint_id)
    if expected_version is not None:
        conditions.append(Endpoint.version == expected_version)
    deleted_id = await session.scalar(
        update(Endpoint)
        .where(*conditions)
        .values(deleted_at=func.now(), enabled=False, version=Endpoint.version + 1)
        .returning(Endpoint.id)
        .execution_options(synchronize_session=False)
    )
    if deleted_id is None:
        raise await _version_mismatch(session, tenant_id, endpoint_id)
    await session.execute(
        delete(Subscription).where(
            Subscription.tenant_id == tenant_id, Subscription.endpoint_id == endpoint_id
        )
    )
    logger.info("endpoint.deleted", tenant_id=str(tenant_id), endpoint_id=str(endpoint_id))


async def reveal_signing_secret(
    session: AsyncSession, tenant_id: uuid.UUID, endpoint_id: uuid.UUID, *, box: SecretBox
) -> str:
    endpoint = await get_endpoint(session, tenant_id, endpoint_id)
    try:
        secret = box.decrypt(endpoint.secret_ciphertext, aad=secret_aad(tenant_id, endpoint_id))
    except SecretDecryptionError as exc:
        # Usually a retired SECRETS_ENCRYPTION_KEY: an operator problem, never the caller's.
        logger.error(  # noqa: TRY400 - the traceback adds nothing; no secret material either
            "endpoint.secret_undecryptable", tenant_id=str(tenant_id), endpoint_id=str(endpoint_id)
        )
        raise AppError(
            "Signing secret is unavailable; contact the operator", code="secret_unavailable"
        ) from exc
    logger.info("endpoint.secret_revealed", tenant_id=str(tenant_id), endpoint_id=str(endpoint_id))
    return secret
