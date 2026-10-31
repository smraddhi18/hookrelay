"""Tenant API keys: issue, list, rotate (with an overlap window), revoke, authenticate.

Authentication flow (one indexed lookup per request):

1. Reject anything that is not shaped like ``hrk_<12-char prefix>_<43-char secret>``.
2. ``SELECT ... FROM api_keys JOIN tenants WHERE prefix = $1`` (unique index).
3. Compare ``SHA-256(presented)`` with the stored digest in constant time. When the prefix is
   unknown a dummy digest is compared instead, so both failure modes cost the same.
4. Check revocation / expiry against the database clock, then the tenant's status.
"""

from __future__ import annotations

import hmac
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

import structlog
from sqlalchemy import exists, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    UnauthorizedError,
    UnprocessableEntityError,
)
from hookrelay.core.security import (
    DUMMY_API_KEY_DIGEST,
    api_key_matches,
    generate_api_key,
    hash_api_key,
    parse_api_key_prefix,
)
from hookrelay.db.integrity import violated_constraint
from hookrelay.db.models import ApiKey, ApiKeyRole, ApiKeyStatus, Tenant, TenantStatus
from hookrelay.services.clock import db_now
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.api_keys")

BEARER_CHALLENGE = {"WWW-Authenticate": 'Bearer realm="hookrelay"'}
_INVALID_KEY_MESSAGE = "Invalid or missing API key"


@dataclass(frozen=True, slots=True)
class IssuedApiKey:
    key: ApiKey
    plaintext: str  # the only time the secret exists outside the caller's hands


@dataclass(frozen=True, slots=True)
class Principal:
    """Who is calling: resolved once per request and passed to every tenant-scoped service."""

    tenant_id: uuid.UUID
    tenant_slug: str
    api_key_id: uuid.UUID
    api_key_name: str
    api_key_prefix: str
    role: ApiKeyRole

    def can(self, required: ApiKeyRole) -> bool:
        return self.role.satisfies(required)


def _unauthorized(
    message: str = _INVALID_KEY_MESSAGE, code: str = "unauthorized"
) -> UnauthorizedError:
    return UnauthorizedError(message, code=code, headers=BEARER_CHALLENGE)


async def issue_api_key(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    name: str,
    role: ApiKeyRole,
    expires_at: datetime | None = None,
    rotated_from_id: uuid.UUID | None = None,
) -> IssuedApiKey:
    for _attempt in range(3):  # a 12-char base36 prefix collision is ~1 in 4.7e18; retry anyway
        generated = generate_api_key()
        key = ApiKey(
            tenant_id=tenant_id,
            name=name,
            role=role.value,
            prefix=generated.prefix,
            key_hash=generated.digest,
            expires_at=expires_at,
            rotated_from_id=rotated_from_id,
        )
        try:
            async with session.begin_nested():
                session.add(key)
                await session.flush()
        except IntegrityError as exc:
            constraint = violated_constraint(exc)
            if constraint == "uq_api_keys_prefix":
                continue
            if constraint == "uq_api_keys_rotated_from_id":
                raise ConflictError(
                    "API key has already been rotated", code="api_key_already_rotated"
                ) from exc
            raise
        logger.info(
            "api_key.issued",
            tenant_id=str(tenant_id),
            api_key_id=str(key.id),
            prefix=key.prefix,
            role=role.value,
            rotated_from_id=str(rotated_from_id) if rotated_from_id else None,
        )
        return IssuedApiKey(key=key, plaintext=generated.plaintext)
    raise ConflictError("Could not allocate a unique key prefix", code="api_key_prefix_collision")


async def create_api_key(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    name: str,
    role: ApiKeyRole,
    expires_at: datetime | None,
) -> IssuedApiKey:
    if expires_at is not None and expires_at <= await db_now(session):
        raise UnprocessableEntityError(
            "expires_at must be in the future", details=[{"loc": ["body", "expires_at"]}]
        )
    return await issue_api_key(
        session, tenant_id=tenant_id, name=name, role=role, expires_at=expires_at
    )


async def list_api_keys(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    include_inactive: bool,
    cursor: str | None,
    limit: int,
) -> Page[ApiKey]:
    stmt = select(ApiKey).where(ApiKey.tenant_id == tenant_id)
    if not include_inactive:
        now = await db_now(session)
        stmt = stmt.where(
            ApiKey.revoked_at.is_(None),
            or_(ApiKey.expires_at.is_(None), ApiKey.expires_at > now),
        )
    return await paginate(
        session, stmt, created_col=ApiKey.created_at, id_col=ApiKey.id, cursor=cursor, limit=limit
    )


async def get_api_key(
    session: AsyncSession, tenant_id: uuid.UUID, key_id: uuid.UUID, *, for_update: bool = False
) -> ApiKey:
    stmt = select(ApiKey).where(ApiKey.tenant_id == tenant_id, ApiKey.id == key_id)
    if for_update:
        stmt = stmt.with_for_update()
    key = await session.scalar(stmt)
    if key is None:
        # 404 (not 403) for other tenants' ids: existence is not disclosed across tenants.
        raise NotFoundError("API key not found", code="api_key_not_found")
    return key


async def rotate_api_key(
    session: AsyncSession, tenant_id: uuid.UUID, key_id: uuid.UUID, *, overlap_s: int
) -> IssuedApiKey:
    """Mint a successor with the same name/role; the old key keeps working for ``overlap_s``.

    The overlap lets clients roll the new secret out without downtime. The old key's expiry
    only ever moves *earlier* (an already-sooner expiry is kept).
    """
    old = await get_api_key(session, tenant_id, key_id, for_update=True)
    now = await db_now(session)
    if old.status_at(now) is not ApiKeyStatus.ACTIVE:
        raise ConflictError("Only active API keys can be rotated", code="api_key_inactive")
    has_successor = await session.scalar(select(exists().where(ApiKey.rotated_from_id == old.id)))
    if has_successor:
        raise ConflictError("API key has already been rotated", code="api_key_already_rotated")

    cutoff = now + timedelta(seconds=overlap_s)
    if old.expires_at is None or old.expires_at > cutoff:
        old.expires_at = max(cutoff, old.created_at + timedelta(microseconds=1))
    issued = await issue_api_key(
        session,
        tenant_id=tenant_id,
        name=old.name,
        role=ApiKeyRole(old.role),
        rotated_from_id=old.id,
    )
    logger.info(
        "api_key.rotated",
        tenant_id=str(tenant_id),
        api_key_id=str(old.id),
        successor_id=str(issued.key.id),
        overlap_s=overlap_s,
    )
    return issued


async def revoke_api_key(session: AsyncSession, tenant_id: uuid.UUID, key_id: uuid.UUID) -> ApiKey:
    """Idempotent: revoking twice keeps the first ``revoked_at``."""
    key = await get_api_key(session, tenant_id, key_id, for_update=True)
    if key.revoked_at is None:
        key.revoked_at = await db_now(session)
        await session.flush()
        logger.info("api_key.revoked", tenant_id=str(tenant_id), api_key_id=str(key.id))
    return key


def require_well_formed(presented: str) -> str:
    """Return the key's lookup prefix; malformed keys are rejected before any database work."""
    prefix = parse_api_key_prefix(presented)
    if prefix is None:
        logger.info("auth.api_key.rejected", reason="malformed")
        raise _unauthorized()
    return prefix


async def authenticate(
    session: AsyncSession, presented: str, *, last_used_resolution_s: int
) -> Principal:
    prefix = require_well_formed(presented)

    digest = hash_api_key(presented)
    row = (
        await session.execute(
            select(ApiKey, Tenant)
            .join(Tenant, Tenant.id == ApiKey.tenant_id)
            .where(ApiKey.prefix == prefix)
        )
    ).one_or_none()
    if row is None:
        hmac.compare_digest(digest, DUMMY_API_KEY_DIGEST)  # equalise timing with a wrong secret
        logger.info("auth.api_key.rejected", reason="unknown_prefix", prefix=prefix)
        raise _unauthorized()
    key, tenant = row
    if not api_key_matches(presented, key.key_hash):
        logger.info("auth.api_key.rejected", reason="hash_mismatch", api_key_id=str(key.id))
        raise _unauthorized()

    # The caller proved possession of the secret, so precise reasons disclose nothing new.
    now = await db_now(session)
    status = key.status_at(now)
    if status is ApiKeyStatus.REVOKED:
        logger.info("auth.api_key.rejected", reason="revoked", api_key_id=str(key.id))
        raise _unauthorized("API key has been revoked", code="api_key_revoked")
    if status is ApiKeyStatus.EXPIRED:
        logger.info("auth.api_key.rejected", reason="expired", api_key_id=str(key.id))
        raise _unauthorized("API key has expired", code="api_key_expired")
    if tenant.status != TenantStatus.ACTIVE.value:
        logger.info("auth.api_key.rejected", reason="tenant_suspended", tenant_id=str(tenant.id))
        raise ForbiddenError("Tenant is suspended", code="tenant_suspended")

    await _touch_last_used(session, key, now, last_used_resolution_s)
    return Principal(
        tenant_id=tenant.id,
        tenant_slug=tenant.slug,
        api_key_id=key.id,
        api_key_name=key.name,
        api_key_prefix=key.prefix,
        role=ApiKeyRole(key.role),
    )


async def _touch_last_used(
    session: AsyncSession, key: ApiKey, now: datetime, resolution_s: int
) -> None:
    """Record usage at most once per ``resolution_s`` per key (bounded write amplification).

    The conditional UPDATE makes concurrent requests race safely: only one of them writes.
    """
    threshold = now - timedelta(seconds=resolution_s)
    if key.last_used_at is not None and key.last_used_at > threshold:
        return
    await session.execute(
        update(ApiKey)
        .where(
            ApiKey.id == key.id,
            or_(ApiKey.last_used_at.is_(None), ApiKey.last_used_at <= threshold),
        )
        .values(last_used_at=now)
        .execution_options(synchronize_session=False)
    )
