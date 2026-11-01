"""``Idempotency-Key`` support for unsafe requests (today: ``POST /v1/events``).

Protocol, all inside the request's own transaction:

1. **Claim**: ``INSERT ... ON CONFLICT (tenant_id, key) DO UPDATE ... WHERE expires_at <= now()``.
   A fresh or expired key is (re)claimed by this transaction. Because the row is uncommitted
   until the request finishes, a concurrent request with the same key *blocks* on it inside
   Postgres instead of racing; ``lock_timeout`` bounds that wait and turns it into
   ``409 idempotency_key_in_progress`` + ``Retry-After``.
2. **Replay or reject**: a live key that was not claimed is read back. Same fingerprint
   (SHA-256 of method, path and canonical body): the stored status and body are replayed with
   ``Idempotent-Replayed: true``. Different fingerprint: ``422 idempotency_key_mismatch``.
3. **Complete**: the success response is written to the claimed row before commit, so other
   transactions only ever observe completed records. Errors are not stored: the rollback that
   undoes the request also releases the key, and the client may retry with the same key.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import delete, func, null, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import BadRequestError, ConflictError, UnprocessableEntityError
from hookrelay.db.integrity import LOCK_NOT_AVAILABLE, sqlstate
from hookrelay.db.models import IdempotencyKey
from hookrelay.db.models.event import MAX_IDEMPOTENCY_KEY_LENGTH

logger = structlog.stdlib.get_logger("hookrelay.idempotency")

IDEMPOTENCY_HEADER = "Idempotency-Key"
REPLAYED_HEADER = "Idempotent-Replayed"
RETRY_AFTER_S = 1
_KEY_RE = re.compile(rf"^[\x21-\x7e]{{1,{MAX_IDEMPOTENCY_KEY_LENGTH}}}$")


@dataclass(frozen=True, slots=True)
class StoredResponse:
    status_code: int
    body: dict[str, Any]


def validate_key(raw: str) -> str:
    """Keys are 1 to 255 printable ASCII characters (UUIDs, ULIDs, ``order-42-attempt``)."""
    if not _KEY_RE.fullmatch(raw):
        raise BadRequestError(
            f"{IDEMPOTENCY_HEADER} must be 1 to {MAX_IDEMPOTENCY_KEY_LENGTH} printable ASCII "
            "characters without spaces",
            code="invalid_idempotency_key",
        )
    return raw


def fingerprint(method: str, path: str, canonical_body: bytes) -> bytes:
    digest = hashlib.sha256()
    digest.update(method.upper().encode("ascii"))
    digest.update(b"\n")
    digest.update(path.encode("utf-8"))
    digest.update(b"\n")
    digest.update(canonical_body)
    return digest.digest()


def _in_progress() -> ConflictError:
    return ConflictError(
        f"A request with this {IDEMPOTENCY_HEADER} is still being processed; retry shortly",
        code="idempotency_key_in_progress",
        headers={"Retry-After": str(RETRY_AFTER_S)},
    )


async def claim(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    key: str,
    method: str,
    path: str,
    request_fingerprint: bytes,
    ttl_s: int,
    lock_timeout_ms: int,
) -> StoredResponse | None:
    """Claim ``key`` for this transaction (returns ``None``) or return the response to replay."""
    # SET LOCAL cannot take bind parameters; the value is a validated int from Settings.
    await session.execute(text(f"SET LOCAL lock_timeout = '{int(lock_timeout_ms)}ms'"))
    insert_stmt = pg_insert(IdempotencyKey).values(
        tenant_id=tenant_id,
        key=key,
        request_method=method,
        request_path=path,
        fingerprint=request_fingerprint,
        expires_at=func.now() + timedelta(seconds=ttl_s),
    )
    stmt = insert_stmt.on_conflict_do_update(
        constraint="pk_idempotency_keys",
        set_={
            "request_method": insert_stmt.excluded.request_method,
            "request_path": insert_stmt.excluded.request_path,
            "fingerprint": insert_stmt.excluded.fingerprint,
            # SQL NULL explicitly: a bare None would be stored as the JSON literal 'null'.
            "event_id": null(),
            "response_status": null(),
            "response_body": null(),
            "created_at": func.now(),
            "expires_at": insert_stmt.excluded.expires_at,
        },
        # Only an expired record may be taken over; a live one is replayed or rejected.
        where=IdempotencyKey.expires_at <= func.now(),
    ).returning(IdempotencyKey.key)
    try:
        claimed = await session.scalar(stmt)
    except DBAPIError as exc:
        if sqlstate(exc) == LOCK_NOT_AVAILABLE:
            logger.info("idempotency.in_progress", tenant_id=str(tenant_id))
            raise _in_progress() from exc
        raise
    await session.execute(text("SET LOCAL lock_timeout TO DEFAULT"))
    if claimed is not None:
        return None

    record = await session.scalar(
        select(IdempotencyKey).where(
            IdempotencyKey.tenant_id == tenant_id, IdempotencyKey.key == key
        )
    )
    if record is None or record.response_status is None or record.response_body is None:
        # Not reachable through this module (records are completed before commit); kept as a
        # safe answer should a future writer commit a claim without a response.
        raise _in_progress()
    if (
        record.fingerprint != request_fingerprint
        or record.request_method != method
        or record.request_path != path
    ):
        raise UnprocessableEntityError(
            f"This {IDEMPOTENCY_HEADER} was already used with a different request",
            code="idempotency_key_mismatch",
        )
    logger.info("idempotency.replayed", tenant_id=str(tenant_id), event_id=str(record.event_id))
    return StoredResponse(status_code=record.response_status, body=record.response_body)


async def complete(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    key: str,
    event_id: uuid.UUID,
    response: StoredResponse,
) -> None:
    await session.execute(
        update(IdempotencyKey)
        .where(IdempotencyKey.tenant_id == tenant_id, IdempotencyKey.key == key)
        .values(
            event_id=event_id, response_status=response.status_code, response_body=response.body
        )
        .execution_options(synchronize_session=False)
    )


async def purge_expired(session: AsyncSession, *, batch_size: int) -> int:
    """Delete up to ``batch_size`` expired records (skipping rows a request is reclaiming)."""
    doomed = (
        select(IdempotencyKey.tenant_id, IdempotencyKey.key)
        .where(IdempotencyKey.expires_at <= func.now())
        .order_by(IdempotencyKey.expires_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(
        delete(IdempotencyKey)
        .where(tuple_(IdempotencyKey.tenant_id, IdempotencyKey.key).in_(doomed))
        .returning(IdempotencyKey.key)
        .execution_options(synchronize_session=False)
    )
    return len(result.all())
