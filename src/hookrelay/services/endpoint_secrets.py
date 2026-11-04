"""Endpoint signing-secret rotation with a dual-signing overlap window.

Rotating a secret must not break a receiver that has not deployed the new one yet. Standard
Webhooks allows several space-separated ``v1,`` signatures in ``webhook-signature``, so after a
rotation the worker signs every attempt with the **new and the previous** secret until
``previous_secret_expires_at``; a receiver holding either verifies, and can switch at its own
pace. ``overlap_s = 0`` is an immediate cutover (e.g. after a leak).

* One conditional ``UPDATE ... WHERE version = :expected RETURNING``: the current ciphertext
  moves to ``previous_secret_ciphertext`` (still bound to the same tenant+endpoint AAD, so it
  is moved, never decrypted) and the new secret is encrypted in the same statement.
* ``If-Match`` is **required**: a blindly retried rotate would otherwise replace the secret the
  client just received (and was about to deploy) and push the one it still uses out of the
  overlap. ``If-Match: *`` opts out explicitly.
* Rotating during an overlap replaces the previous secret: at most two secrets ever sign.
* Ending the overlap early (``DELETE .../secret/previous``) is idempotent.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import structlog
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings
from hookrelay.core.errors import UnprocessableEntityError
from hookrelay.core.security import SecretBox, generate_signing_secret
from hookrelay.core.signing import SECRET_PREFIX, decode_secret
from hookrelay.db.models import Endpoint
from hookrelay.services.endpoints import _live, _version_mismatch, get_endpoint, secret_aad

logger = structlog.stdlib.get_logger("hookrelay.endpoint_secrets")

MIN_SECRET_BYTES = 24
MAX_SECRET_BYTES = 64


@dataclass(frozen=True, slots=True)
class SecretRotationPolicy:
    default_overlap_s: int
    max_overlap_s: int

    @classmethod
    def from_settings(cls, settings: Settings) -> SecretRotationPolicy:
        return cls(
            default_overlap_s=settings.endpoint_secret_rotation_overlap_s,
            max_overlap_s=settings.endpoint_secret_max_overlap_s,
        )

    def overlap(self, requested: int | None) -> int:
        if requested is None:
            return self.default_overlap_s
        if requested > self.max_overlap_s:
            raise UnprocessableEntityError(
                f"overlap_s must be at most {self.max_overlap_s} on this deployment",
                code="overlap_too_long",
                details=[{"loc": ["body", "overlap_s"]}],
            )
        return requested


@dataclass(frozen=True, slots=True)
class RotatedSecret:
    endpoint: Endpoint
    signing_secret: str


def check_custom_secret(secret: str) -> str:
    """A caller-supplied secret (migrating from another provider) must be ``whsec_`` +
    base64 of 24-64 random bytes."""
    if not secret.startswith(SECRET_PREFIX):
        raise _invalid_secret("signing secret must start with whsec_")
    try:
        key = decode_secret(secret)
    except ValueError as exc:
        raise _invalid_secret(str(exc)) from exc
    if not MIN_SECRET_BYTES <= len(key) <= MAX_SECRET_BYTES:
        raise _invalid_secret(
            f"signing secret must decode to {MIN_SECRET_BYTES}-{MAX_SECRET_BYTES} bytes"
        )
    return secret


def _invalid_secret(message: str) -> UnprocessableEntityError:
    return UnprocessableEntityError(
        message, code="invalid_signing_secret", details=[{"loc": ["body", "secret"]}]
    )


async def rotate_signing_secret(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    *,
    box: SecretBox,
    policy: SecretRotationPolicy,
    expected_version: int | None,
    overlap_s: int | None = None,
    secret: str | None = None,
) -> RotatedSecret:
    """Replace the signing secret; the old one keeps signing for the overlap window."""
    overlap = policy.overlap(overlap_s)
    new_secret = generate_signing_secret() if secret is None else check_custom_secret(secret)
    values: dict[str, Any] = {
        "secret_ciphertext": box.encrypt(new_secret, aad=secret_aad(tenant_id, endpoint_id)),
        "secret_rotated_at": func.now(),
        "version": Endpoint.version + 1,
    }
    if overlap > 0:
        # Right-hand sides see the row before the update: this moves the *current* secret.
        values["previous_secret_ciphertext"] = Endpoint.secret_ciphertext
        values["previous_secret_expires_at"] = func.now() + timedelta(seconds=overlap)
    else:
        values["previous_secret_ciphertext"] = None
        values["previous_secret_expires_at"] = None
    conditions = _live(tenant_id, endpoint_id)
    if expected_version is not None:
        conditions.append(Endpoint.version == expected_version)
    endpoint = await session.scalar(
        update(Endpoint)
        .where(*conditions)
        .values(**values)
        .returning(Endpoint)
        .execution_options(populate_existing=True)
    )
    if endpoint is None:
        raise await _version_mismatch(session, tenant_id, endpoint_id)
    logger.info(
        "endpoint.secret_rotated",
        tenant_id=str(tenant_id),
        endpoint_id=str(endpoint_id),
        overlap_s=overlap,
        custom_secret=secret is not None,
        version=endpoint.version,
    )
    return RotatedSecret(endpoint=endpoint, signing_secret=new_secret)


async def end_secret_overlap(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    *,
    expected_version: int | None,
) -> Endpoint:
    """Stop signing with the previous secret now. Idempotent: without an active previous
    secret it changes nothing (no version bump), but a stale If-Match still answers 412."""
    conditions = [*_live(tenant_id, endpoint_id), Endpoint.previous_secret_ciphertext.is_not(None)]
    if expected_version is not None:
        conditions.append(Endpoint.version == expected_version)
    endpoint = await session.scalar(
        update(Endpoint)
        .where(*conditions)
        .values(
            previous_secret_ciphertext=None,
            previous_secret_expires_at=None,
            version=Endpoint.version + 1,
        )
        .returning(Endpoint)
        .execution_options(populate_existing=True)
    )
    if endpoint is not None:
        logger.info(
            "endpoint.secret_overlap_ended", tenant_id=str(tenant_id), endpoint_id=str(endpoint_id)
        )
        return endpoint
    current = await get_endpoint(session, tenant_id, endpoint_id)  # 404 when gone
    if expected_version is not None and current.version != expected_version:
        raise await _version_mismatch(session, tenant_id, endpoint_id)
    return current
