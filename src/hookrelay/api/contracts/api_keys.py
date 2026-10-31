"""Contract for tenant API keys. The plaintext key appears in exactly two responses: create and
rotate. Everything else exposes only the non-secret ``prefix``."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from hookrelay.db.models import ApiKey, ApiKeyRole, ApiKeyStatus

_KEY_EXAMPLE: dict[str, Any] = {
    "id": "6f1c2b8e-1a7d-4a51-9a3f-5d7a0f1e2c33",
    "name": "billing-service",
    "role": "publisher",
    "prefix": "k3x9q2m7a1b4",
    "status": "active",
    "expires_at": None,
    "revoked_at": None,
    "last_used_at": "2026-10-31T09:41:00Z",
    "rotated_from_id": None,
    "created_at": "2026-10-31T09:30:00Z",
}


class ApiKeyCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={"examples": [{"name": "billing-service", "role": "publisher"}]},
    )

    name: str = Field(min_length=1, max_length=100)
    role: ApiKeyRole = Field(
        description="`admin` ⊇ `publisher` ⊇ `reader`. Admin manages keys, endpoints and "
        "subscriptions; publisher registers event types (and will publish messages in M2); "
        "reader is read-only."
    )
    expires_at: AwareDatetime | None = Field(
        default=None, description="Optional hard expiry (must be in the future)."
    )


class ApiKeyOut(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": [_KEY_EXAMPLE]})

    id: uuid.UUID
    name: str
    role: ApiKeyRole
    prefix: str = Field(description="Non-secret lookup prefix, safe to show in UIs and logs.")
    status: ApiKeyStatus
    expires_at: datetime | None
    revoked_at: datetime | None
    last_used_at: datetime | None = Field(
        description="Approximate (updated at most once per API_KEY_LAST_USED_RESOLUTION_S)."
    )
    rotated_from_id: uuid.UUID | None
    created_at: datetime

    @classmethod
    def from_model(cls, key: ApiKey) -> ApiKeyOut:
        return cls(
            id=key.id,
            name=key.name,
            role=ApiKeyRole(key.role),
            prefix=key.prefix,
            status=key.status_at(datetime.now(UTC)),
            expires_at=key.expires_at,
            revoked_at=key.revoked_at,
            last_used_at=key.last_used_at,
            rotated_from_id=key.rotated_from_id,
            created_at=key.created_at,
        )


class ApiKeyCreated(ApiKeyOut):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    **_KEY_EXAMPLE,
                    "key": "hrk_k3x9q2m7a1b4_Zr4p1mY8u0x7e2QwN5kV9sJ3hT6bL0aC8dF1gH4jK7mP",
                }
            ]
        }
    )

    key: str = Field(description="The full API key. Shown once; store it in a secret manager.")


class RotatedApiKey(BaseModel):
    """The successor key (with its secret) plus the old key with its shortened expiry."""

    new_key: ApiKeyCreated
    previous_key: ApiKeyOut
