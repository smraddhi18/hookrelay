"""Contract for webhook endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from hookrelay.core.config import TimeoutMs
from hookrelay.db.models import Endpoint
from hookrelay.db.models.endpoint import MAX_URL_LENGTH
from hookrelay.services.endpoint_rules import mask_headers, validate_custom_headers

_ENDPOINT_EXAMPLE: dict[str, Any] = {
    "id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
    "url": "https://hooks.example.com/hookrelay",
    "description": "Billing service receiver",
    "enabled": True,
    "headers": {"X-Receiver-Token": "********"},
    "timeout_ms": 10000,
    "version": 1,
    "created_at": "2026-10-31T09:30:00Z",
    "updated_at": "2026-10-31T09:30:00Z",
}


def _check_headers(value: dict[str, str] | None) -> dict[str, str] | None:
    return None if value is None else validate_custom_headers(value)


class EndpointCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "url": "https://hooks.example.com/hookrelay",
                    "description": "Billing service receiver",
                    "headers": {"X-Receiver-Token": "s3cr3t"},
                    "timeout_ms": 10000,
                }
            ]
        },
    )

    url: str = Field(
        min_length=1,
        max_length=MAX_URL_LENGTH,
        description="Absolute https URL (http only if ENDPOINT_ALLOW_HTTP=true). No credentials "
        "or fragment; private/loopback hosts are rejected unless explicitly allowed.",
    )
    description: str | None = Field(default=None, max_length=500)
    enabled: bool = True
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Extra headers sent with every delivery (max 20). Hop-by-hop, framing and "
        "signature headers are reserved.",
    )
    timeout_ms: TimeoutMs | None = Field(
        default=None, description="Per-attempt timeout; defaults to ENDPOINT_DEFAULT_TIMEOUT_MS."
    )

    @field_validator("headers")
    @classmethod
    def _validate_headers(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        return _check_headers(value)


class EndpointUpdate(BaseModel):
    """Partial update (JSON merge semantics): only the fields present are changed."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"enabled": False}, {"timeout_ms": 5000}]},
    )

    url: str | None = Field(default=None, min_length=1, max_length=MAX_URL_LENGTH)
    description: str | None = Field(default=None, max_length=500)
    enabled: bool | None = None
    headers: dict[str, str] | None = Field(
        default=None, description="Replaces the whole header map."
    )
    timeout_ms: TimeoutMs | None = None

    @field_validator("headers")
    @classmethod
    def _validate_headers(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        return _check_headers(value)

    @model_validator(mode="after")
    def _non_empty_and_non_null(self) -> EndpointUpdate:
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided")
        nullable = {"description"}
        nulls = [f for f in self.model_fields_set - nullable if getattr(self, f) is None]
        if nulls:
            raise ValueError(f"Fields cannot be null: {', '.join(sorted(nulls))}")
        return self

    def changes(self) -> dict[str, Any]:
        return {field: getattr(self, field) for field in self.model_fields_set}


class EndpointOut(BaseModel):
    model_config = ConfigDict(json_schema_extra={"examples": [_ENDPOINT_EXAMPLE]})

    id: uuid.UUID
    url: str
    description: str | None
    enabled: bool
    headers: dict[str, str] = Field(description="Values are masked unless the caller is admin.")
    timeout_ms: int
    version: int = Field(description="Also returned as the ETag header.")
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, endpoint: Endpoint, *, reveal_headers: bool) -> EndpointOut:
        headers = dict(endpoint.headers)
        return cls(
            id=endpoint.id,
            url=endpoint.url,
            description=endpoint.description,
            enabled=endpoint.enabled,
            headers=headers if reveal_headers else mask_headers(headers),
            timeout_ms=endpoint.timeout_ms,
            version=endpoint.version,
            created_at=endpoint.created_at,
            updated_at=endpoint.updated_at,
        )


class EndpointCreated(EndpointOut):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    **_ENDPOINT_EXAMPLE,
                    "headers": {"X-Receiver-Token": "s3cr3t"},
                    "signing_secret": "whsec_MfKQ9r8GKYqrTqhvnGYfDSWRgJdnTgRkWDNg8tEKlrM=",
                }
            ]
        }
    )

    signing_secret: str = Field(
        description="HMAC-SHA256 signing secret (Standard Webhooks `whsec_` format). Store it in "
        "the receiver; admins can fetch it again from GET /v1/endpoints/{id}/secret."
    )


class EndpointSecret(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "endpoint_id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                    "signing_secret": "whsec_MfKQ9r8GKYqrTqhvnGYfDSWRgJdnTgRkWDNg8tEKlrM=",
                }
            ]
        }
    )

    endpoint_id: uuid.UUID
    signing_secret: str
