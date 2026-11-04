"""Contract for webhook endpoints."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from hookrelay.core.config import TimeoutMs
from hookrelay.db.models import BreakerState, Endpoint
from hookrelay.db.models.endpoint import MAX_IN_FLIGHT, MAX_RATE_LIMIT_PER_S, MAX_URL_LENGTH
from hookrelay.services.endpoint_health import EndpointHealth
from hookrelay.services.endpoint_rules import mask_headers, validate_custom_headers
from hookrelay.services.endpoint_secrets import MAX_SECRET_BYTES, MIN_SECRET_BYTES
from hookrelay.services.endpoints import CircuitStatus

_ENDPOINT_EXAMPLE: dict[str, Any] = {
    "id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
    "url": "https://hooks.example.com/hookrelay",
    "description": "Billing service receiver",
    "enabled": True,
    "headers": {"X-Receiver-Token": "********"},
    "timeout_ms": 10000,
    "rate_limit_per_s": 50,
    "max_in_flight": 10,
    "auto_disabled_at": None,
    "secret_rotated_at": None,
    "previous_secret_expires_at": None,
    "version": 1,
    "created_at": "2026-10-31T09:30:00Z",
    "updated_at": "2026-10-31T09:30:00Z",
}


RateLimitPerS = Annotated[
    int,
    Field(
        ge=1,
        le=MAX_RATE_LIMIT_PER_S,
        description="Most delivery attempts started per second for this endpoint, across all "
        "workers (token bucket, burst = 1 s). null = unlimited.",
    ),
]
MaxInFlight = Annotated[
    int,
    Field(
        ge=1,
        le=MAX_IN_FLIGHT,
        description="Most concurrent attempts for this endpoint across all workers. null = the "
        "deployment default (ENDPOINT_DEFAULT_MAX_IN_FLIGHT).",
    ),
]


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
                    "rate_limit_per_s": 50,
                    "max_in_flight": 10,
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
    rate_limit_per_s: RateLimitPerS | None = None
    max_in_flight: MaxInFlight | None = None

    @field_validator("headers")
    @classmethod
    def _validate_headers(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        return _check_headers(value)


class EndpointUpdate(BaseModel):
    """Partial update (JSON merge semantics): only the fields present are changed."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [{"enabled": False}, {"timeout_ms": 5000}, {"rate_limit_per_s": None}]
        },
    )

    url: str | None = Field(default=None, min_length=1, max_length=MAX_URL_LENGTH)
    description: str | None = Field(default=None, max_length=500)
    enabled: bool | None = None
    headers: dict[str, str] | None = Field(
        default=None, description="Replaces the whole header map."
    )
    timeout_ms: TimeoutMs | None = None
    rate_limit_per_s: RateLimitPerS | None = Field(default=None, description="null = unlimited.")
    max_in_flight: MaxInFlight | None = Field(
        default=None, description="null = the deployment default."
    )

    @field_validator("headers")
    @classmethod
    def _validate_headers(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        return _check_headers(value)

    @model_validator(mode="after")
    def _non_empty_and_non_null(self) -> EndpointUpdate:
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided")
        nullable = {"description", "rate_limit_per_s", "max_in_flight"}
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
    rate_limit_per_s: int | None = Field(description="null = unlimited.")
    max_in_flight: int | None = Field(description="null = the deployment default.")
    auto_disabled_at: datetime | None = Field(
        description="Set when HookRelay disabled the endpoint after sustained failure; cleared "
        "when enabled is changed."
    )
    secret_rotated_at: datetime | None = Field(
        default=None, description="When the signing secret was last rotated."
    )
    previous_secret_expires_at: datetime | None = Field(
        default=None,
        description="While in the future, deliveries are signed with the current and the "
        "previous secret (two space-separated v1 signatures).",
    )
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
            rate_limit_per_s=endpoint.rate_limit_per_s,
            max_in_flight=endpoint.max_in_flight,
            auto_disabled_at=endpoint.auto_disabled_at,
            secret_rotated_at=endpoint.secret_rotated_at,
            previous_secret_expires_at=endpoint.previous_secret_expires_at
            if endpoint.previous_secret_ciphertext is not None
            else None,
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


_CIRCUIT_EXAMPLE: dict[str, Any] = {
    "state": "open",
    "consecutive_failures": 7,
    "failing_since": "2026-11-03T08:00:00Z",
    "opened_at": "2026-11-03T08:02:10Z",
    "open_until": "2026-11-03T08:03:10Z",
    "trips": 2,
    "last_failure_at": "2026-11-03T08:02:10Z",
    "in_flight": 0,
    "max_in_flight": 10,
    "rate_limit_per_s": 50,
    "auto_disabled_at": None,
}


class CircuitOut(BaseModel):
    """Back-pressure state of an endpoint: circuit breaker, in-flight load and limits."""

    model_config = ConfigDict(json_schema_extra={"examples": [_CIRCUIT_EXAMPLE]})

    state: BreakerState = Field(
        description="closed: delivering normally. open: deliveries wait (no attempts spent) "
        "until open_until. half_open: one probe attempt decides between closed and open."
    )
    consecutive_failures: int
    failing_since: datetime | None = Field(description="Start of the current failure streak.")
    opened_at: datetime | None
    open_until: datetime | None = Field(description="When a probe is allowed (state open).")
    trips: int = Field(description="Times the breaker opened in this streak (cooldown doubles).")
    last_failure_at: datetime | None
    in_flight: int = Field(description="Attempts currently being made, across all workers.")
    max_in_flight: int = Field(description="Effective in-flight cap.")
    rate_limit_per_s: int | None
    auto_disabled_at: datetime | None

    @classmethod
    def from_status(cls, status: CircuitStatus) -> CircuitOut:
        return cls(
            state=status.state,
            consecutive_failures=status.consecutive_failures,
            failing_since=status.failing_since,
            opened_at=status.opened_at,
            open_until=status.open_until,
            trips=status.trips,
            last_failure_at=status.last_failure_at,
            in_flight=status.in_flight,
            max_in_flight=status.max_in_flight,
            rate_limit_per_s=status.rate_limit_per_s,
            auto_disabled_at=status.auto_disabled_at,
        )


class SecretRotate(BaseModel):
    """Body of POST /v1/endpoints/{id}/secret/rotate (every field optional)."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{"overlap_s": 86400}, {"overlap_s": 0}]},
    )

    overlap_s: int | None = Field(
        default=None,
        ge=0,
        description="How long the previous secret keeps signing alongside the new one. 0 = "
        "immediate cutover (e.g. after a leak). Default ENDPOINT_SECRET_ROTATION_OVERLAP_S, "
        "at most ENDPOINT_SECRET_MAX_OVERLAP_S.",
    )
    secret: str | None = Field(
        default=None,
        min_length=10,
        max_length=200,
        description=f"Bring your own secret (e.g. migrating from another provider): `whsec_` + "
        f"base64 of {MIN_SECRET_BYTES}-{MAX_SECRET_BYTES} random bytes. Default: generated.",
    )


class SecretRotated(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "endpoint_id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                    "signing_secret": "whsec_Zm9vYmFyYmF6cXV4cXV1eGNvcmdlZ3JhdWx0Z2FycGx5Lg==",
                    "rotated_at": "2026-11-04T10:00:00Z",
                    "previous_secret_expires_at": "2026-11-05T10:00:00Z",
                    "version": 4,
                }
            ]
        }
    )

    endpoint_id: uuid.UUID
    signing_secret: str = Field(description="The new secret; deploy it to the receiver.")
    rotated_at: datetime
    previous_secret_expires_at: datetime | None = Field(
        description="Until then the old secret signs too; null = immediate cutover."
    )
    version: int = Field(description="Also returned as the ETag header.")


class LastErrorOut(BaseModel):
    at: datetime
    delivery_id: uuid.UUID
    error_class: str
    status_code: int | None
    error_message: str | None


class EndpointHealthOut(BaseModel):
    """Delivery health of one endpoint over a recent window."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "endpoint_id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                    "enabled": True,
                    "window_s": 86400,
                    "window_started_at": "2026-11-03T10:00:00Z",
                    "computed_at": "2026-11-04T10:00:00Z",
                    "sampled": False,
                    "sample_started_at": "2026-11-03T10:00:04Z",
                    "attempts": 1250,
                    "succeeded": 1231,
                    "retrying": 15,
                    "failed": 4,
                    "success_rate": 0.9848,
                    "latency_p50_ms": 84.0,
                    "latency_p95_ms": 412.5,
                    "latency_p99_ms": 1890.2,
                    "errors_by_class": {"http_status": 12, "timeout": 7},
                    "last_error": {
                        "at": "2026-11-04T09:58:12Z",
                        "delivery_id": "5d2c8e7a-1b3f-4e9a-8c6d-7f0a1b2c3d4e",
                        "error_class": "timeout",
                        "status_code": None,
                        "error_message": "no response within 10000 ms",
                    },
                    "last_success_at": "2026-11-04T09:59:58Z",
                    "circuit": _CIRCUIT_EXAMPLE,
                    "backlog": 3,
                    "backlog_capped": False,
                }
            ]
        }
    )

    endpoint_id: uuid.UUID
    enabled: bool
    window_s: int
    window_started_at: datetime
    computed_at: datetime
    sampled: bool = Field(
        description="true when the window held more than ENDPOINT_HEALTH_MAX_ATTEMPTS attempts: "
        "the stats cover the newest ones, from sample_started_at."
    )
    sample_started_at: datetime | None
    attempts: int
    succeeded: int
    retrying: int
    failed: int
    success_rate: float | None = Field(description="succeeded / attempts; null with no attempts.")
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    errors_by_class: dict[str, int]
    last_error: LastErrorOut | None = Field(description="Most recent failed attempt, any time.")
    last_success_at: datetime | None
    circuit: CircuitOut
    backlog: int = Field(description="Pending deliveries waiting for this endpoint.")
    backlog_capped: bool = Field(description="true when backlog is a lower bound (capped).")

    @classmethod
    def from_health(cls, health: EndpointHealth) -> EndpointHealthOut:
        last = health.last_error
        return cls(
            endpoint_id=health.endpoint_id,
            enabled=health.enabled,
            window_s=health.window_s,
            window_started_at=health.window_started_at,
            computed_at=health.computed_at,
            sampled=health.sampled,
            sample_started_at=health.sample_started_at,
            attempts=health.attempts,
            succeeded=health.succeeded,
            retrying=health.retrying,
            failed=health.failed,
            success_rate=health.success_rate,
            latency_p50_ms=health.latency_p50_ms,
            latency_p95_ms=health.latency_p95_ms,
            latency_p99_ms=health.latency_p99_ms,
            errors_by_class=health.errors_by_class,
            last_error=None
            if last is None
            else LastErrorOut(
                at=last.at,
                delivery_id=last.delivery_id,
                error_class=last.error_class,
                status_code=last.status_code,
                error_message=last.error_message,
            ),
            last_success_at=health.last_success_at,
            circuit=CircuitOut.from_status(health.circuit),
            backlog=health.backlog,
            backlog_capped=health.backlog_capped,
        )
