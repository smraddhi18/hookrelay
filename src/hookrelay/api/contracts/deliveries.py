"""Contracts for the delivery read API and the delivery-attempt log."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from hookrelay.db.models import DeliveryAttempt


class DeliveryAttemptOut(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "3e1f0c2b-7d4a-4f6e-9b8c-0a1b2c3d4e5f",
                    "delivery_id": "5d2c8e7a-1b3f-4e9a-8c6d-7f0a1b2c3d4e",
                    "attempt_number": 1,
                    "outcome": "retrying",
                    "status_code": 503,
                    "error_class": "http_status",
                    "error_message": "receiver returned HTTP 503",
                    "latency_ms": 182,
                    "request_url": "https://hooks.acme.example/hookrelay",
                    "request_headers": {
                        "content-type": "application/json",
                        "user-agent": "hookrelay-webhooks/1.0",
                        "webhook-id": "0b9f3c1e-6a43-4c55-9d0e-2f6a7b8c9d10",
                        "webhook-timestamp": "1793525400",
                        "webhook-signature": "v1,[redacted]",
                        "x-acme-tenant": "********",
                    },
                    "response_body": '{"error":"maintenance"}',
                    "response_truncated": False,
                    "worker_id": "worker-1:7:1a2b3c4d",
                    "started_at": "2026-11-01T09:30:00.410Z",
                    "finished_at": "2026-11-01T09:30:00.592Z",
                    "next_attempt_at": "2026-11-01T09:30:04.120Z",
                    "created_at": "2026-11-01T09:30:00.600Z",
                }
            ]
        }
    )

    id: uuid.UUID
    delivery_id: uuid.UUID
    attempt_number: int
    outcome: str = Field(
        description="`succeeded`, `retrying` (another attempt is scheduled) "
        "or `failed` (no more attempts)."
    )
    status_code: int | None = Field(description="Receiver's HTTP status; null if none arrived.")
    error_class: str | None = Field(
        description="`http_status`, `redirect`, `timeout`, `connect_error`, `dns_error`, "
        "`tls_error`, `protocol_error`, `destination_blocked`, `secret_unavailable`, "
        "`lease_expired` or `max_age_exceeded`; null on success."
    )
    error_message: str | None
    latency_ms: int | None
    request_url: str
    request_headers: dict[str, Any] = Field(
        description="Headers sent, with custom header values masked and the signature redacted."
    )
    response_body: str | None = Field(
        description="First DELIVERY_RESPONSE_MAX_BYTES of the response body (UTF-8)."
    )
    response_truncated: bool
    worker_id: str
    started_at: datetime
    finished_at: datetime
    next_attempt_at: datetime | None
    created_at: datetime

    @classmethod
    def from_model(cls, attempt: DeliveryAttempt) -> DeliveryAttemptOut:
        return cls(
            id=attempt.id,
            delivery_id=attempt.delivery_id,
            attempt_number=attempt.attempt_number,
            outcome=attempt.outcome,
            status_code=attempt.status_code,
            error_class=attempt.error_class,
            error_message=attempt.error_message,
            latency_ms=attempt.latency_ms,
            request_url=attempt.request_url,
            request_headers=dict(attempt.request_headers),
            response_body=attempt.response_body,
            response_truncated=attempt.response_truncated,
            worker_id=attempt.worker_id,
            started_at=attempt.started_at,
            finished_at=attempt.finished_at,
            next_attempt_at=attempt.next_attempt_at,
            created_at=attempt.created_at,
        )
