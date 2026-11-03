"""Contracts for event ingestion and the event/delivery read APIs."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from hookrelay.db.models import Delivery, Event
from hookrelay.db.models.event import EXTERNAL_ID_PATTERN, MAX_EXTERNAL_ID_LENGTH
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH

_EVENT_EXAMPLE_ID = "0b9f3c1e-6a43-4c55-9d0e-2f6a7b8c9d10"


class EventCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "event_type": "invoice.paid",
                    "external_id": "inv_1042:paid",
                    "occurred_at": "2026-11-01T09:30:00Z",
                    "payload": {"invoice_id": "inv_1042", "amount": 4200, "currency": "INR"},
                }
            ]
        },
    )

    event_type: str = Field(
        min_length=1,
        max_length=MAX_EVENT_TYPE_LENGTH,
        pattern=EVENT_TYPE_PATTERN,
        description="A registered, non-archived event type.",
    )
    payload: dict[str, Any] = Field(
        description="JSON object delivered to subscribers (size limited by "
        "`EVENT_MAX_PAYLOAD_BYTES`, measured as canonical JSON)."
    )
    external_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=MAX_EXTERNAL_ID_LENGTH,
        pattern=EXTERNAL_ID_PATTERN,
        description="Optional client-supplied event id, unique per tenant. Publishing the same "
        "id twice returns 409 with the existing event's id.",
    )
    occurred_at: AwareDatetime | None = Field(
        default=None,
        description="When the event happened (timezone required). Defaults to the time it was "
        "accepted; may not be more than `EVENT_OCCURRED_AT_MAX_FUTURE_S` in the future.",
    )


class EventAccepted(BaseModel):
    """Body of ``202 Accepted``: the event is stored and queued for fan-out."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": _EVENT_EXAMPLE_ID,
                    "event_type": "invoice.paid",
                    "external_id": "inv_1042:paid",
                    "occurred_at": "2026-11-01T09:30:00Z",
                    "created_at": "2026-11-01T09:30:00.120Z",
                    "retain_until": "2026-12-01T09:30:00.120Z",
                }
            ]
        }
    )

    id: uuid.UUID
    event_type: str
    external_id: str | None
    occurred_at: datetime
    created_at: datetime
    retain_until: datetime

    @classmethod
    def from_model(cls, event: Event) -> EventAccepted:
        return cls(
            id=event.id,
            event_type=event.event_type,
            external_id=event.external_id,
            occurred_at=event.occurred_at,
            created_at=event.created_at,
            retain_until=event.retain_until,
        )


class EventSummary(EventAccepted):
    """List item: metadata only (payloads can be large; fetch one event for its payload)."""

    payload_size: int
    fanned_out_at: datetime | None = Field(
        description="When the relay created this event's deliveries; null while queued."
    )

    @classmethod
    def from_model(cls, event: Event) -> EventSummary:
        return cls(
            **EventAccepted.from_model(event).model_dump(),
            payload_size=event.payload_size,
            fanned_out_at=event.fanned_out_at,
        )


class EventOut(EventSummary):
    payload: dict[str, Any]

    @classmethod
    def from_model(cls, event: Event) -> EventOut:
        return cls(**EventSummary.from_model(event).model_dump(), payload=event.payload)


class DeliveryOut(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "5d2c8e7a-1b3f-4e9a-8c6d-7f0a1b2c3d4e",
                    "event_id": _EVENT_EXAMPLE_ID,
                    "endpoint_id": "9a8b7c6d-5e4f-4a3b-9c2d-1e0f9a8b7c6d",
                    "status": "pending",
                    "attempt_count": 1,
                    "next_attempt_at": "2026-11-01T09:30:04.120Z",
                    "last_attempt_at": "2026-11-01T09:30:00.410Z",
                    "last_status_code": 503,
                    "last_error_class": "http_status",
                    "completed_at": None,
                    "replay_count": 0,
                    "run_started_at": "2026-11-01T09:30:00.300Z",
                    "created_at": "2026-11-01T09:30:00.300Z",
                    "updated_at": "2026-11-01T09:30:00.690Z",
                }
            ]
        }
    )

    id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    status: str = Field(
        description="`pending`, `delivering` (a worker holds it), `succeeded`, `failed` "
        "(permanent error, e.g. 4xx or a blocked destination) or `dead` (dead-letter queue: "
        "retryable errors until attempts or max age ran out). `failed` and `dead` deliveries "
        "(and `succeeded` ones) can be replayed."
    )
    attempt_count: int = Field(description="Attempts started so far (including an in-flight one).")
    next_attempt_at: datetime = Field(description="When a `pending` delivery is next due.")
    last_attempt_at: datetime | None
    last_status_code: int | None = Field(description="HTTP status of the latest attempt, if any.")
    last_error_class: str | None = Field(
        description="Error class of the latest unsuccessful attempt (see the attempt log)."
    )
    completed_at: datetime | None
    replay_count: int = Field(description="Times this delivery was replayed.")
    run_started_at: datetime = Field(
        description="Start of the current run (creation, or the latest replay); the retry "
        "budget counts from here."
    )
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, delivery: Delivery) -> DeliveryOut:
        return cls(
            id=delivery.id,
            event_id=delivery.event_id,
            endpoint_id=delivery.endpoint_id,
            status=delivery.status,
            attempt_count=delivery.attempt_count,
            next_attempt_at=delivery.next_attempt_at,
            last_attempt_at=delivery.last_attempt_at,
            last_status_code=delivery.last_status_code,
            last_error_class=delivery.last_error_class,
            completed_at=delivery.completed_at,
            replay_count=delivery.replay_count,
            run_started_at=delivery.run_started_at,
            created_at=delivery.created_at,
            updated_at=delivery.updated_at,
        )
