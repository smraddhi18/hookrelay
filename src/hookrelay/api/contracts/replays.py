"""Contract for the replay API."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from hookrelay.db.models import DeliveryStatus, ReplayRequest
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH
from hookrelay.services.replays import DEFAULT_BULK_STATUSES, REPLAYABLE_STATUSES

_STATUSES_DESCRIPTION = "Which terminal deliveries to replay: any of `succeeded`, `failed`, `dead`."


def _check_statuses(value: list[DeliveryStatus]) -> list[DeliveryStatus]:
    bad = sorted(s.value for s in value if s not in REPLAYABLE_STATUSES)
    if bad:
        raise ValueError(f"Only terminal statuses can be replayed, not: {', '.join(bad)}")
    return sorted(set(value))


class EventReplay(BaseModel):
    """Optional body of ``POST /v1/events/{id}/replay``."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"examples": [{}, {"statuses": ["dead"], "endpoint_id": None}]},
    )

    statuses: list[DeliveryStatus] = Field(
        default_factory=lambda: list(DEFAULT_BULK_STATUSES),
        min_length=1,
        description=_STATUSES_DESCRIPTION + " Defaults to `failed` and `dead`.",
    )
    endpoint_id: uuid.UUID | None = Field(
        default=None, description="Only this endpoint's delivery of the event."
    )

    @field_validator("statuses")
    @classmethod
    def _validate_statuses(cls, value: list[DeliveryStatus]) -> list[DeliveryStatus]:
        return _check_statuses(value)


class BulkReplay(BaseModel):
    """Body of ``POST /v1/replays``: replay deliveries created in a time window."""

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "created_after": "2026-11-03T08:00:00Z",
                    "created_before": "2026-11-03T09:00:00Z",
                    "endpoint_id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                    "statuses": ["failed", "dead"],
                }
            ]
        },
    )

    created_after: AwareDatetime = Field(description="Inclusive lower bound on delivery creation.")
    created_before: AwareDatetime = Field(
        description="Exclusive upper bound; the window may span at most REPLAY_MAX_WINDOW_S."
    )
    endpoint_id: uuid.UUID | None = None
    event_type: str | None = Field(
        default=None, max_length=MAX_EVENT_TYPE_LENGTH, pattern=EVENT_TYPE_PATTERN
    )
    statuses: list[DeliveryStatus] = Field(
        default_factory=lambda: list(DEFAULT_BULK_STATUSES),
        min_length=1,
        description=_STATUSES_DESCRIPTION + " Defaults to `failed` and `dead`.",
    )

    @field_validator("statuses")
    @classmethod
    def _validate_statuses(cls, value: list[DeliveryStatus]) -> list[DeliveryStatus]:
        return _check_statuses(value)


class ReplayOut(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "0b6c2d1e-3f4a-4b5c-8d6e-7f8091a2b3c4",
                    "scope": "filter",
                    "filters": {
                        "created_after": "2026-11-03T08:00:00+00:00",
                        "created_before": "2026-11-03T09:00:00+00:00",
                        "statuses": ["dead", "failed"],
                    },
                    "replayed": 1000,
                    "has_more": True,
                    "created_at": "2026-11-03T09:05:00Z",
                }
            ]
        }
    )

    id: uuid.UUID
    scope: str = Field(description="`delivery`, `event` or `filter`.")
    filters: dict[str, Any]
    replayed: int = Field(description="Deliveries put back on the queue by this request.")
    has_more: bool = Field(
        description="More deliveries matched than REPLAY_MAX_DELIVERIES; replay again with a "
        "later created_after to continue."
    )
    created_at: datetime

    @classmethod
    def from_model(cls, request: ReplayRequest) -> ReplayOut:
        return cls(
            id=request.id,
            scope=request.scope,
            filters=dict(request.filters),
            replayed=request.replayed,
            has_more=request.has_more,
            created_at=request.created_at,
        )
