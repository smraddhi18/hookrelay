"""Contract for subscriptions and fan-out matching."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from hookrelay.db.models import Subscription
from hookrelay.db.models.subscription import MAX_PATTERN_LENGTH, SUBSCRIPTION_PATTERN


class SubscriptionCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [{"event_type": "invoice.paid"}, {"event_type": "invoice.*"}]
        },
    )

    event_type: str = Field(
        min_length=1,
        max_length=MAX_PATTERN_LENGTH,
        pattern=SUBSCRIPTION_PATTERN,
        description="A registered event type (`invoice.paid`), a prefix wildcard (`invoice.*`, "
        "matching `invoice.paid` and `invoice.line.added`) or `*` for everything.",
    )


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "e1f2a3b4-c5d6-4e7f-8a9b-0c1d2e3f4a5b",
                    "endpoint_id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                    "event_type": "invoice.*",
                    "wildcard": True,
                    "created_at": "2026-10-31T09:30:00Z",
                }
            ]
        }
    )

    id: uuid.UUID
    endpoint_id: uuid.UUID
    event_type: str
    wildcard: bool
    created_at: datetime

    @classmethod
    def from_model(cls, subscription: Subscription) -> SubscriptionOut:
        return cls(
            id=subscription.id,
            endpoint_id=subscription.endpoint_id,
            event_type=subscription.event_type_pattern,
            wildcard=subscription.is_wildcard,
            created_at=subscription.created_at,
        )


class MatchedEndpoint(BaseModel):
    id: uuid.UUID
    url: str
    matched_patterns: list[str]


class MatchResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "event_type": "invoice.paid",
                    "endpoints": [
                        {
                            "id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                            "url": "https://hooks.example.com/hookrelay",
                            "matched_patterns": ["invoice.*", "invoice.paid"],
                        }
                    ],
                }
            ]
        }
    )

    event_type: str
    endpoints: list[MatchedEndpoint]
