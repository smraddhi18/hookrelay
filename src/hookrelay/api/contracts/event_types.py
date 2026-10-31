"""Contract for the event type registry."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from hookrelay.db.models import EventType
from hookrelay.db.models.event_type import EVENT_TYPE_PATTERN, MAX_EVENT_TYPE_LENGTH


class EventTypeCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [{"name": "invoice.paid", "description": "An invoice was paid in full"}]
        },
    )

    name: str = Field(
        min_length=1,
        max_length=MAX_EVENT_TYPE_LENGTH,
        pattern=EVENT_TYPE_PATTERN,
        description="Dot-separated lowercase segments (max 8), e.g. `invoice.paid`.",
    )
    description: str | None = Field(default=None, max_length=500)


class EventTypeUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid", json_schema_extra={"examples": [{"archived": True}]})

    description: str | None = Field(default=None, max_length=500)
    archived: bool | None = Field(
        default=None,
        description="Archived types keep existing subscriptions but accept no new exact ones.",
    )

    @model_validator(mode="after")
    def _non_empty(self) -> EventTypeUpdate:
        if not self.model_fields_set:
            raise ValueError("At least one field must be provided")
        if "archived" in self.model_fields_set and self.archived is None:
            raise ValueError("archived cannot be null")
        return self

    def changes(self) -> dict[str, Any]:
        return {field: getattr(self, field) for field in self.model_fields_set}


class EventTypeOut(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "id": "c2b7d1e4-3f5a-4b6c-8d9e-0f1a2b3c4d5e",
                    "name": "invoice.paid",
                    "description": "An invoice was paid in full",
                    "archived": False,
                    "archived_at": None,
                    "version": 1,
                    "created_at": "2026-10-31T09:30:00Z",
                    "updated_at": "2026-10-31T09:30:00Z",
                }
            ]
        }
    )

    id: uuid.UUID
    name: str
    description: str | None
    archived: bool
    archived_at: datetime | None
    version: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_model(cls, event_type: EventType) -> EventTypeOut:
        return cls(
            id=event_type.id,
            name=event_type.name,
            description=event_type.description,
            archived=event_type.archived_at is not None,
            archived_at=event_type.archived_at,
            version=event_type.version,
            created_at=event_type.created_at,
            updated_at=event_type.updated_at,
        )
