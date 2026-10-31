"""Admin (operator) contract for tenants."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from hookrelay.db.models import TenantStatus
from hookrelay.db.models.tenant import SLUG_PATTERN


class TenantCreate(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={"examples": [{"slug": "acme", "name": "Acme Corp"}]},
    )

    slug: str = Field(min_length=1, max_length=63, pattern=SLUG_PATTERN)
    name: str = Field(min_length=1, max_length=200)


class TenantOut(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        json_schema_extra={
            "examples": [
                {
                    "id": "0b7e3c9e-5d0c-4a3e-9c51-2f1d1f6f7a10",
                    "slug": "acme",
                    "name": "Acme Corp",
                    "status": "active",
                    "created_at": "2026-10-31T09:30:00Z",
                    "updated_at": "2026-10-31T09:30:00Z",
                }
            ]
        },
    )

    id: uuid.UUID
    slug: str
    name: str
    status: TenantStatus
    created_at: datetime
    updated_at: datetime
