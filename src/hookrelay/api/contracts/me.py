"""``GET /v1/me``: who am I (lets clients verify a key and discover its role)."""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict

from hookrelay.db.models import ApiKeyRole


class MeTenant(BaseModel):
    id: uuid.UUID
    slug: str


class MeApiKey(BaseModel):
    id: uuid.UUID
    name: str
    prefix: str
    role: ApiKeyRole


class MeResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "tenant": {"id": "0b7e3c9e-5d0c-4a3e-9c51-2f1d1f6f7a10", "slug": "acme"},
                    "api_key": {
                        "id": "6f1c2b8e-1a7d-4a51-9a3f-5d7a0f1e2c33",
                        "name": "billing-service",
                        "prefix": "k3x9q2m7a1b4",
                        "role": "publisher",
                    },
                }
            ]
        }
    )

    tenant: MeTenant
    api_key: MeApiKey
