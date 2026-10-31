"""``GET /v1/me``."""

from __future__ import annotations

from fastapi import APIRouter

from hookrelay.api.contracts.me import MeApiKey, MeResponse, MeTenant
from hookrelay.api.deps import ReaderDep

router = APIRouter(tags=["me"])


@router.get("/me", response_model=MeResponse, summary="Describe the calling API key and tenant")
async def me(principal: ReaderDep) -> MeResponse:
    return MeResponse(
        tenant=MeTenant(id=principal.tenant_id, slug=principal.tenant_slug),
        api_key=MeApiKey(
            id=principal.api_key_id,
            name=principal.api_key_name,
            prefix=principal.api_key_prefix,
            role=principal.role,
        ),
    )
