"""Tenant dashboard API (reader+)."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query

from hookrelay.api.contracts.common import error_responses
from hookrelay.api.contracts.dashboard import DashboardOut
from hookrelay.api.deps import ReaderDep, SettingsDep
from hookrelay.db.session import SessionDep
from hookrelay.services import dashboard as service

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


@router.get(
    "",
    response_model=DashboardOut,
    summary="Tenant dashboard: throughput, success rate, latency percentiles",
    responses=error_responses(422),
)
async def get_dashboard(
    principal: ReaderDep,
    session: SessionDep,
    settings: SettingsDep,
    window_s: Annotated[
        int | None,
        Query(
            ge=60,
            description="Look-back window in seconds. Default DASHBOARD_DEFAULT_WINDOW_S, "
            "at most DASHBOARD_MAX_WINDOW_S.",
        ),
    ] = None,
) -> DashboardOut:
    dash = await service.get_tenant_dashboard(
        session,
        principal.tenant_id,
        policy=service.DashboardPolicy.from_settings(settings),
        window_s=window_s,
    )
    return DashboardOut.from_dashboard(dash)
