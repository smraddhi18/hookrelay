"""Operational probes. Excluded from auth (when it lands) and cheap enough to poll every second."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response, status

from hookrelay import __version__
from hookrelay.api.schemas import DependencyCheck, LivenessResponse, ReadinessResponse
from hookrelay.core.config import Settings
from hookrelay.db.session import DatabaseDep
from hookrelay.redis_ import RedisBundle
from hookrelay.services.health import readiness

router = APIRouter(tags=["health"])


@router.get(
    "/health",
    summary="Liveness probe",
    response_model=LivenessResponse,
)
async def health(request: Request) -> LivenessResponse:
    settings: Settings = request.app.state.settings
    return LivenessResponse(service=settings.app_name, version=__version__)


@router.get(
    "/ready",
    summary="Readiness probe (Postgres required; Redis soft unless REDIS_REQUIRED_FOR_READY)",
    response_model=ReadinessResponse,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
)
async def ready(request: Request, response: Response, database: DatabaseDep) -> ReadinessResponse:
    settings: Settings = request.app.state.settings
    redis_bundle: RedisBundle | None = getattr(request.app.state, "redis", None)
    redis_facade = None if redis_bundle is None else redis_bundle.command
    report = await readiness(
        database,
        timeout_s=settings.readiness_timeout_s,
        draining=bool(getattr(request.app.state, "draining", False)),
        redis=redis_facade,
        redis_required=settings.redis_required_for_ready,
    )
    if not report.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    response.headers["Cache-Control"] = "no-store"
    return ReadinessResponse(
        status="ready" if report.ready else "not_ready",
        draining=report.draining,
        checks={
            name: DependencyCheck(
                status=check.status.value, latency_ms=check.latency_ms, error=check.error
            )
            for name, check in report.checks.items()
        },
    )
