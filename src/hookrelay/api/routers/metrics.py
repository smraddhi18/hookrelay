"""Prometheus scrape endpoint.

Unauthenticated by default (scrape-network assumption — put Prometheus on a private network
or mesh). When ``METRICS_TOKEN`` is set, require ``Authorization: Bearer <token>`` compared in
constant time. The exposition never includes tenant payloads or secrets.
"""

from __future__ import annotations

import hmac
import logging

from fastapi import APIRouter, Request, Response
from fastapi.responses import PlainTextResponse

from hookrelay.core.config import Settings
from hookrelay.core.errors import UnauthorizedError
from hookrelay.db.session import DatabaseDep
from hookrelay.observability.gauges import refresh_gauges
from hookrelay.observability.metrics import Metrics, render_metrics
from hookrelay.services.api_keys import BEARER_CHALLENGE

router = APIRouter(tags=["metrics"])
_log = logging.getLogger("hookrelay.metrics")


@router.get(
    "/metrics",
    summary="Prometheus metrics",
    response_class=PlainTextResponse,
    responses={401: {"description": "METRICS_TOKEN set and Authorization missing/wrong"}},
)
async def prometheus_metrics(request: Request, database: DatabaseDep) -> Response:
    settings: Settings = request.app.state.settings
    if not settings.metrics_enabled:
        return PlainTextResponse("# metrics disabled\n", status_code=404)

    expected = settings.metrics_token
    if expected is not None:
        auth = request.headers.get("authorization", "")
        scheme, _, token = auth.partition(" ")
        if scheme.lower() != "bearer" or not hmac.compare_digest(
            token.encode(), expected.get_secret_value().encode()
        ):
            raise UnauthorizedError("Invalid metrics token", headers=BEARER_CHALLENGE)

    obs = getattr(request.app.state, "observability", None)
    metrics_obj: Metrics | None = None if obs is None else obs.metrics
    if metrics_obj is None:
        return PlainTextResponse("# metrics not initialised\n", status_code=503)

    try:
        async with database.session() as session:
            await refresh_gauges(session, metrics_obj)
    except Exception as exc:
        _log.warning("metrics.gauge_refresh_failed", exc_info=exc)

    body, content_type = render_metrics(metrics_obj)
    return Response(content=body, media_type=content_type)
