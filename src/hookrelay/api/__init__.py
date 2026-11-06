"""HTTP layer: routers, request/response contracts and dependencies.

Auth is attached at the router level (deny by default): everything under ``/v1`` requires a
tenant API key and everything under ``/admin`` the operator token. Only the probes in
``health`` and the Prometheus ``/metrics`` scrape endpoint are public.
"""

from fastapi import APIRouter, Depends

from hookrelay.api.contracts.common import error_responses
from hookrelay.api.deps import authenticate
from hookrelay.api.routers import (
    admin_tenants,
    api_keys,
    dashboard,
    deliveries,
    endpoints,
    event_types,
    events,
    health,
    me,
    metrics,
    replays,
    subscriptions,
)

v1_router = APIRouter(
    prefix="/v1",
    dependencies=[Depends(authenticate)],
    responses=error_responses(401, 403),
)
v1_router.include_router(me.router)
v1_router.include_router(api_keys.router)
v1_router.include_router(endpoints.router)
v1_router.include_router(event_types.router)
v1_router.include_router(subscriptions.router)
v1_router.include_router(events.router)
v1_router.include_router(deliveries.router)
v1_router.include_router(replays.router)
v1_router.include_router(dashboard.router)

api_router = APIRouter()
api_router.include_router(health.router)
api_router.include_router(metrics.router)
api_router.include_router(admin_tenants.router)
api_router.include_router(v1_router)

__all__ = ["api_router"]
