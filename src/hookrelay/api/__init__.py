"""HTTP layer: routers, request/response schemas and dependencies."""

from fastapi import APIRouter

from hookrelay.api.routers import health

api_router = APIRouter()
api_router.include_router(health.router)

__all__ = ["api_router"]
