"""Application factory.

Run with ``uvicorn --factory hookrelay.main:create_app`` (or ``python -m hookrelay``). Using a
factory instead of a module-level ``app`` means importing the package never reads the
environment or opens sockets, which keeps tests and tooling side-effect free.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

from hookrelay import __version__
from hookrelay.api import api_router
from hookrelay.core.config import Settings, get_settings
from hookrelay.core.errors import ErrorEnvelope, register_exception_handlers
from hookrelay.core.logging import configure_logging
from hookrelay.core.request_context import RequestContextMiddleware
from hookrelay.db.engine import Database

logger = structlog.stdlib.get_logger("hookrelay.app")


def create_app(settings: Settings | None = None, database: Database | None = None) -> FastAPI:
    """Build a fully wired FastAPI app.

    ``database`` may be injected (tests); otherwise the lifespan builds one from ``settings``.
    Either way the lifespan owns disposal so pooled connections are closed on shutdown.
    """
    settings = settings or get_settings()
    configure_logging(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        db = database or Database.from_settings(settings)
        app.state.database = db
        app.state.draining = False
        logger.info(
            "app.startup",
            env=settings.app_env.value,
            version=__version__,
            pool_size=settings.db_pool_size,
        )
        try:
            # Fail soft: a cold database should make /ready return 503, not crash-loop the pod.
            await db.ping(settings.readiness_timeout_s)
        except Exception as exc:
            logger.warning("app.startup.db_unreachable", error=type(exc).__name__)
        try:
            yield
        finally:
            app.state.draining = True
            logger.info("app.shutdown.begin")
            await db.dispose()
            logger.info("app.shutdown.complete")

    app = FastAPI(
        title="hookrelay",
        summary="Multi-tenant webhook delivery platform",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url="/redoc" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
        responses={
            422: {"model": ErrorEnvelope, "description": "Validation error"},
            500: {"model": ErrorEnvelope, "description": "Internal error"},
        },
    )
    app.state.settings = settings
    app.state.request_id_header = settings.request_id_header
    app.state.draining = False

    register_exception_handlers(app)
    app.add_middleware(RequestContextMiddleware, header_name=settings.request_id_header)
    app.include_router(api_router)
    return app
