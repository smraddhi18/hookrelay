"""Process wiring for metrics + tracing (API lifespan, relay, worker)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import structlog
from prometheus_client import start_http_server

from hookrelay.core.config import Settings
from hookrelay.observability.metrics import (
    SERVICE_API,
    Metrics,
    build_metrics,
    get_metrics,
)
from hookrelay.observability.tracing import setup_tracing, shutdown_tracing

logger = structlog.stdlib.get_logger("hookrelay.observability")


@dataclass(slots=True)
class Observability:
    """Bundle held on ``app.state.observability`` / relay / worker."""

    metrics: Metrics
    tracing: bool
    service: str


def build_observability(
    settings: Settings,
    *,
    service: str,
    metrics: Metrics | None = None,
    exporter: Any | None = None,
) -> Observability:
    """Build metrics (always) and optionally enable tracing from settings."""
    # Prefer an injected registry (tests); otherwise the process singleton for this role.
    if metrics is None:
        metrics = get_metrics(service)
    endpoint = settings.tracing_endpoint
    tracing = setup_tracing(
        service_name=settings.tracing_service_name or settings.app_name,
        service_role=service,
        endpoint=endpoint,
        sample_ratio=settings.tracing_sample_ratio,
        enabled=settings.tracing_enabled_effective if exporter is None else True,
        exporter=exporter,
    )
    return Observability(metrics=metrics, tracing=tracing, service=service)


def shutdown_observability() -> None:
    shutdown_tracing()


def metrics_http_server(settings: Settings, metrics: Metrics) -> None:
    """Start a side-car Prometheus HTTP server for relay/worker when METRICS_PORT is set."""
    port = settings.metrics_port
    if port is None or port <= 0:
        return
    start_http_server(port, registry=metrics.registry)
    logger.info("metrics.http_server", port=port, service=metrics.service)


# Re-export role constants for callers.
__all__ = [
    "SERVICE_API",
    "Metrics",
    "Observability",
    "build_metrics",
    "build_observability",
    "metrics_http_server",
    "shutdown_observability",
]
