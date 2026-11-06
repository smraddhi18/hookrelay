"""Observability: Prometheus metrics, OpenTelemetry tracing, process wiring (Milestone 7).

Metrics are always installed (prometheus-client is a hard dependency). The ``/metrics`` scrape
endpoint is unauthenticated by default so Prometheus can scrape it; set ``METRICS_TOKEN`` to
require ``Authorization: Bearer <token>``. The exposition never includes tenant payloads or
secrets — only counters, histograms and coarse gauges.

Tracing is off unless ``OTEL_EXPORTER_OTLP_ENDPOINT`` (or ``TRACING_OTLP_ENDPOINT``) is set, so
unit tests stay clean. When on, ingest → fan-out → delivery spans share a W3C trace via the
``events.traceparent`` / ``events.tracestate`` columns, and the worker injects those headers on
outbound webhooks (they are deny-listed as tenant custom headers so only hookrelay may set them).
"""

from __future__ import annotations

from hookrelay.observability.metrics import Metrics, get_metrics, render_metrics
from hookrelay.observability.runtime import (
    Observability,
    build_observability,
    shutdown_observability,
)
from hookrelay.observability.tracing import (
    TRACEPARENT_HEADER,
    TRACESTATE_HEADER,
    inject_trace_headers,
    setup_tracing,
    shutdown_tracing,
    span_context_headers,
    start_span,
)

__all__ = [
    "TRACEPARENT_HEADER",
    "TRACESTATE_HEADER",
    "Metrics",
    "Observability",
    "build_observability",
    "get_metrics",
    "inject_trace_headers",
    "render_metrics",
    "setup_tracing",
    "shutdown_observability",
    "shutdown_tracing",
    "span_context_headers",
    "start_span",
]
