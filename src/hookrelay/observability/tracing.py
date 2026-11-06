"""OpenTelemetry tracing: optional OTLP export and W3C header propagation.

Tracing is **off** when neither ``OTEL_EXPORTER_OTLP_ENDPOINT`` nor ``TRACING_OTLP_ENDPOINT``
is set (and ``TRACING_ENABLED`` is not forced on). Unit tests therefore never open network
sockets or mutate a global TracerProvider unless they opt in.

Cross-process continuity (API → relay → worker) uses the W3C ``traceparent`` / ``tracestate``
columns on ``events``; the worker injects those headers on outbound webhooks. Tenants cannot
set them as custom headers (they are on the deny-list in ``endpoint_rules``).
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from contextlib import contextmanager
from typing import Any, Final

import structlog
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter
from opentelemetry.sdk.trace.sampling import ParentBasedTraceIdRatio
from opentelemetry.trace import Span, Status, StatusCode, Tracer
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = structlog.stdlib.get_logger("hookrelay.tracing")

TRACEPARENT_HEADER: Final = "traceparent"
TRACESTATE_HEADER: Final = "tracestate"

_propagator = TraceContextTextMapPropagator()
_provider: TracerProvider | None = None
_enabled: bool = False


def tracing_enabled() -> bool:
    return _enabled


def setup_tracing(
    *,
    service_name: str,
    service_role: str,
    endpoint: str | None,
    sample_ratio: float = 1.0,
    enabled: bool | None = None,
    exporter: SpanExporter | None = None,
) -> bool:
    """Install a process-wide TracerProvider. Returns whether tracing is active.

    ``enabled`` defaults to "on when ``endpoint`` or a custom ``exporter`` is set". Pass
    ``enabled=False`` to force off even with an endpoint (tests).
    """
    global _provider, _enabled  # noqa: PLW0603
    active = bool(enabled) if enabled is not None else bool(endpoint or exporter is not None)
    if not active:
        # Ensure a previous test's provider cannot leak into tracing-off runs.
        if _provider is not None or _enabled:
            shutdown_tracing()
        else:
            _enabled = False
        logger.info("tracing.disabled")
        return False

    if _provider is not None:
        # Already configured in this process (e.g. create_app called twice in tests).
        return _enabled

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": "hookrelay",
            "hookrelay.service": service_role,
        }
    )
    ratio = max(0.0, min(1.0, sample_ratio))
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBasedTraceIdRatio(ratio),
    )
    if exporter is None:
        if not endpoint:
            raise ValueError("tracing endpoint is required when no custom exporter is provided")
        exporter = OTLPSpanExporter(endpoint=_otlp_traces_url(endpoint))
        provider.add_span_processor(BatchSpanProcessor(exporter))
    else:
        # Tests: export immediately so assertions see finished spans.
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _provider = provider
    _enabled = True
    logger.info(
        "tracing.enabled",
        service=service_name,
        role=service_role,
        endpoint=endpoint,
        sample_ratio=ratio,
    )
    return True


def _otlp_traces_url(endpoint: str) -> str:
    """Accept a base (``http://jaeger:4318``) or a full traces path."""
    base = endpoint.rstrip("/")
    if base.endswith("/v1/traces"):
        return base
    return f"{base}/v1/traces"


def shutdown_tracing() -> None:
    """Flush and shut down the process TracerProvider (no-op when tracing was never set up).

    Also clears OpenTelemetry's process-wide provider so a later :func:`setup_tracing` can
    install a new one (needed by unit tests; production processes set up once).
    """
    global _provider, _enabled  # noqa: PLW0603
    provider = _provider
    _provider = None
    _enabled = False
    if provider is not None:
        provider.shutdown()
    # OTEL only allows set_tracer_provider once unless the Once flag is cleared.
    trace._TRACER_PROVIDER = None
    once = getattr(trace, "_TRACER_PROVIDER_SET_ONCE", None)
    if once is not None and hasattr(once, "_done"):
        once._done = False


def get_tracer(name: str = "hookrelay") -> Tracer:
    return trace.get_tracer(name)


def extract_context(headers: Mapping[str, str]) -> Context:
    """Extract a W3C context from inbound HTTP headers (case-insensitive keys)."""
    lowered = {k.lower(): v for k, v in headers.items()}
    return _propagator.extract(lowered)


def span_context_headers(context: Context | None = None) -> dict[str, str]:
    """Serialise the current (or given) context to ``traceparent`` / ``tracestate``."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier, context=context)
    return carrier


def inject_trace_headers(
    headers: MutableMapping[str, str], *, context: Context | None = None
) -> None:
    """Add / overwrite W3C trace headers on an outbound header map (webhook delivery)."""
    carrier: dict[str, str] = {}
    _propagator.inject(carrier, context=context)
    for key, value in carrier.items():
        headers[key] = value


@contextmanager
def start_span(
    name: str,
    *,
    kind: trace.SpanKind = trace.SpanKind.INTERNAL,
    attributes: Mapping[str, Any] | None = None,
    context: Context | None = None,
    links: list[trace.Link] | None = None,
) -> Iterator[Span]:
    """Start a span when tracing is on; yield a no-op span otherwise."""
    tracer = get_tracer()
    with tracer.start_as_current_span(
        name,
        context=context,
        kind=kind,
        attributes=dict(attributes or {}),
        links=links or (),
    ) as span:
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


def attach_parent_from_headers(headers: Mapping[str, str | None]) -> Context | None:
    """Build a parent context from stored ``traceparent`` / ``tracestate``, or None."""
    carrier = {
        TRACEPARENT_HEADER: headers.get(TRACEPARENT_HEADER) or headers.get("traceparent"),
        TRACESTATE_HEADER: headers.get(TRACESTATE_HEADER) or headers.get("tracestate"),
    }
    carrier = {k: v for k, v in carrier.items() if v}
    if TRACEPARENT_HEADER not in carrier:
        return None
    return _propagator.extract(carrier)
