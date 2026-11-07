"""OpenTelemetry tracing helpers and W3C header injection."""

from __future__ import annotations

from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from hookrelay.observability.tracing import (
    TRACEPARENT_HEADER,
    attach_parent_from_headers,
    inject_trace_headers,
    setup_tracing,
    shutdown_tracing,
    span_context_headers,
    start_span,
)


def test_tracing_off_by_default_is_noop() -> None:
    shutdown_tracing()
    assert (
        setup_tracing(service_name="t", service_role="api", endpoint=None, enabled=False) is False
    )
    headers: dict[str, str] = {"content-type": "application/json"}
    inject_trace_headers(headers)
    assert "content-type" in headers
    shutdown_tracing()


def test_inject_trace_headers_from_active_span() -> None:
    shutdown_tracing()
    exporter = InMemorySpanExporter()
    assert setup_tracing(
        service_name="hookrelay-test",
        service_role="worker",
        endpoint=None,
        enabled=True,
        exporter=exporter,
    )
    headers: dict[str, str] = {"content-type": "application/json"}
    with start_span("delivery", attributes={"hookrelay.event_id": "e1"}):
        inject_trace_headers(headers)
        carrier = span_context_headers()
    assert TRACEPARENT_HEADER in headers
    assert headers[TRACEPARENT_HEADER].startswith("00-")
    assert carrier[TRACEPARENT_HEADER] == headers[TRACEPARENT_HEADER]
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "delivery"
    attrs = spans[0].attributes or {}
    assert attrs.get("hookrelay.event_id") == "e1"
    shutdown_tracing()


def test_traceparent_propagates_to_child() -> None:
    shutdown_tracing()
    exporter = InMemorySpanExporter()
    setup_tracing(
        service_name="hookrelay-test",
        service_role="api",
        endpoint=None,
        enabled=True,
        exporter=exporter,
    )
    with start_span("ingest"):
        carrier = span_context_headers()
    ctx = attach_parent_from_headers(carrier)
    with start_span("delivery", context=ctx):
        pass
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert "ingest" in spans
    assert "delivery" in spans
    assert spans["ingest"].context.trace_id == spans["delivery"].context.trace_id
    shutdown_tracing()
