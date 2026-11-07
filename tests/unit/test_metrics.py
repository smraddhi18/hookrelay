"""Prometheus metrics registry and /metrics scrape endpoint."""

from __future__ import annotations

from hookrelay.main import create_app
from hookrelay.observability.metrics import (
    SERVICE_API,
    build_metrics,
    render_metrics,
    reset_metrics_for_tests,
)
from tests.conftest import FakeDatabase, make_settings, serve


def test_metrics_record_and_render() -> None:
    reset_metrics_for_tests()
    m = build_metrics(SERVICE_API)
    m.record_event_ingested()
    m.record_event_ingested()
    m.record_delivery_attempt(outcome="succeeded", latency_ms=42)
    m.record_delivery_attempt(outcome="failed", latency_ms=None)
    m.set_queue_depth(
        outbox_pending=3,
        outbox_lag_seconds=1.5,
        deliveries_pending=7,
        deliveries_delivering=2,
    )
    m.set_breaker_counts(closed=4, open_=1, half_open=0)
    body, content_type = render_metrics(m)
    text = body.decode()
    assert "text/plain" in content_type or "openmetrics" in content_type
    assert "hookrelay_events_ingested_total" in text
    assert 'service="api"' in text
    assert "hookrelay_delivery_attempts_total" in text
    assert "hookrelay_outbox_pending" in text
    assert "hookrelay_circuit_breaker_endpoints" in text


async def test_metrics_endpoint_is_public(app_factory) -> None:
    reset_metrics_for_tests()
    app = app_factory()
    async with serve(app) as client:
        assert getattr(app.state, "observability", None) is not None
        app.state.observability.metrics.record_event_ingested()
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "hookrelay_events_ingested_total" in resp.text


async def test_metrics_token_required() -> None:
    reset_metrics_for_tests()
    settings = make_settings(metrics_token="test-only-EXAMPLE-metrics-token")
    app = create_app(settings, database=FakeDatabase().as_database())
    async with serve(app) as client:
        denied = await client.get("/metrics")
        assert denied.status_code == 401
        ok = await client.get(
            "/metrics",
            headers={"Authorization": "Bearer test-only-EXAMPLE-metrics-token"},
        )
        assert ok.status_code == 200


async def test_metrics_disabled_returns_404() -> None:
    reset_metrics_for_tests()
    app = create_app(make_settings(metrics_enabled=False), database=FakeDatabase().as_database())
    async with serve(app) as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 404
