"""Milestone 7: dashboard API + metrics after a real delivery (Postgres)."""

from __future__ import annotations

from typing import Any

import httpx
from sqlalchemy import text

from hookrelay.db.engine import Database
from hookrelay.observability.gauges import refresh_gauges
from hookrelay.observability.metrics import (
    SERVICE_RELAY,
    build_metrics,
    render_metrics,
    reset_metrics_for_tests,
)
from hookrelay.relay import Relay
from tests.integration.conftest import (
    create_tenant,
    publish,
    register_event_type,
    subscribe,
)
from tests.integration.test_worker import (  # noqa: F401
    api,
    database,
    db_url,
    make_worker,
    receiver,
    settings_for,
    worker_template,
)


async def _fan_out(settings: Any) -> None:
    db = Database.from_settings(settings)
    try:
        relay = Relay(db, settings)
        while (await relay.run_once()).claimed:
            pass
    finally:
        await db.dispose()


async def test_dashboard_and_metrics_after_delivery(
    api: httpx.AsyncClient,
    settings_for: Any,
    make_worker: Any,
    receiver: Any,
) -> None:
    reset_metrics_for_tests()
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, "invoice.paid")
    created = await api.post(
        "/v1/endpoints",
        json={"url": f"http://receiver.test:{receiver.port}/hook"},
        headers=tenant.headers,
    )
    assert created.status_code == 201, created.text
    endpoint_id = str(created.json()["id"])
    await subscribe(api, tenant, endpoint_id, "invoice.*")

    assert (await publish(api, tenant, "invoice.paid", {"n": 1})).status_code == 202
    await _fan_out(settings_for())

    await make_worker().run_once()

    dash = await api.get("/v1/dashboard", headers=tenant.headers)
    assert dash.status_code == 200, dash.text
    body = dash.json()
    assert body["attempts"] >= 1
    assert body["succeeded"] >= 1
    assert body["success_rate"] is not None
    assert body["tenant_id"] == tenant.id
    assert any(e["endpoint_id"] == endpoint_id for e in body["endpoints"])

    huge = await api.get("/v1/dashboard", params={"window_s": 10**9}, headers=tenant.headers)
    assert huge.status_code == 422
    assert huge.json()["error"]["code"] == "window_too_large"

    anon = await api.get("/v1/dashboard")
    assert anon.status_code == 401

    metrics = await api.get("/metrics")
    assert metrics.status_code == 200
    assert "hookrelay_" in metrics.text


async def test_queue_gauges_split_due_from_scheduled_and_breaker_blocked(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Any,
    database: Database,
) -> None:
    """``hookrelay_deliveries_due`` (the KEDA signal) counts only work a worker could lease now."""
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, "order.created")
    endpoint_ids: list[str] = []
    for name in ("due", "later", "open"):
        created = await api.post(
            "/v1/endpoints",
            json={"url": f"http://receiver.test:{receiver.port}/{name}"},
            headers=tenant.headers,
        )
        assert created.status_code == 201, created.text
        endpoint_ids.append(str(created.json()["id"]))
        await subscribe(api, tenant, endpoint_ids[-1], "order.*")
    for n in range(2):
        assert (await publish(api, tenant, "order.created", {"n": n})).status_code == 202
    await _fan_out(settings_for())
    _, later, blocked = endpoint_ids

    async with database.session() as session:
        await session.execute(
            text(
                "UPDATE deliveries SET next_attempt_at = now() + interval '1 hour' "
                "WHERE endpoint_id = :e"
            ),
            {"e": later},
        )
        await session.execute(
            text(
                "INSERT INTO endpoint_states (endpoint_id, tenant_id, breaker_state, open_until, "
                "opened_at, trips) VALUES (:e, :t, 'open', now() + interval '1 hour', now(), 1) "
                "ON CONFLICT (endpoint_id) DO UPDATE SET breaker_state = 'open', "
                "open_until = EXCLUDED.open_until, opened_at = EXCLUDED.opened_at, trips = 1"
            ),
            {"e": blocked, "t": tenant.id},
        )

    metrics = build_metrics(SERVICE_RELAY)
    async with database.session() as session:
        snap = await refresh_gauges(session, metrics)
    assert snap.deliveries_pending == 6
    assert snap.deliveries_due == 2  # the "later" retries and the open-breaker backlog excluded
    assert snap.breaker_open == 1
    body = render_metrics(metrics)[0].decode()
    assert 'hookrelay_deliveries_due{service="relay"} 2.0' in body
    assert 'hookrelay_deliveries_pending{service="relay"} 6.0' in body

    # Once the cooldown has passed, the backlog is leasable again (half-open probe) and counts.
    async with database.session() as session:
        await session.execute(
            text(
                "UPDATE endpoint_states SET open_until = now() - interval '1 second' "
                "WHERE breaker_state = 'open'"
            )
        )
        snap = await refresh_gauges(session, metrics)
    assert snap.deliveries_due == 4
