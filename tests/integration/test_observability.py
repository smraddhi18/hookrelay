"""Milestone 7: dashboard API + metrics after a real delivery (Postgres)."""

from __future__ import annotations

from typing import Any

import httpx

from hookrelay.db.engine import Database
from hookrelay.observability.metrics import reset_metrics_for_tests
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
