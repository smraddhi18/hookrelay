from __future__ import annotations

from fastapi import FastAPI

from hookrelay import __version__
from hookrelay.redis_.client import RedisConnectionSettings, create_redis
from tests.conftest import FakeDatabase, serve


async def test_health_is_ok_without_touching_db(app_factory, fake_db: FakeDatabase) -> None:
    fake_db.ping_error = ConnectionRefusedError("db down")
    async with serve(app_factory()) as client:
        calls_after_startup = fake_db.ping_calls
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "service": "hookrelay", "version": __version__}
    assert fake_db.ping_calls == calls_after_startup


async def test_ready_when_postgres_up(app_factory) -> None:
    async with serve(app_factory()) as client:
        resp = await client.get("/ready")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["draining"] is False
    assert body["checks"]["postgres"]["status"] == "up"
    assert body["checks"]["postgres"]["error"] is None
    assert resp.headers["cache-control"] == "no-store"


async def test_ready_503_when_postgres_errors(app_factory) -> None:
    db = FakeDatabase(ping_error=OSError("connection refused to 10.0.0.5"))
    async with serve(app_factory(db)) as client:
        resp = await client.get("/ready")
    assert resp.status_code == 503
    pg = resp.json()["checks"]["postgres"]
    assert pg == {"status": "down", "latency_ms": pg["latency_ms"], "error": "OSError"}
    assert "10.0.0.5" not in resp.text


async def test_ready_503_on_timeout(app_factory) -> None:
    db = FakeDatabase(ping_delay_s=5)
    app: FastAPI = app_factory(db, readiness_timeout_s=0.05)
    async with serve(app) as client:
        resp = await client.get("/ready")
    assert resp.status_code == 503
    pg = resp.json()["checks"]["postgres"]
    assert pg["error"] == "timed out after 0.05s"
    assert pg["latency_ms"] < 2000


async def test_ready_503_while_draining(app_factory) -> None:
    app: FastAPI = app_factory()
    async with serve(app) as client:
        app.state.draining = True
        resp = await client.get("/ready")
    assert resp.status_code == 503
    body = resp.json()
    assert body["status"] == "not_ready"
    assert body["draining"] is True
    assert body["checks"]["postgres"]["status"] == "up"


async def test_ready_reports_redis_soft_when_configured(app_factory, fake_db: FakeDatabase) -> None:
    facade = create_redis(
        RedisConnectionSettings(
            url="redis://127.0.0.1:1/0",
            connect_timeout_ms=50,
            command_timeout_ms=50,
            reconnect_base_ms=50,
            reconnect_max_ms=100,
        ),
        name="hookrelay:test:ready",
    )

    class Bundle:
        command = facade

    app = app_factory(fake_db, redis_required_for_ready=False)
    async with serve(app) as client:
        app.state.redis = Bundle()
        # lifespan may have set redis already; force our broken one
        resp = await client.get("/ready")
    await facade.close()
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["checks"]["postgres"]["status"] == "up"
    assert body["checks"]["redis"]["status"] == "down"


async def test_ready_503_when_redis_required(app_factory, fake_db: FakeDatabase) -> None:
    facade = create_redis(
        RedisConnectionSettings(
            url="redis://127.0.0.1:1/0",
            connect_timeout_ms=50,
            command_timeout_ms=50,
            reconnect_base_ms=50,
            reconnect_max_ms=100,
        ),
        name="hookrelay:test:ready-hard",
    )

    class Bundle:
        command = facade

    app = app_factory(fake_db, redis_required_for_ready=True)
    async with serve(app) as client:
        app.state.redis = Bundle()
        resp = await client.get("/ready")
    await facade.close()
    assert resp.status_code == 503
    assert resp.json()["checks"]["redis"]["status"] == "down"
