from __future__ import annotations

from hookrelay.main import create_app
from tests.conftest import FakeDatabase, make_settings, serve


async def test_lifespan_disposes_database(fake_db: FakeDatabase) -> None:
    app = create_app(make_settings(), database=fake_db.as_database())
    async with serve(app) as client:
        assert app.state.database is fake_db
        assert (await client.get("/health")).status_code == 200
        assert not fake_db.disposed
    assert fake_db.disposed
    assert app.state.draining is True


async def test_startup_survives_unreachable_database() -> None:
    db = FakeDatabase(ping_error=ConnectionRefusedError())
    app = create_app(make_settings(), database=db.as_database())
    async with serve(app) as client:
        assert (await client.get("/health")).status_code == 200
        assert (await client.get("/ready")).status_code == 503
    assert db.ping_calls == 2
    assert db.disposed


async def test_openapi_lists_operational_endpoints(app_factory) -> None:
    async with serve(app_factory()) as client:
        spec = (await client.get("/openapi.json")).json()
        docs = await client.get("/docs")
    assert {"/health", "/ready"} <= set(spec["paths"])
    assert "ErrorEnvelope" in spec["components"]["schemas"]
    assert docs.status_code == 200


async def test_docs_can_be_disabled(app_factory) -> None:
    async with serve(app_factory(docs_enabled=False)) as client:
        assert (await client.get("/docs")).status_code == 404
        assert (await client.get("/openapi.json")).status_code == 404
