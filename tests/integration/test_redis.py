"""Milestone 6 Redis integration: wake-ups, subscription cache, rate limits, degraded mode."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from alembic import command

from hookrelay.db.engine import Database
from hookrelay.redis_ import build_redis_bundle, build_wakeup_subscriber
from hookrelay.redis_.client import RedisConnectionSettings, create_redis
from hookrelay.relay import Relay
from hookrelay.services import fanout
from tests.conftest import TEST_ENCRYPTION_KEY, make_settings
from tests.integration.conftest import (
    alembic_config,
    api_client,
    create_endpoint,
    create_scratch_database,
    create_tenant,
    drop_scratch_database,
    publish,
    register_event_type,
    subscribe,
)

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")


@pytest.fixture(scope="module")
def redis_ok() -> None:
    async def _ping() -> bool:
        facade = create_redis(
            RedisConnectionSettings(
                url=REDIS_URL,
                connect_timeout_ms=500,
                command_timeout_ms=500,
                reconnect_base_ms=50,
                reconnect_max_ms=200,
            ),
            name="hookrelay:it:ping",
        )
        try:
            return await facade.ensure_connected()
        finally:
            await facade.close()

    if not asyncio.run(_ping()):
        pytest.skip("REDIS_URL not reachable")


@pytest.fixture
def db_url(redis_ok: None) -> Iterator[str]:
    name, url = create_scratch_database("hookrelay_m6")
    try:
        command.upgrade(alembic_config(url), "head")
        yield url
    finally:
        drop_scratch_database(name)


@pytest.fixture
async def database(db_url: str) -> AsyncIterator[Database]:
    db = Database.from_settings(
        make_settings(database_url=db_url, secrets_encryption_key=TEST_ENCRYPTION_KEY)
    )
    try:
        yield db
    finally:
        await db.dispose()


def _settings(db_url: str, tmp_path: Path, **overrides: Any):
    base = {
        "database_url": db_url,
        "secrets_encryption_key": TEST_ENCRYPTION_KEY,
        "redis_url": REDIS_URL,
        "redis_key_prefix": f"hr-m6-{uuid.uuid4().hex[:8]}",
        "wakeup_coalesce_ms": 0,
        "subscription_cache_ttl_s": 60,
        "endpoint_allow_http": True,
        "endpoint_allow_private_networks": True,
        "delivery_allow_private_networks": True,
        "worker_poll_min_ms": 50,
        "worker_poll_max_ms": 2_000,
        "worker_concurrency": 4,
        "worker_heartbeat_file": str(tmp_path / "worker.hb"),
        "relay_heartbeat_file": str(tmp_path / "relay.hb"),
    }
    base.update(overrides)
    return make_settings(**base)


@pytest.mark.asyncio
async def test_wakeup_short_circuits_idle_delay(
    database: Database, db_url: str, tmp_path: Path
) -> None:
    settings = _settings(db_url, tmp_path)
    redis = build_redis_bundle(settings, role="relay", source="relay")
    assert redis.publisher is not None
    woke = asyncio.Event()
    listening = asyncio.Event()
    sub = build_wakeup_subscriber(
        settings,
        instance_id="it-worker",
        on_wake=woke.set,
        on_listening_change=lambda ok: listening.set() if ok else None,
    )
    assert sub is not None
    try:
        await sub.start()
        await asyncio.wait_for(listening.wait(), timeout=2)
        # Simulate idle wait interrupted by a wake
        wait_task = asyncio.create_task(asyncio.wait_for(woke.wait(), timeout=5))
        await asyncio.sleep(0.05)
        redis.publisher.notify()
        await redis.publisher.flush()
        await wait_task
    finally:
        await sub.close()
        await redis.close()


@pytest.mark.asyncio
async def test_subscription_cache_matches_sql_and_invalidates(
    database: Database, db_url: str, tmp_path: Path
) -> None:
    settings = _settings(db_url, tmp_path)
    async with api_client(
        db_url, redis_url=REDIS_URL, redis_key_prefix=settings.redis_key_prefix
    ) as client:
        tenant = await create_tenant(client)
        await register_event_type(client, tenant, "invoice.paid")
        ep_a = await create_endpoint(client, tenant, "a")
        ep_b = await create_endpoint(client, tenant, "b")
        await subscribe(client, tenant, ep_a, "invoice.paid")
        await subscribe(client, tenant, ep_b, "invoice.*")

        redis = build_redis_bundle(settings, role="relay", source="relay")
        assert redis.cache is not None
        try:
            async with database.session() as session:
                sql_ids = await fanout.matching_endpoint_ids(
                    session, UUID(tenant.id), "invoice.paid"
                )
            assert set(sql_ids) == {UUID(ep_a), UUID(ep_b)}

            loads = {"n": 0}

            async def loader(tid: UUID, et: str) -> list[UUID]:
                loads["n"] += 1
                async with database.session() as session:
                    return await fanout.matching_endpoint_ids(session, tid, et)

            first = await redis.cache.get_or_load(UUID(tenant.id), "invoice.paid", loader)
            assert set(first) == set(sql_ids)
            assert loads["n"] == 1
            second = await redis.cache.get_or_load(UUID(tenant.id), "invoice.paid", loader)
            assert second == first
            assert loads["n"] == 1

            # Disable endpoint a via API → invalidation → cache miss reflects new membership
            etag = (await client.get(f"/v1/endpoints/{ep_a}", headers=tenant.headers)).headers[
                "etag"
            ]
            patched = await client.patch(
                f"/v1/endpoints/{ep_a}",
                json={"enabled": False},
                headers={**tenant.headers, "If-Match": etag},
            )
            assert patched.status_code == 200, patched.text

            # After invalidation the key is gone
            assert await redis.cache.get(UUID(tenant.id), "invoice.paid") is None
            third = await redis.cache.get_or_load(UUID(tenant.id), "invoice.paid", loader)
            assert set(third) == {UUID(ep_b)}
            assert loads["n"] == 2
        finally:
            await redis.close()


@pytest.mark.asyncio
async def test_redis_rate_limit_enforced_and_pg_fallback(
    database: Database, db_url: str, tmp_path: Path
) -> None:
    settings = _settings(db_url, tmp_path)
    redis = build_redis_bundle(settings, role="worker", source="worker")
    assert redis.rate_limiter is not None
    eid = uuid.uuid4()
    try:
        # Burst of 1 at 1/s
        a = await redis.rate_limiter.take(eid, rate_per_s=1, n=1, burst=1)
        b = await redis.rate_limiter.take(eid, rate_per_s=1, n=1, burst=1)
        assert a is not None
        assert a.allowed == 1
        assert b is not None
        assert b.allowed == 0

        # Closing the client → take returns None (caller must fall back to PG)
        await redis.command.close()  # type: ignore[union-attr]
        assert await redis.rate_limiter.take(eid, rate_per_s=1, n=1) is None
    finally:
        await redis.close()


@pytest.mark.asyncio
async def test_ready_reports_redis_soft(db_url: str, redis_ok: None, tmp_path: Path) -> None:
    async with api_client(db_url, redis_url=REDIS_URL) as client:
        resp = await client.get("/ready")
        assert resp.status_code == 200
        body = resp.json()
        assert body["checks"]["postgres"]["status"] == "up"
        assert body["checks"]["redis"]["status"] == "up"

    # Soft: redis required false + bogus URL still ready (redis down reported)
    async with api_client(
        db_url,
        redis_url="redis://127.0.0.1:1/0",
        redis_connect_timeout_ms=100,
        redis_command_timeout_ms=100,
        redis_required_for_ready=False,
    ) as client:
        resp = await client.get("/ready")
        assert resp.status_code == 200
        assert resp.json()["checks"]["redis"]["status"] == "down"

    # Hard: redis required → 503
    async with api_client(
        db_url,
        redis_url="redis://127.0.0.1:1/0",
        redis_connect_timeout_ms=100,
        redis_command_timeout_ms=100,
        redis_required_for_ready=True,
    ) as client:
        resp = await client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["checks"]["redis"]["status"] == "down"


@pytest.mark.asyncio
async def test_fan_out_cached_inserts_same_as_sql(
    database: Database, db_url: str, tmp_path: Path
) -> None:
    settings = _settings(db_url, tmp_path)
    async with api_client(db_url) as client:
        tenant = await create_tenant(client)
        await register_event_type(client, tenant, "order.created")
        ep = await create_endpoint(client, tenant, "fan")
        await subscribe(client, tenant, ep, "order.created")
        resp = await publish(client, tenant, "order.created", {"x": 1})
        assert resp.status_code == 202

    redis = build_redis_bundle(settings, role="relay", source="relay")
    relay = Relay(database, settings, redis=redis)
    try:
        result = await relay.run_once()
        assert result.deliveries_created == 1
        # Second pass should create 0 (ON CONFLICT)
        again = await relay.run_once()
        assert again.deliveries_created == 0
    finally:
        await redis.close()
