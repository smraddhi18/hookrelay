"""Subscription cache against live Redis (skip if unreachable)."""

from __future__ import annotations

import os
from uuid import UUID, uuid4

import pytest

from hookrelay.redis_.client import RedisConnectionSettings, create_redis
from hookrelay.redis_.keys import redis_keys
from hookrelay.redis_.subscription_cache import SubscriptionCache

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")


@pytest.mark.asyncio
async def test_cache_set_get_invalidate() -> None:
    facade = create_redis(
        RedisConnectionSettings(
            url=REDIS_URL,
            connect_timeout_ms=500,
            command_timeout_ms=500,
            reconnect_base_ms=50,
            reconnect_max_ms=200,
        ),
        name="hookrelay:test:cache",
    )
    if not await facade.ensure_connected():
        await facade.close()
        pytest.skip("Redis not reachable")
    cache = SubscriptionCache(facade, redis_keys("hookrelay-test"), ttl_s=30)
    tenant = uuid4()
    endpoints = [uuid4(), uuid4()]
    try:
        assert await cache.get(tenant, "invoice.paid") is None
        await cache.set(tenant, "invoice.paid", endpoints)
        got = await cache.get(tenant, "invoice.paid")
        assert got == endpoints

        loads = {"n": 0}

        async def loader(tid: UUID, et: str) -> list[UUID]:
            loads["n"] += 1
            return endpoints

        again = await cache.get_or_load(tenant, "invoice.paid", loader)
        assert again == endpoints
        assert loads["n"] == 0  # served from cache

        await cache.invalidate_tenant(tenant)
        assert await cache.get(tenant, "invoice.paid") is None

        loaded = await cache.get_or_load(tenant, "invoice.paid", loader)
        assert loaded == endpoints
        assert loads["n"] == 1
    finally:
        await cache.invalidate_tenant(tenant)
        await facade.close()
