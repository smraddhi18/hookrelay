"""Token-bucket script math against a live Redis (skip if unreachable)."""

from __future__ import annotations

import os
from uuid import uuid4

import pytest

from hookrelay.redis_.client import RedisConnectionSettings, RedisFacade, create_redis
from hookrelay.redis_.keys import redis_keys
from hookrelay.redis_.rate_limit import RedisRateLimiter

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")


async def _limiter() -> tuple[RedisRateLimiter, RedisFacade]:
    facade = create_redis(
        RedisConnectionSettings(
            url=REDIS_URL,
            connect_timeout_ms=500,
            command_timeout_ms=500,
            reconnect_base_ms=50,
            reconnect_max_ms=200,
        ),
        name="hookrelay:test:rl",
    )
    ok = await facade.ensure_connected()
    if not ok:
        await facade.close()
        pytest.skip("Redis not reachable")
    return RedisRateLimiter(facade, redis_keys("hookrelay-test")), facade


@pytest.mark.asyncio
async def test_token_bucket_allows_burst_then_rejects() -> None:
    limiter, facade = await _limiter()
    try:
        eid = uuid4()
        first = await limiter.take(eid, rate_per_s=2, n=2, burst=2)
        assert first is not None
        assert first.allowed == 2
        second = await limiter.take(eid, rate_per_s=2, n=1, burst=2)
        assert second is not None
        assert second.allowed == 0
        assert second.retry_after_ms >= 0
    finally:
        await facade.close()


@pytest.mark.asyncio
async def test_token_bucket_returns_none_when_closing() -> None:
    limiter, facade = await _limiter()
    await facade.close()
    assert await limiter.take(uuid4(), rate_per_s=10, n=1) is None
