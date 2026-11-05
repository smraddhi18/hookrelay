"""Mock-based unit coverage for redis_ client, cache, rate limit, and wakeup edge paths."""

# mypy: disable-error-code="method-assign,unreachable"

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from hookrelay.db.models import BreakerState
from hookrelay.redis_.client import (
    RedisConnectionSettings,
    RedisFacade,
    close_redis,
    create_redis,
    is_redis_ready,
    ping_redis,
    reconnect_delay_ms,
)
from hookrelay.redis_.keys import redis_keys
from hookrelay.redis_.rate_limit import RateLimitResult, RedisRateLimiter, parse_bucket_reply
from hookrelay.redis_.subscription_cache import SubscriptionCache
from hookrelay.redis_.wakeup import WakeupPublisher, WakeupSubscriber, decode_wakeup
from hookrelay.services.delivery_queue import EndpointSlot, _apply_redis_rate_limits


async def _async_iter(items: list[Any]):
    for item in items:
        yield item


def _facade(*, closing: bool = False, connected: bool = True) -> RedisFacade:
    client = MagicMock()
    client.ping = AsyncMock(return_value=True)
    client.aclose = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.set = AsyncMock(return_value=True)
    client.delete = AsyncMock(return_value=1)
    client.publish = AsyncMock(return_value=1)
    client.scan_iter = MagicMock(return_value=_async_iter([]))
    client.register_script = MagicMock(return_value=AsyncMock(return_value=[1, 0, 0]))
    client.pubsub = MagicMock()
    facade = RedisFacade(client, name="hookrelay:test:mock")
    if connected:
        facade.mark_connected()
    if closing:
        facade._closing = True
        facade._connected = False
    return facade


def test_reconnect_delay_attempt_zero() -> None:
    assert reconnect_delay_ms(0, 100, 5_000, random_fn=lambda: 0.0) == 50


def test_is_redis_ready_none() -> None:
    assert not is_redis_ready(None)
    facade = _facade(connected=False)
    assert not is_redis_ready(facade)
    facade.mark_connected()
    assert is_redis_ready(facade)


@pytest.mark.asyncio
async def test_facade_mark_disconnected_and_note_error_throttle() -> None:
    facade = _facade()
    assert facade.raw is facade.client
    assert facade.is_ready
    facade.mark_disconnected()
    assert not facade.is_ready
    # Second call is a deliberate no-op when already down (covers the early return).
    facade.mark_disconnected()
    facade.note_reconnect(25)
    facade.note_error(RuntimeError("boom"))
    facade.note_error(RuntimeError("boom2"))  # throttled
    facade.mark_connected()
    assert facade.is_ready


@pytest.mark.asyncio
async def test_ensure_connected_closing_and_failures() -> None:
    closing = _facade(closing=True)
    assert await closing.ensure_connected() is False

    facade = _facade(connected=False)
    facade.client.ping = AsyncMock(side_effect=RedisConnectionError("down"))
    assert await facade.ensure_connected() is False

    facade.client.ping = AsyncMock(return_value=False)
    assert await facade.ensure_connected() is False

    facade.client.ping = AsyncMock(return_value=True)
    assert await facade.ensure_connected() is True


@pytest.mark.asyncio
async def test_close_and_ping_redis() -> None:
    facade = _facade()
    await close_redis(None)
    await facade.close()
    await facade.close()  # idempotent
    with pytest.raises(RuntimeError, match="closing"):
        await ping_redis(facade, timeout_s=0.1)

    live = _facade()
    live.client.ping = AsyncMock(return_value=False)
    with pytest.raises(RuntimeError, match="falsy"):
        await ping_redis(live, timeout_s=0.1)

    live.client.ping = AsyncMock(return_value=True)
    await ping_redis(live, timeout_s=0.1)


@pytest.mark.asyncio
async def test_close_falls_back_to_pool_disconnect() -> None:
    facade = _facade()
    facade.client.aclose = AsyncMock(side_effect=RuntimeError("boom"))
    facade.client.connection_pool = MagicMock()
    facade.client.connection_pool.disconnect = AsyncMock()
    await facade.close()
    facade.client.connection_pool.disconnect.assert_awaited()


def test_create_redis_builds_facade() -> None:
    settings = RedisConnectionSettings(
        url="redis://127.0.0.1:6379/0",
        connect_timeout_ms=100,
        command_timeout_ms=100,
        reconnect_base_ms=50,
        reconnect_max_ms=200,
    )
    mocked = MagicMock()
    with patch("hookrelay.redis_.client.redis_async.from_url", return_value=mocked) as from_url:
        facade = create_redis(settings, name="hookrelay:test:build")
        assert facade.name == "hookrelay:test:build"
        from_url.assert_called_once()


def test_parse_bucket_reply_bad_item() -> None:
    with pytest.raises(ValueError, match="Unexpected"):
        parse_bucket_reply([1, "x", 3])


@pytest.mark.asyncio
async def test_rate_limiter_edge_paths() -> None:
    facade = _facade()
    limiter = RedisRateLimiter(facade, redis_keys("t"))
    assert limiter.is_available()
    zero = await limiter.take(uuid4(), rate_per_s=10, n=0)
    assert zero is not None
    assert zero.allowed == 0

    facade._closing = True
    assert await limiter.take(uuid4(), rate_per_s=10, n=1) is None
    assert not limiter.is_available()

    live = _facade()
    script = AsyncMock(side_effect=RedisConnectionError("down"))
    live.client.register_script = MagicMock(return_value=script)
    limiter2 = RedisRateLimiter(live, redis_keys("t"))
    assert await limiter2.take(uuid4(), rate_per_s=10, n=1) is None

    script2 = AsyncMock(return_value=[1, 2])  # bad length → ValueError
    live2 = _facade()
    live2.client.register_script = MagicMock(return_value=script2)
    limiter3 = RedisRateLimiter(live2, redis_keys("t"))
    assert await limiter3.take(uuid4(), rate_per_s=10, n=1) is None


@pytest.mark.asyncio
async def test_subscription_cache_disabled_and_bad_payload() -> None:
    facade = _facade()
    cache = SubscriptionCache(facade, redis_keys("t"), ttl_s=30, enabled=False)
    assert await cache.get(uuid4(), "a.b") is None
    await cache.set(uuid4(), "a.b", [uuid4()])
    assert await cache.invalidate_tenant(uuid4()) == 0
    await cache.invalidate_event_type(uuid4(), "a.b")

    live = _facade()
    live.client.get = AsyncMock(return_value='{"not":"a-list"}')
    cache2 = SubscriptionCache(live, redis_keys("t"), ttl_s=30)
    assert await cache2.get(uuid4(), "a.b") is None

    live.client.get = AsyncMock(return_value="not-json")
    assert await cache2.get(uuid4(), "a.b") is None

    live.client.get = AsyncMock(side_effect=RedisConnectionError("down"))
    assert await cache2.get(uuid4(), "a.b") is None

    live.client.set = AsyncMock(side_effect=RedisConnectionError("down"))
    await cache2.set(uuid4(), "a.b", [])  # negative cache path + error

    live.client.scan_iter = MagicMock(return_value=_async_iter(["k1"]))
    live.client.delete = AsyncMock(side_effect=RedisConnectionError("down"))
    assert await cache2.invalidate_tenant(uuid4()) == 0

    live.client.delete = AsyncMock(side_effect=RedisConnectionError("down"))
    await cache2.invalidate_event_type(uuid4(), "a.b")


@pytest.mark.asyncio
async def test_subscription_cache_get_or_load_stampede_and_disabled() -> None:
    facade = _facade(closing=True)
    cache = SubscriptionCache(facade, redis_keys("t"), ttl_s=30)
    loads = {"n": 0}

    async def loader(_tid: UUID, _et: str) -> list[UUID]:
        loads["n"] += 1
        return [uuid4()]

    result = await cache.get_or_load(uuid4(), "x.y", loader)
    assert len(result) == 1
    assert loads["n"] == 1

    live = _facade()
    live.client.get = AsyncMock(return_value=None)
    # First SET NX returns False → wait and re-get still miss → load
    live.client.set = AsyncMock(side_effect=[False, True])
    live.client.delete = AsyncMock()
    cache2 = SubscriptionCache(live, redis_keys("t"), ttl_s=30, lock_ttl_s=0.1)

    async def loader2(_tid: UUID, _et: str) -> list[UUID]:
        return []

    with patch("hookrelay.redis_.subscription_cache.asyncio.sleep", new=AsyncMock()):
        empty = await cache2.get_or_load(uuid4(), "z.z", loader2)
    assert empty == []


@pytest.mark.asyncio
async def test_wakeup_publisher_closing_and_failure() -> None:
    facade = _facade(closing=True)
    pub = WakeupPublisher(facade, redis_keys("t"), coalesce_ms=0, source="test")
    pub.notify()
    await pub.flush()

    live = _facade()
    live.client.publish = AsyncMock(side_effect=RedisConnectionError("down"))
    pub2 = WakeupPublisher(live, redis_keys("t"), coalesce_ms=0, source="test")
    pub2.notify()
    await pub2.flush()
    pub2._log_failure("deliveries", RuntimeError("x"))
    pub2._log_failure("deliveries", RuntimeError("y"))  # throttled


@pytest.mark.asyncio
async def test_wakeup_subscriber_start_close_and_decode() -> None:
    assert decode_wakeup("null") == {}
    facade = _facade()
    pubsub = MagicMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.aclose = AsyncMock()
    pubsub.get_message = AsyncMock(
        side_effect=[
            {"type": "pmessage"},
            {"type": "message", "data": '{"source":"relay","sentAt":1}'},
            RedisConnectionError("drop"),
        ]
    )
    facade.client.pubsub = MagicMock(return_value=pubsub)
    facade.client.ping = AsyncMock(return_value=True)

    wakes: list[int] = []
    listening_states: list[bool] = []
    sub = WakeupSubscriber(
        facade,
        redis_keys("t"),
        on_wake=lambda: wakes.append(1),
        on_listening_change=listening_states.append,
        reconnect_base_ms=10,
        reconnect_max_ms=20,
    )
    await sub.start()
    with pytest.raises(RuntimeError, match="already started"):
        await sub.start()
    await asyncio.sleep(0.05)
    await sub.close()
    await sub.close()  # idempotent
    assert sub.is_listening is False


@pytest.mark.asyncio
async def test_apply_redis_rate_limits_paths() -> None:
    eid_unlimited = uuid4()
    eid_ok = uuid4()
    eid_degraded = uuid4()
    eid_zero = uuid4()
    slots = [
        EndpointSlot(
            endpoint_id=eid_unlimited,
            breaker_state=BreakerState.CLOSED,
            capacity=5,
            tokens=None,
            due=1,
            rate_limit_per_s=None,
        ),
        EndpointSlot(
            endpoint_id=eid_ok,
            breaker_state=BreakerState.CLOSED,
            capacity=5,
            tokens=3.0,
            due=2,
            rate_limit_per_s=10,
        ),
        EndpointSlot(
            endpoint_id=eid_degraded,
            breaker_state=BreakerState.CLOSED,
            capacity=5,
            tokens=2.7,
            due=2,
            rate_limit_per_s=5,
        ),
        EndpointSlot(
            endpoint_id=eid_zero,
            breaker_state=BreakerState.CLOSED,
            capacity=5,
            tokens=0.2,
            due=1,
            rate_limit_per_s=5,
        ),
    ]
    limiter = MagicMock()

    async def take(eid: UUID, **_kwargs: object) -> RateLimitResult | None:
        if eid == eid_ok:
            return RateLimitResult(allowed=1, retry_after_ms=0, remaining=0)
        return None

    limiter.take = take
    quotas = {
        eid_unlimited: 3,
        eid_ok: 2,
        eid_degraded: 2,
        eid_zero: 1,
    }
    capped = await _apply_redis_rate_limits(quotas, slots, limiter)
    assert capped[eid_unlimited] == 3
    assert capped[eid_ok] == 1
    assert capped[eid_degraded] == 2
    assert eid_zero not in capped
