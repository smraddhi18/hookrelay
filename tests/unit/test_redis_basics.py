"""Unit tests for Redis helpers that need no Redis server."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from pydantic import ValidationError

from hookrelay.redis_.client import reconnect_delay_ms
from hookrelay.redis_.coalescer import KeyedCoalescer
from hookrelay.redis_.keys import redis_keys
from hookrelay.redis_.log_throttle import LogThrottle
from hookrelay.redis_.rate_limit import RateLimitResult, parse_bucket_reply
from hookrelay.redis_.wakeup import decode_wakeup, encode_wakeup
from tests.conftest import make_settings


def test_reconnect_delay_equal_jitter_bounds() -> None:
    delays = [reconnect_delay_ms(1, 100, 5_000, random_fn=lambda: 0.0) for _ in range(3)]
    assert delays == [50, 50, 50]  # floor of cap/2 when random=0
    high = reconnect_delay_ms(1, 100, 5_000, random_fn=lambda: 1.0)
    assert high == 100
    capped = reconnect_delay_ms(20, 100, 5_000, random_fn=lambda: 1.0)
    assert capped == 5_000


def test_log_throttle_suppresses_within_interval() -> None:
    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    throttle = LogThrottle(10.0, clock=now)
    first = throttle.check("e")
    assert first.log
    assert first.suppressed == 0
    second = throttle.check("e")
    assert not second.log
    third = throttle.check("e")
    assert not third.log
    clock["t"] = 10.0
    fourth = throttle.check("e")
    assert fourth.log
    assert fourth.suppressed == 2


def test_redis_keys() -> None:
    keys = redis_keys("hookrelay")
    tid = uuid4()
    eid = uuid4()
    assert keys.wakeup_channel() == "hookrelay:wake:deliveries"
    assert keys.subscription_cache(tid, "invoice.paid") == f"hookrelay:sub:{tid}:invoice.paid"
    assert keys.rate_limit_endpoint(eid) == f"hookrelay:rl:endpoint:{eid}"


def test_parse_bucket_reply() -> None:
    result = parse_bucket_reply([2, 150, 3])
    assert result == RateLimitResult(allowed=2, retry_after_ms=150, remaining=3)
    with pytest.raises(ValueError, match="Unexpected"):
        parse_bucket_reply([1, 2])
    with pytest.raises(ValueError, match="Unexpected"):
        parse_bucket_reply("nope")


def test_encode_decode_wakeup() -> None:
    raw = encode_wakeup(source="relay", name="deliveries", sent_at_ms=1_700_000_000_000)
    body = decode_wakeup(raw)
    assert body["source"] == "relay"
    assert body["queue"] == "deliveries"
    assert body["sentAt"] == 1_700_000_000_000
    assert decode_wakeup("not-json") == {}
    assert decode_wakeup('"just-a-string"') == {}


@pytest.mark.asyncio
async def test_coalescer_leading_and_trailing() -> None:
    fired: list[str] = []
    coalescer = KeyedCoalescer(50, fired.append)
    coalescer.trigger("a")
    coalescer.trigger("a")
    coalescer.trigger("a")
    assert fired == ["a"]  # leading edge
    await asyncio.sleep(0.08)
    assert fired == ["a", "a"]  # trailing edge once
    # Trailing fire re-opens a window with pending=False until it expires.
    assert coalescer.open_windows == 1
    coalescer.flush()
    assert coalescer.open_windows == 0


@pytest.mark.asyncio
async def test_coalescer_flush_fires_pending() -> None:
    fired: list[str] = []
    coalescer = KeyedCoalescer(5_000, fired.append)
    coalescer.trigger("x")
    coalescer.trigger("x")
    assert fired == ["x"]
    assert coalescer.flush() == 1
    assert fired == ["x", "x"]
    assert coalescer.open_windows == 0


def test_redis_settings_defaults_and_validators() -> None:
    s = make_settings()
    assert s.redis_url is None
    assert not s.wakeup_enabled_effective
    assert not s.subscription_cache_enabled_effective
    assert not s.rate_limit_redis_enabled_effective

    on = make_settings(redis_url="redis://localhost:6379/0")
    assert on.wakeup_enabled_effective
    assert on.subscription_cache_enabled_effective
    assert on.rate_limit_redis_enabled_effective

    off = make_settings(redis_url="redis://localhost:6379/0", wakeup_enabled=False)
    assert not off.wakeup_enabled_effective

    with pytest.raises(ValidationError, match="RECONNECT"):
        make_settings(redis_reconnect_base_ms=10_000, redis_reconnect_max_ms=100)

    with pytest.raises(ValidationError, match="PREFIX"):
        make_settings(redis_key_prefix="")
