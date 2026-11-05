"""Wakeup encode/publisher/subscriber against live Redis."""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest

from hookrelay.redis_.client import RedisConnectionSettings, create_redis
from hookrelay.redis_.keys import redis_keys
from hookrelay.redis_.wakeup import WakeupPublisher, WakeupSubscriber

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")


def _settings() -> RedisConnectionSettings:
    return RedisConnectionSettings(
        url=REDIS_URL,
        connect_timeout_ms=500,
        command_timeout_ms=500,
        reconnect_base_ms=50,
        reconnect_max_ms=200,
    )


@pytest.mark.asyncio
async def test_publisher_subscriber_roundtrip() -> None:
    keys = redis_keys(f"hookrelay-test-{uuid4().hex[:8]}")
    pub_client = create_redis(_settings(), name="hookrelay:test:pub")
    sub_client = create_redis(_settings(), name="hookrelay:test:sub")
    if not await pub_client.ensure_connected():
        await pub_client.close()
        await sub_client.close()
        pytest.skip("Redis not reachable")

    woke = asyncio.Event()
    listening = asyncio.Event()

    sub = WakeupSubscriber(
        sub_client,
        keys,
        on_wake=woke.set,
        on_listening_change=lambda ok: listening.set() if ok else None,
        reconnect_base_ms=50,
        reconnect_max_ms=200,
    )
    pub = WakeupPublisher(pub_client, keys, coalesce_ms=0, source="test")
    try:
        await sub.start()
        await asyncio.wait_for(listening.wait(), timeout=2)
        pub.notify()
        await pub.flush()
        await asyncio.wait_for(woke.wait(), timeout=2)
    finally:
        await sub.close()
        await pub_client.close()
