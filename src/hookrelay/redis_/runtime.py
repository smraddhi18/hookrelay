"""Process-scoped Redis wiring for the API, relay and worker.

Redis is optional. A process with ``REDIS_URL`` unset gets a no-op bundle: every feature
degrades to the Postgres path. Features can also be flipped off individually via settings.
"""

from __future__ import annotations

import socket
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import structlog

from hookrelay.core.config import Settings
from hookrelay.redis_.client import (
    RedisConnectionSettings,
    RedisFacade,
    close_redis,
    create_redis,
)
from hookrelay.redis_.keys import RedisKeys, redis_keys
from hookrelay.redis_.rate_limit import RedisRateLimiter
from hookrelay.redis_.subscription_cache import SubscriptionCache
from hookrelay.redis_.wakeup import WakeupPublisher, WakeupSubscriber

logger = structlog.stdlib.get_logger("hookrelay.redis.runtime")


@dataclass(slots=True)
class RedisBundle:
    """What one process holds when Redis is configured. Fields may still be None when a
    feature is disabled even though the URL is set."""

    keys: RedisKeys
    command: RedisFacade | None = None
    publisher: WakeupPublisher | None = None
    cache: SubscriptionCache | None = None
    rate_limiter: RedisRateLimiter | None = None

    @property
    def enabled(self) -> bool:
        return self.command is not None

    async def close(self) -> None:
        if self.publisher is not None:
            await self.publisher.flush()
        await close_redis(self.command)


def _connection_settings(settings: Settings) -> RedisConnectionSettings:
    if settings.redis_url is None:  # pragma: no cover - guarded by callers
        raise RuntimeError("REDIS_URL is required to build connection settings")
    return RedisConnectionSettings(
        url=settings.redis_url,
        connect_timeout_ms=settings.redis_connect_timeout_ms,
        command_timeout_ms=settings.redis_command_timeout_ms,
        reconnect_base_ms=settings.redis_reconnect_base_ms,
        reconnect_max_ms=settings.redis_reconnect_max_ms,
    )


def _client_name(role: str, instance_id: str | None = None) -> str:
    suffix = instance_id or f"{socket.gethostname()[:40]}:{uuid.uuid4().hex[:8]}"
    return f"hookrelay:{role}:cmd:{suffix}"


def build_redis_bundle(settings: Settings, *, role: str, source: str) -> RedisBundle:
    """Construct the command-side Redis features for ``role`` (``api`` / ``relay`` / ``worker``).

    Does not open a pub/sub connection; workers call :func:`build_wakeup_subscriber` separately.
    """
    keys = redis_keys(settings.redis_key_prefix)
    if settings.redis_url is None:
        logger.info("redis.disabled", reason="no_url", role=role)
        return RedisBundle(keys=keys)

    facade = create_redis(_connection_settings(settings), name=_client_name(role))
    publisher: WakeupPublisher | None = None
    if settings.wakeup_enabled_effective:
        publisher = WakeupPublisher(
            client=facade,
            keys=keys,
            coalesce_ms=settings.wakeup_coalesce_ms,
            source=source,
        )
    cache: SubscriptionCache | None = None
    if settings.subscription_cache_enabled_effective:
        cache = SubscriptionCache(
            client=facade,
            keys=keys,
            ttl_s=settings.subscription_cache_ttl_s,
            enabled=True,
        )
    rate_limiter: RedisRateLimiter | None = None
    if settings.rate_limit_redis_enabled_effective:
        rate_limiter = RedisRateLimiter(facade, keys)

    logger.info(
        "redis.enabled",
        role=role,
        wakeup=publisher is not None,
        subscription_cache=cache is not None,
        rate_limit=rate_limiter is not None,
    )
    return RedisBundle(
        keys=keys,
        command=facade,
        publisher=publisher,
        cache=cache,
        rate_limiter=rate_limiter,
    )


def build_wakeup_subscriber(
    settings: Settings,
    *,
    instance_id: str,
    on_wake: Callable[[], None],
    on_listening_change: Callable[[bool], None],
) -> WakeupSubscriber | None:
    """Dedicated pub/sub connection for a worker. ``None`` when wake-ups are off."""
    if settings.redis_url is None or not settings.wakeup_enabled_effective:
        return None
    keys = redis_keys(settings.redis_key_prefix)
    facade = create_redis(
        _connection_settings(settings),
        name=f"hookrelay:worker:sub:{instance_id}",
    )
    return WakeupSubscriber(
        facade,
        keys,
        on_wake=on_wake,
        on_listening_change=on_listening_change,
        reconnect_base_ms=settings.redis_reconnect_base_ms,
        reconnect_max_ms=settings.redis_reconnect_max_ms,
    )
