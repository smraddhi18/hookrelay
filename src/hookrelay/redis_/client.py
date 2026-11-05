"""Fail-soft redis.asyncio client: reconnect forever, commands fail fast while down."""

from __future__ import annotations

import asyncio
import contextlib
import random
from collections.abc import Callable
from dataclasses import dataclass

import redis.asyncio as redis_async
import structlog
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from hookrelay.redis_.log_throttle import LogThrottle

logger = structlog.stdlib.get_logger("hookrelay.redis")

ERROR_LOG_INTERVAL_S = 10.0

# Errors callers treat as "Redis is unavailable; fall back to Postgres".
REDIS_TRANSIENT = (
    RedisConnectionError,
    RedisTimeoutError,
    RedisError,
    OSError,
    TimeoutError,
    asyncio.TimeoutError,
)


def reconnect_delay_ms(
    attempt: int,
    base_ms: int,
    max_ms: int,
    random_fn: Callable[[], float] = random.random,
) -> int:
    """Equal-jitter delay for reconnect attempt ``attempt`` (1-based).

    ``cap = min(max, base * 2^(n-1))``, then ``random(cap/2, cap)``. Never 0 so a dead
    server cannot hot-loop, and replicas restarting together spread out.
    """
    cap = min(max_ms, base_ms * (2 ** min(max(attempt - 1, 0), 30)))
    return int(max(1, round(cap / 2 + random_fn() * (cap / 2))))


@dataclass(frozen=True, slots=True)
class RedisConnectionSettings:
    url: str
    connect_timeout_ms: int
    command_timeout_ms: int
    reconnect_base_ms: int
    reconnect_max_ms: int


class RedisFacade:
    """Thin wrapper around one redis.asyncio connection with readiness + throttled logs.

    Commands are not queued offline: ``socket_timeout`` bounds each call and callers treat
    errors as a miss / degraded mode. Pub/sub needs a dedicated connection (a subscribed
    connection cannot run other commands), so callers create one facade per role.
    """

    def __init__(
        self,
        client: Redis,
        *,
        name: str,
        random_fn: Callable[[], float] = random.random,
    ) -> None:
        self.client = client
        self.name = name
        self._random = random_fn
        self._log = logger.bind(redis_client=name)
        self._errors = LogThrottle(ERROR_LOG_INTERVAL_S)
        self._connected = False
        self._closing = False
        self._reconnect_attempts = 0

    @property
    def raw(self) -> Redis:
        return self.client

    def mark_connected(self) -> None:
        was = self._connected
        self._connected = True
        if not was:
            self._log.info("redis.connected", reconnect_attempts=self._reconnect_attempts)
            self._reconnect_attempts = 0

    def mark_disconnected(self) -> None:
        if not self._connected:
            return
        self._connected = False
        if self._closing:
            self._log.info("redis.closed")
            return
        self._log.warning("redis.disconnected")

    def note_reconnect(self, delay_ms: int) -> None:
        self._reconnect_attempts += 1
        self._log.debug("redis.reconnecting", attempt=self._reconnect_attempts, delay_ms=delay_ms)

    def note_error(self, err: BaseException) -> None:
        decision = self._errors.check("error")
        if not decision.log:
            return
        self._log.warning(
            "redis.error",
            error=type(err).__name__,
            detail=str(err)[:200],
            suppressed=decision.suppressed,
        )

    @property
    def is_ready(self) -> bool:
        return self._connected and not self._closing

    @property
    def closing(self) -> bool:
        return self._closing

    async def ensure_connected(self) -> bool:
        """Best-effort PING; updates readiness. Never raises."""
        if self._closing:
            return False
        try:
            pong = await self.client.ping()
        except REDIS_TRANSIENT as exc:
            self.mark_disconnected()
            self.note_error(exc)
            return False
        if pong:
            self.mark_connected()
            return True
        self.mark_disconnected()
        return False

    async def close(self, *, timeout_s: float = 1.0) -> None:
        if self._closing:
            return
        self._closing = True
        self._connected = False
        try:
            await asyncio.wait_for(self.client.aclose(), timeout=timeout_s)
        except Exception:
            with contextlib.suppress(Exception):
                await self.client.connection_pool.disconnect(inuse_connections=True)
        self._log.info("redis.closed")


def create_redis(
    settings: RedisConnectionSettings,
    *,
    name: str,
    random_fn: Callable[[], float] = random.random,
) -> RedisFacade:
    """Build a command (or to-be-subscribed) client. Never raises on construction."""
    connect_s = settings.connect_timeout_ms / 1000
    command_s = settings.command_timeout_ms / 1000
    client: Redis = redis_async.from_url(  # type: ignore[no-untyped-call]
        settings.url,
        socket_connect_timeout=connect_s,
        socket_timeout=command_s,
        retry_on_timeout=False,
        health_check_interval=max(1, int(command_s) or 1),
        client_name=name,
        decode_responses=True,
    )
    return RedisFacade(client, name=name, random_fn=random_fn)


def is_redis_ready(facade: RedisFacade | None) -> bool:
    return facade is not None and facade.is_ready


async def ping_redis(facade: RedisFacade, *, timeout_s: float) -> None:
    """Readiness probe: rejects unless the connection answers PING within ``timeout_s``."""
    if facade.closing:
        raise RuntimeError("Redis is closing")
    async with asyncio.timeout(timeout_s):
        ok = await facade.client.ping()
    if not ok:
        raise RuntimeError("Redis PING returned falsy")
    facade.mark_connected()


async def close_redis(facade: RedisFacade | None, *, timeout_s: float = 1.0) -> None:
    if facade is not None:
        await facade.close(timeout_s=timeout_s)
