"""Worker wake-ups via Redis pub/sub. Presence of the message is the signal."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import structlog
from redis.asyncio.client import PubSub

from hookrelay.redis_.client import (
    REDIS_TRANSIENT,
    RedisFacade,
    close_redis,
    reconnect_delay_ms,
)
from hookrelay.redis_.coalescer import KeyedCoalescer
from hookrelay.redis_.keys import RedisKeys
from hookrelay.redis_.log_throttle import LogThrottle

logger = structlog.stdlib.get_logger("hookrelay.wakeup")

FAILURE_LOG_INTERVAL_S = 10.0
# Global deliveries channel: every worker listens; publishers coalesce bursts. Per-endpoint
# channels would multiply publishes with endpoint count for little gain while the worker still
# has to poll Postgres for *which* deliveries are due.
DEFAULT_WAKE_NAME = "deliveries"


def encode_wakeup(
    *, source: str, name: str = DEFAULT_WAKE_NAME, sent_at_ms: int | None = None
) -> str:
    return json.dumps(
        {
            "queue": name,
            "sentAt": sent_at_ms if sent_at_ms is not None else int(time.time() * 1000),
            "source": source,
        },
        separators=(",", ":"),
    )


def decode_wakeup(raw: str) -> dict[str, Any]:
    """Lenient: a malformed body still counts as a wake-up for its channel."""
    try:
        parsed: object = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


@dataclass(slots=True)
class WakeupPublisher:
    """Fire-and-forget publisher with leading+trailing coalescing per channel name."""

    client: RedisFacade
    keys: RedisKeys
    coalesce_ms: float
    source: str
    _coalescer: KeyedCoalescer = field(init=False, repr=False)
    _in_flight: set[asyncio.Task[None]] = field(init=False, repr=False)
    _failures: LogThrottle = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._coalescer = KeyedCoalescer(self.coalesce_ms, self._publish)
        self._in_flight = set()
        self._failures = LogThrottle(FAILURE_LOG_INTERVAL_S)

    def notify(self, name: str = DEFAULT_WAKE_NAME) -> None:
        self._coalescer.trigger(name)

    async def flush(self) -> None:
        self._coalescer.flush()
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)

    def _publish(self, name: str) -> None:
        if self.client.closing:
            self._log_failure(name, RuntimeError(f"Redis is closing ({self.client.name})"))
            return
        channel = self.keys.wakeup_channel(name)
        message = encode_wakeup(source=self.source, name=name)

        async def _run() -> None:
            try:
                receivers = await self.client.client.publish(channel, message)
                self.client.mark_connected()
                logger.debug("wakeup.published", queue=name, receivers=receivers)
            except REDIS_TRANSIENT as exc:
                self.client.mark_disconnected()
                self.client.note_error(exc)
                self._log_failure(name, exc)

        task = asyncio.create_task(_run(), name=f"wakeup-publish-{name}")
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

    def _log_failure(self, name: str, err: BaseException) -> None:
        decision = self._failures.check("publish")
        if not decision.log:
            return
        logger.warning(
            "wakeup.publish_failed",
            queue=name,
            error=type(err).__name__,
            suppressed=decision.suppressed,
        )


class WakeupSubscriber:
    """Listens on the deliveries wake channel; owns a dedicated Redis connection."""

    def __init__(
        self,
        client: RedisFacade,
        keys: RedisKeys,
        *,
        on_wake: Callable[[], None],
        on_listening_change: Callable[[bool], None],
        name: str = DEFAULT_WAKE_NAME,
        reconnect_base_ms: int = 100,
        reconnect_max_ms: int = 5_000,
    ) -> None:
        self._client = client
        self._channel = keys.wakeup_channel(name)
        self._on_wake = on_wake
        self._on_listening_change = on_listening_change
        self._reconnect_base_ms = reconnect_base_ms
        self._reconnect_max_ms = reconnect_max_ms
        self._failures = LogThrottle(FAILURE_LOG_INTERVAL_S)
        self._listening = False
        self._started = False
        self._closed = False
        self._pubsub: PubSub | None = None
        self._reader: asyncio.Task[None] | None = None
        self._reconnect: asyncio.Task[None] | None = None

    @property
    def is_listening(self) -> bool:
        return self._listening

    async def start(self) -> None:
        if self._started:
            raise RuntimeError("WakeupSubscriber already started")
        self._started = True
        await self._subscribe()
        self._reconnect = asyncio.create_task(self._reconnect_loop(), name="wakeup-reconnect")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._set_listening(False)
        if self._reconnect is not None:
            self._reconnect.cancel()
            await asyncio.gather(self._reconnect, return_exceptions=True)
            self._reconnect = None
        if self._reader is not None:
            self._reader.cancel()
            await asyncio.gather(self._reader, return_exceptions=True)
            self._reader = None
        if self._pubsub is not None:
            with contextlib.suppress(Exception):
                await self._pubsub.unsubscribe(self._channel)
                await self._pubsub.aclose()  # type: ignore[no-untyped-call]
            self._pubsub = None
        await close_redis(self._client)

    async def _subscribe(self) -> None:
        if self._closed:
            return
        try:
            if not await self._client.ensure_connected():
                self._set_listening(False)
                return
            pubsub = self._client.client.pubsub(ignore_subscribe_messages=True)
            await pubsub.subscribe(self._channel)
        except REDIS_TRANSIENT as exc:
            self._client.mark_disconnected()
            self._client.note_error(exc)
            decision = self._failures.check("subscribe")
            if decision.log:
                logger.warning(
                    "wakeup.subscribe_failed",
                    error=type(exc).__name__,
                    suppressed=decision.suppressed,
                )
            self._set_listening(False)
            return
        self._pubsub = pubsub
        if self._reader is not None:
            self._reader.cancel()
            await asyncio.gather(self._reader, return_exceptions=True)
        self._reader = asyncio.create_task(self._read_loop(pubsub), name="wakeup-reader")
        logger.info("wakeup.subscribed", channel=self._channel)
        self._set_listening(True)

    async def _read_loop(self, pubsub: PubSub) -> None:
        try:
            while not self._closed:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message is None:
                    continue
                if message.get("type") != "message":
                    continue
                raw = message.get("data")
                body = decode_wakeup(raw if isinstance(raw, str) else "")
                sent_at = body.get("sentAt")
                lag_ms = (
                    int(time.time() * 1000) - int(sent_at)
                    if isinstance(sent_at, (int, float))
                    else None
                )
                logger.debug("wakeup.received", source=body.get("source"), lag_ms=lag_ms)
                self._on_wake()
        except asyncio.CancelledError:
            raise
        except REDIS_TRANSIENT as exc:
            self._client.mark_disconnected()
            self._client.note_error(exc)
            self._set_listening(False)
        except Exception as exc:  # keep the subscriber alive
            logger.warning("wakeup.reader_failed", error=type(exc).__name__)
            self._set_listening(False)

    async def _reconnect_loop(self) -> None:
        attempt = 0
        while not self._closed:
            await asyncio.sleep(0.5)
            if self._listening or self._closed:
                attempt = 0
                continue
            attempt += 1
            delay_ms = reconnect_delay_ms(attempt, self._reconnect_base_ms, self._reconnect_max_ms)
            self._client.note_reconnect(delay_ms)
            await asyncio.sleep(delay_ms / 1000)
            await self._subscribe()  # no-op when closed

    def _set_listening(self, listening: bool) -> None:
        if self._closed:
            return
        changed = self._listening != listening
        self._listening = listening
        if not listening and changed:
            logger.warning("wakeup.unsubscribed")
        if changed or listening:
            self._on_listening_change(listening)
