"""Cached subscription → endpoint ID lookups for fan-out, with explicit invalidation.

Correctness over cache: on miss or Redis error the caller falls back to the SQL
``pattern_matches_sql`` join. The cache never invents matches. Stampede protection uses a
short single-flight lock plus a negative cache for empty results.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from uuid import UUID

import structlog

from hookrelay.redis_.client import REDIS_TRANSIENT, RedisFacade
from hookrelay.redis_.keys import RedisKeys

logger = structlog.stdlib.get_logger("hookrelay.subscription_cache")

Loader = Callable[[UUID, str], Awaitable[list[UUID]]]


@dataclass
class SubscriptionCache:
    client: RedisFacade
    keys: RedisKeys
    ttl_s: int
    enabled: bool = True
    lock_ttl_s: float = 2.0
    negative_ttl_s: int = 5
    _local_locks: dict[tuple[str, str], asyncio.Lock] = field(default_factory=dict)

    def is_available(self) -> bool:
        """Enabled and not closing. A cold client still attempts commands (fail-soft)."""
        return self.enabled and not self.client.closing

    def _lock_for(self, tenant_id: UUID, event_type: str) -> asyncio.Lock:
        key = (str(tenant_id), event_type)
        lock = self._local_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._local_locks[key] = lock
        return lock

    async def get(self, tenant_id: UUID, event_type: str) -> list[UUID] | None:
        """Return cached endpoint IDs, or ``None`` on miss / error (never an invented match)."""
        if not self.is_available():
            return None
        key = self.keys.subscription_cache(tenant_id, event_type)
        try:
            raw = await self.client.client.get(key)
            self.client.mark_connected()
        except REDIS_TRANSIENT as exc:
            self.client.mark_disconnected()
            self.client.note_error(exc)
            return None
        if raw is None:
            return None
        try:
            parsed = json.loads(raw)
            if not isinstance(parsed, list):
                return None
            return [UUID(item) for item in parsed]
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    async def set(self, tenant_id: UUID, event_type: str, endpoint_ids: list[UUID]) -> None:
        if not self.is_available():
            return
        key = self.keys.subscription_cache(tenant_id, event_type)
        payload = json.dumps([str(eid) for eid in endpoint_ids], separators=(",", ":"))
        ttl = self.ttl_s if endpoint_ids else min(self.ttl_s, self.negative_ttl_s)
        try:
            await self.client.client.set(key, payload, ex=max(ttl, 1))
            self.client.mark_connected()
        except REDIS_TRANSIENT as exc:
            self.client.mark_disconnected()
            self.client.note_error(exc)

    async def invalidate_tenant(self, tenant_id: UUID) -> int:
        """Drop every cached (tenant, event_type) entry. Returns keys removed (best effort)."""
        if not self.is_available():
            return 0
        pattern = f"{self.keys.prefix}:sub:{tenant_id}:*"
        removed = 0
        try:
            async for key in self.client.client.scan_iter(match=pattern, count=100):
                await self.client.client.delete(key)
                removed += 1
            self.client.mark_connected()
        except REDIS_TRANSIENT as exc:
            self.client.mark_disconnected()
            self.client.note_error(exc)
            logger.warning(
                "subscription_cache.invalidate_failed",
                tenant_id=str(tenant_id),
                error=type(exc).__name__,
            )
        else:
            if removed:
                logger.info(
                    "subscription_cache.invalidated", tenant_id=str(tenant_id), keys=removed
                )
        return removed

    async def invalidate_event_type(self, tenant_id: UUID, event_type: str) -> None:
        if not self.is_available():
            return
        key = self.keys.subscription_cache(tenant_id, event_type)
        try:
            await self.client.client.delete(key)
            # Also drop wildcard / prefix keys that could have matched: safest is tenant-wide.
            # Fan-out keys are per concrete event_type string, so deleting the concrete key is
            # enough for exact; wildcards are stored under the concrete type they were queried
            # with, so a subscription change must invalidate the whole tenant.
            self.client.mark_connected()
        except REDIS_TRANSIENT as exc:
            self.client.mark_disconnected()
            self.client.note_error(exc)

    async def get_or_load(self, tenant_id: UUID, event_type: str, loader: Loader) -> list[UUID]:
        """Cache-aside with single-flight: one loader call per cold key per process."""
        cached = await self.get(tenant_id, event_type)
        if cached is not None:
            return cached
        async with self._lock_for(tenant_id, event_type):
            cached = await self.get(tenant_id, event_type)
            if cached is not None:
                return cached
            # Cross-process single-flight: SET NX lock; losers wait briefly then re-get / load.
            lock_key = self.keys.subscription_lock(tenant_id, event_type)
            got_lock = False
            if self.is_available():
                try:
                    got_lock = bool(
                        await self.client.client.set(
                            lock_key, "1", nx=True, px=int(self.lock_ttl_s * 1000)
                        )
                    )
                    self.client.mark_connected()
                except REDIS_TRANSIENT as exc:
                    self.client.mark_disconnected()
                    self.client.note_error(exc)
                    got_lock = True  # proceed to load; correctness over cache
            else:
                got_lock = True
            if not got_lock:
                await asyncio.sleep(0.05)
                cached = await self.get(tenant_id, event_type)
                if cached is not None:
                    return cached
            ids = await loader(tenant_id, event_type)
            await self.set(tenant_id, event_type, ids)
            if got_lock and self.is_available():
                with contextlib.suppress(*REDIS_TRANSIENT):
                    await self.client.client.delete(lock_key)
            return ids
