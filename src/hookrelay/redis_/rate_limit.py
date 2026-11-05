"""Atomic Redis token-bucket rate limits (Lua), with a clear degraded-mode contract.

When Redis is healthy the worker takes tokens here before leasing, and still records the
spend on the Postgres ``endpoint_states`` row so a failover has approximately correct
buckets. When Redis is down the caller uses the existing Postgres-only path - never fail
open to unlimited. ``max_in_flight`` and the circuit breaker stay authoritative in Postgres.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final
from uuid import UUID

import structlog

from hookrelay.redis_.client import REDIS_TRANSIENT, RedisFacade
from hookrelay.redis_.keys import RedisKeys

logger = structlog.stdlib.get_logger("hookrelay.rate_limit")

_BUCKET_REPLY_LEN: Final = 3

# KEYS[1] = bucket hash; ARGV = rate_per_s, burst, n_to_take.
# Refills by Redis server TIME (no API clock skew), caps at burst (1 s of tokens by
# convention), takes up to n tokens, returns {allowed_count, retry_after_ms, remaining}.
_LUA_BUCKET_SCRIPT: Final[str] = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local want = tonumber(ARGV[3])
if rate <= 0 or burst <= 0 or want <= 0 then
  return {0, 0, 0}
end
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local level = burst
if state[1] and state[2] then
  local elapsed = math.max(0, now - tonumber(state[2]))
  level = math.min(burst, tonumber(state[1]) + elapsed * rate / 1000)
end
local take = math.min(want, math.floor(level))
local retry = 0
if take < want then
  local need = 1 - (level - take)
  if need < 0 then need = 0 end
  retry = math.ceil(need * 1000 / rate)
end
level = level - take
local full_in = math.ceil((burst - level) * 1000 / rate)
redis.call('HSET', KEYS[1], 'tokens', string.format('%.6f', level), 'ts', string.format('%d', now))
redis.call('PEXPIRE', KEYS[1], full_in + 1000)
return {take, retry, math.floor(level)}
"""

# Public alias kept for tests / docs that import the script body.
TOKEN_BUCKET_SCRIPT = _LUA_BUCKET_SCRIPT


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: int
    retry_after_ms: int
    remaining: int


def parse_bucket_reply(raw: object) -> RateLimitResult:
    if not isinstance(raw, (list, tuple)) or len(raw) != _BUCKET_REPLY_LEN:
        raise ValueError("Unexpected token bucket script reply")
    nums = []
    for item in raw:
        try:
            nums.append(int(item))
        except (TypeError, ValueError) as exc:
            raise ValueError("Unexpected token bucket script reply") from exc
    return RateLimitResult(allowed=nums[0], retry_after_ms=nums[1], remaining=nums[2])


class RedisRateLimiter:
    """Per-endpoint token bucket backed by one EVAL of :data:`TOKEN_BUCKET_SCRIPT`."""

    def __init__(self, client: RedisFacade, keys: RedisKeys) -> None:
        self._client = client
        self._keys = keys
        self._script = client.client.register_script(_LUA_BUCKET_SCRIPT)

    def is_available(self) -> bool:
        """True when the client exists and is not closing. Cold clients still attempt commands."""
        return not self._client.closing

    async def take(
        self,
        endpoint_id: UUID,
        *,
        rate_per_s: int,
        n: int = 1,
        burst: int | None = None,
    ) -> RateLimitResult | None:
        """Take up to ``n`` tokens. ``None`` when Redis is unavailable (caller falls back)."""
        if n <= 0:
            return RateLimitResult(allowed=0, retry_after_ms=0, remaining=0)
        if self._client.closing:
            return None
        burst_size = burst if burst is not None else rate_per_s
        key = self._keys.rate_limit_endpoint(endpoint_id)
        try:
            raw = await self._script(keys=[key], args=[rate_per_s, burst_size, n])
            self._client.mark_connected()
            return parse_bucket_reply(raw)
        except REDIS_TRANSIENT as exc:
            self._client.mark_disconnected()
            self._client.note_error(exc)
            logger.warning(
                "rate_limit.redis_unavailable",
                endpoint_id=str(endpoint_id),
                error=type(exc).__name__,
            )
            return None
        except ValueError as exc:
            logger.warning(
                "rate_limit.bad_reply", endpoint_id=str(endpoint_id), error=str(exc)[:200]
            )
            return None
