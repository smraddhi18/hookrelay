"""Optional Redis helpers: wake-ups, subscription cache, token-bucket rate limits.

Redis is optional. When ``REDIS_URL`` is unset every feature degrades to the Postgres
path already used in Milestones 0-5: workers poll, fan-out joins subscriptions in SQL,
and rate limits stay inside the lease transaction. Nothing Redis does is on the
correctness critical path for at-least-once delivery.
"""

from __future__ import annotations

from hookrelay.redis_.client import (
    RedisConnectionSettings,
    RedisFacade,
    close_redis,
    create_redis,
    is_redis_ready,
    ping_redis,
    reconnect_delay_ms,
)
from hookrelay.redis_.coalescer import KeyedCoalescer
from hookrelay.redis_.keys import RedisKeys, redis_keys
from hookrelay.redis_.rate_limit import (
    TOKEN_BUCKET_SCRIPT,
    RateLimitResult,
    RedisRateLimiter,
    parse_bucket_reply,
)
from hookrelay.redis_.runtime import RedisBundle, build_redis_bundle, build_wakeup_subscriber
from hookrelay.redis_.subscription_cache import SubscriptionCache
from hookrelay.redis_.wakeup import WakeupPublisher, WakeupSubscriber, decode_wakeup, encode_wakeup

__all__ = [
    "TOKEN_BUCKET_SCRIPT",
    "KeyedCoalescer",
    "RateLimitResult",
    "RedisBundle",
    "RedisConnectionSettings",
    "RedisFacade",
    "RedisKeys",
    "RedisRateLimiter",
    "SubscriptionCache",
    "WakeupPublisher",
    "WakeupSubscriber",
    "build_redis_bundle",
    "build_wakeup_subscriber",
    "close_redis",
    "create_redis",
    "decode_wakeup",
    "encode_wakeup",
    "is_redis_ready",
    "parse_bucket_reply",
    "ping_redis",
    "reconnect_delay_ms",
    "redis_keys",
]
