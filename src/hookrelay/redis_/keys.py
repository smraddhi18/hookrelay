"""Every Redis key and channel hookrelay uses, in one place."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


@dataclass(frozen=True, slots=True)
class RedisKeys:
    """Key builders scoped to a deployment prefix (default ``hookrelay``)."""

    prefix: str = "hookrelay"

    def wakeup_channel(self, name: str = "deliveries") -> str:
        """Pub/sub channel that wakes delivery workers. Payload is observational only."""
        return f"{self.prefix}:wake:{name}"

    def subscription_cache(self, tenant_id: UUID, event_type: str) -> str:
        """Cached set of matching enabled endpoint IDs for one (tenant, event_type)."""
        return f"{self.prefix}:sub:{tenant_id}:{event_type}"

    def subscription_lock(self, tenant_id: UUID, event_type: str) -> str:
        """Short-lived single-flight lock while a cold cache key is filled from Postgres."""
        return f"{self.prefix}:sublock:{tenant_id}:{event_type}"

    def rate_limit_endpoint(self, endpoint_id: UUID) -> str:
        """Token-bucket hash for one endpoint (fields: tokens, ts)."""
        return f"{self.prefix}:rl:endpoint:{endpoint_id}"


def redis_keys(prefix: str = "hookrelay") -> RedisKeys:
    return RedisKeys(prefix=prefix)
