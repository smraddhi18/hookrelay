"""Endpoint health summary: success rate, latency percentiles, errors, breaker and backlog.

Computed on read from ``delivery_attempts`` (no counters to keep consistent with the attempt
log), bounded so one call has a predictable cost however busy the endpoint is:

* only attempts inside the window (``window_s``, capped by ``ENDPOINT_HEALTH_MAX_WINDOW_S``)
  and at most the newest ``ENDPOINT_HEALTH_MAX_ATTEMPTS`` of them; when the cap cuts the
  window short the response says ``sampled: true`` and ``sample_started_at`` tells from when
  the numbers are;
* the scan is an index-only range scan of ``ix_delivery_attempts_endpoint_created`` (key
  ``(endpoint_id, created_at)``, ``INCLUDE (outcome, latency_ms, error_class)``). It filters
  on ``endpoint_id`` alone: the endpoint was looked up tenant-scoped first, and the composite
  FK ``(tenant_id, endpoint_id) -> endpoints`` guarantees its attempts carry the same tenant;
* "last error" is all-time, not windowed, via the partial index on unsuccessful attempts, so
  it never walks past a long run of successes;
* the pending backlog count is capped (it reads ``ix_deliveries_endpoint_due``).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings
from hookrelay.core.errors import UnprocessableEntityError
from hookrelay.services.endpoints import CircuitStatus, get_circuit, get_endpoint


@dataclass(frozen=True, slots=True)
class HealthPolicy:
    default_window_s: int
    max_window_s: int
    max_attempts: int
    default_max_in_flight: int
    backlog_cap: int = 10_000

    @classmethod
    def from_settings(cls, settings: Settings) -> HealthPolicy:
        return cls(
            default_window_s=settings.endpoint_health_default_window_s,
            max_window_s=settings.endpoint_health_max_window_s,
            max_attempts=settings.endpoint_health_max_attempts,
            default_max_in_flight=settings.endpoint_default_max_in_flight,
        )

    def window(self, requested: int | None) -> int:
        if requested is None:
            return self.default_window_s
        if requested > self.max_window_s:
            raise UnprocessableEntityError(
                f"window_s must be at most {self.max_window_s} on this deployment",
                code="window_too_large",
                details=[{"loc": ["query", "window_s"]}],
            )
        return requested


@dataclass(frozen=True, slots=True)
class LastError:
    at: datetime
    delivery_id: uuid.UUID
    error_class: str
    status_code: int | None
    error_message: str | None


@dataclass(frozen=True, slots=True)
class EndpointHealth:
    endpoint_id: uuid.UUID
    enabled: bool
    window_s: int
    window_started_at: datetime
    computed_at: datetime
    sampled: bool
    sample_started_at: datetime | None
    attempts: int
    succeeded: int
    retrying: int
    failed: int
    success_rate: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    errors_by_class: dict[str, int]
    last_error: LastError | None
    last_success_at: datetime | None
    circuit: CircuitStatus
    backlog: int
    backlog_capped: bool


_WINDOW_SQL = text(
    """
    WITH recent AS (
        SELECT outcome, latency_ms, error_class, created_at
        FROM delivery_attempts
        WHERE endpoint_id = :endpoint_id AND created_at >= now() - make_interval(secs => :window_s)
        ORDER BY created_at DESC
        LIMIT :cap_plus_one
    ),
    sample AS (
        SELECT * FROM recent ORDER BY created_at DESC LIMIT :cap
    )
    SELECT now() AS computed_at,
           now() - make_interval(secs => :window_s) AS window_started_at,
           (SELECT count(*) FROM recent) > :cap AS sampled,
           min(created_at) AS sample_started_at,
           count(*) AS attempts,
           count(*) FILTER (WHERE outcome = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE outcome = 'retrying') AS retrying,
           count(*) FILTER (WHERE outcome = 'failed') AS failed,
           max(created_at) FILTER (WHERE outcome = 'succeeded') AS last_success_in_window,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99,
           (SELECT coalesce(jsonb_object_agg(error_class, n), '{}'::jsonb)
            FROM (SELECT error_class, count(*) AS n FROM sample
                  WHERE error_class IS NOT NULL GROUP BY error_class) AS errors
           ) AS errors_by_class
    FROM sample
    """
)

_LAST_ERROR_SQL = text(
    """
    SELECT created_at, delivery_id, error_class, status_code, error_message
    FROM delivery_attempts
    WHERE endpoint_id = :endpoint_id AND outcome <> 'succeeded'
    ORDER BY created_at DESC
    LIMIT 1
    """
)

_LAST_SUCCESS_SQL = text(
    """
    SELECT max(created_at) FROM delivery_attempts
    WHERE endpoint_id = :endpoint_id AND outcome = 'succeeded'
    """
)

_BACKLOG_SQL = text(
    """
    SELECT count(*) FROM (
        SELECT 1 FROM deliveries
        WHERE endpoint_id = :endpoint_id AND status = 'pending'
        LIMIT :cap_plus_one
    ) AS pending
    """
)


async def get_endpoint_health(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    *,
    policy: HealthPolicy,
    window_s: int | None = None,
) -> EndpointHealth:
    window = policy.window(window_s)
    # Tenant-scoped lookup first: another tenant's endpoint is a 404 before any stats query.
    endpoint = await get_endpoint(session, tenant_id, endpoint_id)
    circuit = await get_circuit(
        session, tenant_id, endpoint_id, default_max_in_flight=policy.default_max_in_flight
    )
    ids = {"endpoint_id": endpoint_id}
    row = (
        await session.execute(
            _WINDOW_SQL,
            {
                **ids,
                "window_s": window,
                "cap": policy.max_attempts,
                "cap_plus_one": policy.max_attempts + 1,
            },
        )
    ).one()
    last = (await session.execute(_LAST_ERROR_SQL, ids)).one_or_none()
    last_success = row.last_success_in_window
    if last_success is None:
        last_success = await session.scalar(_LAST_SUCCESS_SQL, ids)
    backlog = int(
        await session.scalar(_BACKLOG_SQL, {**ids, "cap_plus_one": policy.backlog_cap + 1}) or 0
    )
    attempts = int(row.attempts)
    return EndpointHealth(
        endpoint_id=endpoint_id,
        enabled=endpoint.enabled,
        window_s=window,
        window_started_at=row.window_started_at,
        computed_at=row.computed_at,
        sampled=bool(row.sampled),
        sample_started_at=row.sample_started_at,
        attempts=attempts,
        succeeded=int(row.succeeded),
        retrying=int(row.retrying),
        failed=int(row.failed),
        success_rate=None if attempts == 0 else round(row.succeeded / attempts, 4),
        latency_p50_ms=_ms(row.p50),
        latency_p95_ms=_ms(row.p95),
        latency_p99_ms=_ms(row.p99),
        errors_by_class={str(k): int(v) for k, v in dict(row.errors_by_class).items()},
        last_error=None
        if last is None
        else LastError(
            at=last.created_at,
            delivery_id=last.delivery_id,
            error_class=last.error_class,
            status_code=last.status_code,
            error_message=last.error_message,
        ),
        last_success_at=last_success,
        circuit=circuit,
        backlog=min(backlog, policy.backlog_cap),
        backlog_capped=backlog > policy.backlog_cap,
    )


def _ms(value: float | None) -> float | None:
    return None if value is None else round(float(value), 1)
