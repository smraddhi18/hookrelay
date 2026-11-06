"""Tenant dashboard: throughput, success rate and latency percentiles over a bounded window.

Computed on read from ``delivery_attempts`` (same approach as endpoint health): no counters to
drift, index-backed, capped by ``DASHBOARD_MAX_WINDOW_S`` / ``DASHBOARD_MAX_ATTEMPTS``. Optional
per-endpoint breakdown for the busiest endpoints in the window.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings
from hookrelay.core.errors import UnprocessableEntityError

_TENANT_SQL = text(
    """
    WITH recent AS (
        SELECT outcome, latency_ms, endpoint_id, created_at
        FROM delivery_attempts
        WHERE tenant_id = :tenant_id
          AND created_at >= now() - make_interval(secs => :window_s)
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
           percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95,
           percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms) AS p99
    FROM sample
    """
)

_ENDPOINT_SQL = text(
    """
    WITH recent AS (
        SELECT outcome, latency_ms, endpoint_id, created_at
        FROM delivery_attempts
        WHERE tenant_id = :tenant_id
          AND created_at >= now() - make_interval(secs => :window_s)
        ORDER BY created_at DESC
        LIMIT :cap
    )
    SELECT endpoint_id,
           count(*) AS attempts,
           count(*) FILTER (WHERE outcome = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE outcome = 'failed') AS failed,
           percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms) AS p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95
    FROM recent
    GROUP BY endpoint_id
    ORDER BY attempts DESC
    LIMIT :endpoint_limit
    """
)

_BACKLOG_SQL = text(
    """
    SELECT count(*) FILTER (WHERE status = 'pending') AS pending,
           count(*) FILTER (WHERE status = 'delivering') AS delivering
    FROM deliveries
    WHERE tenant_id = :tenant_id
    """
)


@dataclass(frozen=True, slots=True)
class DashboardPolicy:
    default_window_s: int
    max_window_s: int
    max_attempts: int
    endpoint_limit: int = 20

    @classmethod
    def from_settings(cls, settings: Settings) -> DashboardPolicy:
        return cls(
            default_window_s=settings.dashboard_default_window_s,
            max_window_s=settings.dashboard_max_window_s,
            max_attempts=settings.dashboard_max_attempts,
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
class EndpointDashboardRow:
    endpoint_id: uuid.UUID
    attempts: int
    succeeded: int
    failed: int
    success_rate: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None


@dataclass(frozen=True, slots=True)
class TenantDashboard:
    tenant_id: uuid.UUID
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
    throughput_per_minute: float
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    backlog_pending: int
    backlog_delivering: int
    endpoints: list[EndpointDashboardRow]


async def get_tenant_dashboard(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    policy: DashboardPolicy,
    window_s: int | None = None,
) -> TenantDashboard:
    window = policy.window(window_s)
    params = {
        "tenant_id": tenant_id,
        "window_s": window,
        "cap": policy.max_attempts,
        "cap_plus_one": policy.max_attempts + 1,
        "endpoint_limit": policy.endpoint_limit,
    }
    row = (await session.execute(_TENANT_SQL, params)).one()
    endpoint_rows = (await session.execute(_ENDPOINT_SQL, params)).all()
    backlog = (await session.execute(_BACKLOG_SQL, {"tenant_id": tenant_id})).one()
    attempts = int(row.attempts)
    succeeded = int(row.succeeded)
    window_minutes = max(window / 60.0, 1e-9)
    endpoints = [
        EndpointDashboardRow(
            endpoint_id=r.endpoint_id,
            attempts=int(r.attempts),
            succeeded=int(r.succeeded),
            failed=int(r.failed),
            success_rate=None if r.attempts == 0 else round(r.succeeded / r.attempts, 4),
            latency_p50_ms=_ms(r.p50),
            latency_p95_ms=_ms(r.p95),
        )
        for r in endpoint_rows
    ]
    return TenantDashboard(
        tenant_id=tenant_id,
        window_s=window,
        window_started_at=row.window_started_at,
        computed_at=row.computed_at,
        sampled=bool(row.sampled),
        sample_started_at=row.sample_started_at,
        attempts=attempts,
        succeeded=succeeded,
        retrying=int(row.retrying),
        failed=int(row.failed),
        success_rate=None if attempts == 0 else round(succeeded / attempts, 4),
        throughput_per_minute=round(attempts / window_minutes, 4),
        latency_p50_ms=_ms(row.p50),
        latency_p95_ms=_ms(row.p95),
        latency_p99_ms=_ms(row.p99),
        backlog_pending=int(backlog.pending or 0),
        backlog_delivering=int(backlog.delivering or 0),
        endpoints=endpoints,
    )


def _ms(value: float | None) -> float | None:
    return None if value is None else round(float(value), 1)
