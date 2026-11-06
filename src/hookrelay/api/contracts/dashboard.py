"""Tenant dashboard response contracts."""

from __future__ import annotations

import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from hookrelay.services.dashboard import EndpointDashboardRow, TenantDashboard


class EndpointDashboardOut(BaseModel):
    endpoint_id: uuid.UUID
    attempts: int
    succeeded: int
    failed: int
    success_rate: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None

    @classmethod
    def from_row(cls, row: EndpointDashboardRow) -> EndpointDashboardOut:
        return cls(
            endpoint_id=row.endpoint_id,
            attempts=row.attempts,
            succeeded=row.succeeded,
            failed=row.failed,
            success_rate=row.success_rate,
            latency_p50_ms=row.latency_p50_ms,
            latency_p95_ms=row.latency_p95_ms,
        )


class DashboardOut(BaseModel):
    """Tenant-scoped throughput, success rate and latency over a recent window."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "tenant_id": "a3d5c1f0-7b2e-4c8d-9e6f-1a2b3c4d5e6f",
                    "window_s": 3600,
                    "window_started_at": "2026-11-06T08:00:00Z",
                    "computed_at": "2026-11-06T09:00:00Z",
                    "sampled": False,
                    "sample_started_at": "2026-11-06T08:00:12Z",
                    "attempts": 420,
                    "succeeded": 400,
                    "retrying": 15,
                    "failed": 5,
                    "success_rate": 0.9524,
                    "throughput_per_minute": 7.0,
                    "latency_p50_ms": 90.0,
                    "latency_p95_ms": 450.0,
                    "latency_p99_ms": 1200.0,
                    "backlog_pending": 3,
                    "backlog_delivering": 1,
                    "endpoints": [],
                }
            ]
        }
    )

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
    throughput_per_minute: float = Field(
        description="Attempts in the window divided by window length in minutes."
    )
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    backlog_pending: int
    backlog_delivering: int
    endpoints: list[EndpointDashboardOut]

    @classmethod
    def from_dashboard(cls, dash: TenantDashboard) -> DashboardOut:
        return cls(
            tenant_id=dash.tenant_id,
            window_s=dash.window_s,
            window_started_at=dash.window_started_at,
            computed_at=dash.computed_at,
            sampled=dash.sampled,
            sample_started_at=dash.sample_started_at,
            attempts=dash.attempts,
            succeeded=dash.succeeded,
            retrying=dash.retrying,
            failed=dash.failed,
            success_rate=dash.success_rate,
            throughput_per_minute=dash.throughput_per_minute,
            latency_p50_ms=dash.latency_p50_ms,
            latency_p95_ms=dash.latency_p95_ms,
            latency_p99_ms=dash.latency_p99_ms,
            backlog_pending=dash.backlog_pending,
            backlog_delivering=dash.backlog_delivering,
            endpoints=[EndpointDashboardOut.from_row(e) for e in dash.endpoints],
        )
