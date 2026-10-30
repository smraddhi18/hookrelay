"""Response models shared by the operational endpoints."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class LivenessResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ok"] = "ok"
    service: str
    version: str


class DependencyCheck(BaseModel):
    status: Literal["up", "down"]
    latency_ms: float
    error: str | None = None


class ReadinessResponse(BaseModel):
    status: Literal["ready", "not_ready"]
    draining: bool
    checks: dict[str, DependencyCheck]
