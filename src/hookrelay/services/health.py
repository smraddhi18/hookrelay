"""Liveness vs readiness.

* Liveness (``/health``) answers "is the process able to serve HTTP at all?" and never touches
  dependencies, so a database outage does not make an orchestrator restart-loop healthy pods.
* Readiness (``/ready``) answers "should this instance receive traffic?" and checks Postgres
  with a hard timeout, so a hung connection attempt cannot hang the probe itself.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import structlog

logger = structlog.stdlib.get_logger("hookrelay.health")


class Pingable(Protocol):
    async def ping(self, timeout_s: float) -> None: ...


class CheckStatus(StrEnum):
    UP = "up"
    DOWN = "down"


@dataclass(frozen=True, slots=True)
class CheckResult:
    status: CheckStatus
    latency_ms: float
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    ready: bool
    draining: bool
    checks: dict[str, CheckResult]


async def check_postgres(db: Pingable, timeout_s: float) -> CheckResult:
    started = time.perf_counter()
    try:
        await db.ping(timeout_s)
    except TimeoutError:
        error = f"timed out after {timeout_s:g}s"
    except Exception as exc:
        # Only the exception type goes into the response; the message may contain hostnames.
        error = type(exc).__name__
        logger.warning("readiness.postgres.failed", error=error, detail=str(exc)[:500])
    else:
        return CheckResult(CheckStatus.UP, _elapsed_ms(started))
    return CheckResult(CheckStatus.DOWN, _elapsed_ms(started), error)


async def readiness(db: Pingable, *, timeout_s: float, draining: bool) -> ReadinessReport:
    checks = {"postgres": await check_postgres(db, timeout_s)}
    all_up = all(c.status is CheckStatus.UP for c in checks.values())
    return ReadinessReport(ready=all_up and not draining, draining=draining, checks=checks)


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 2)
