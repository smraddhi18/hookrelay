"""Liveness vs readiness.

* Liveness (``/health``) answers "is the process able to serve HTTP at all?" and never touches
  dependencies, so a database outage does not make an orchestrator restart-loop healthy pods.
* Readiness (``/ready``) answers "should this instance receive traffic?" and checks Postgres
  with a hard timeout, so a hung connection attempt cannot hang the probe itself.
* Redis (Milestone 6) is reported when configured. Soft by default
  (``REDIS_REQUIRED_FOR_READY=false``): Redis down does not make ``/ready`` 503 because the API
  can still serve and workers degrade to Postgres paths. Set the flag to hard-fail readiness.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import structlog

from hookrelay.redis_.client import RedisFacade, ping_redis

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


async def check_redis(facade: RedisFacade, timeout_s: float) -> CheckResult:
    started = time.perf_counter()
    try:
        await ping_redis(facade, timeout_s=timeout_s)
    except TimeoutError:
        error = f"timed out after {timeout_s:g}s"
    except Exception as exc:
        error = type(exc).__name__
        logger.warning("readiness.redis.failed", error=error, detail=str(exc)[:500])
    else:
        return CheckResult(CheckStatus.UP, _elapsed_ms(started))
    return CheckResult(CheckStatus.DOWN, _elapsed_ms(started), error)


async def readiness(
    db: Pingable,
    *,
    timeout_s: float,
    draining: bool,
    redis: RedisFacade | None = None,
    redis_required: bool = False,
) -> ReadinessReport:
    checks: dict[str, CheckResult] = {"postgres": await check_postgres(db, timeout_s)}
    required = ["postgres"]
    if redis is not None:
        checks["redis"] = await check_redis(redis, timeout_s)
        if redis_required:
            required.append("redis")
        elif checks["redis"].status is CheckStatus.DOWN:
            logger.warning("readiness.redis.degraded", required=False)
    all_required_up = all(checks[name].status is CheckStatus.UP for name in required)
    return ReadinessReport(ready=all_required_up and not draining, draining=draining, checks=checks)


def _elapsed_ms(started: float) -> float:
    return round((time.perf_counter() - started) * 1000, 2)
