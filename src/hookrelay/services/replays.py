"""Replay: put finished deliveries back on the queue, tenant-scoped, audited and rate-limited.

A replay starts a **new run** of an existing delivery instead of creating a new one: status goes
back to ``pending`` (due now), ``attempt_offset`` is set to the attempts made so far and
``run_started_at`` to now, so the retry budget (max attempts, max age) starts fresh while the
attempt log keeps numbering monotonically. The receiver sees the same ``webhook-id`` (the
delivery id) again, which is exactly what its de-duplication should key on.

Rules:

* Only terminal deliveries (``succeeded``, ``failed``, ``dead``) are replayed; ``pending`` and
  ``delivering`` ones are already on their way. Candidate rows are locked ``FOR UPDATE SKIP
  LOCKED`` and re-checked under the lock, so a replay can never race a worker.
* Only deliveries of enabled, non-deleted endpoints are replayed (otherwise they would sit in
  ``pending`` forever, or until someone re-enables the endpoint).
* Every accepted replay writes one ``replay_requests`` row in the same transaction (audit trail),
  and those rows are also the rate limiter's ledger: under a per-tenant advisory lock the
  tenant's rows in the last 60 s are counted. Postgres-only, exact across API replicas.
* A delivery whose event payload was purged by the retention policy cannot be replayed (there
  is nothing to send): single and per-event replays answer ``409 payload_purged`` and bulk
  replays skip such deliveries. See :mod:`hookrelay.services.retention` for the locking.
* A bulk replay is capped at ``REPLAY_MAX_DELIVERIES`` per request (oldest first) and reports
  ``has_more``; the caller pages by narrowing ``created_after``.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog
from sqlalchemy import and_, func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.errors import (
    ConflictError,
    NotFoundError,
    TooManyRequestsError,
    UnprocessableEntityError,
)
from hookrelay.db.models import (
    Delivery,
    DeliveryStatus,
    Endpoint,
    Event,
    ReplayRequest,
    ReplayScope,
)
from hookrelay.services.pagination import Page, paginate

logger = structlog.stdlib.get_logger("hookrelay.replays")

REPLAYABLE_STATUSES = frozenset(
    {DeliveryStatus.SUCCEEDED, DeliveryStatus.FAILED, DeliveryStatus.DEAD}
)
DEFAULT_BULK_STATUSES = (DeliveryStatus.FAILED, DeliveryStatus.DEAD)
RATE_WINDOW_S = 60


@dataclass(frozen=True, slots=True)
class ReplayLimits:
    per_minute: int
    max_deliveries: int
    max_window_s: int


@dataclass(frozen=True, slots=True)
class ReplayFilter:
    statuses: Sequence[DeliveryStatus] = DEFAULT_BULK_STATUSES
    created_after: datetime | None = None
    created_before: datetime | None = None
    endpoint_id: uuid.UUID | None = None
    event_type: str | None = None
    event_id: uuid.UUID | None = None

    def as_json(self) -> dict[str, Any]:
        """The audit-row representation (only the filters that were set)."""
        out: dict[str, Any] = {"statuses": sorted(s.value for s in self.statuses)}
        for name in ("created_after", "created_before"):
            value = getattr(self, name)
            if value is not None:
                out[name] = value.isoformat()
        for name in ("endpoint_id", "event_type", "event_id"):
            value = getattr(self, name)
            if value is not None:
                out[name] = str(value)
        return out


@dataclass(frozen=True, slots=True)
class ReplayResult:
    request: ReplayRequest
    deliveries: list[Delivery] = field(default_factory=list)


_RATE_LOCK_SQL = text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))")
_RATE_COUNT_SQL = text(
    """
    SELECT count(*) AS n,
           min(created_at) AS oldest,
           now() AS now
    FROM replay_requests
    WHERE tenant_id = :tenant_id AND created_at > now() - make_interval(secs => :window_s)
    """
)


async def check_rate_limit(session: AsyncSession, tenant_id: uuid.UUID, *, per_minute: int) -> None:
    """Raise ``429 replay_rate_limited`` when the tenant made ``per_minute`` replays in the last
    minute. The advisory lock serialises one tenant's replays until commit, so two concurrent
    requests cannot both see the last free slot (other tenants are not affected)."""
    await session.execute(_RATE_LOCK_SQL, {"lock_key": f"replay:{tenant_id}"})
    row = (
        await session.execute(
            _RATE_COUNT_SQL, {"tenant_id": tenant_id, "window_s": float(RATE_WINDOW_S)}
        )
    ).one()
    if row.n < per_minute:
        return
    elapsed = (row.now - row.oldest).total_seconds()
    retry_after = max(1, math.ceil(RATE_WINDOW_S - elapsed))
    logger.info("replay.rate_limited", tenant_id=str(tenant_id), retry_after_s=retry_after)
    raise TooManyRequestsError(
        f"At most {per_minute} replay requests per minute; retry later",
        code="replay_rate_limited",
        details={"limit_per_minute": per_minute, "retry_after_s": retry_after},
        headers={"Retry-After": str(retry_after)},
    )


def _requeue_stmt(ids: Sequence[uuid.UUID]) -> Any:
    return (
        update(Delivery)
        .where(Delivery.id.in_(ids))
        .values(
            status=DeliveryStatus.PENDING.value,
            next_attempt_at=func.now(),
            completed_at=None,
            attempt_offset=Delivery.attempt_count,
            run_started_at=func.now(),
            replay_count=Delivery.replay_count + 1,
        )
        .returning(Delivery)
        .execution_options(populate_existing=True, synchronize_session=False)
    )


async def _requeue(session: AsyncSession, ids: Sequence[uuid.UUID]) -> list[Delivery]:
    if not ids:
        return []
    rows = (await session.scalars(_requeue_stmt(ids))).all()
    return sorted(rows, key=lambda d: (d.created_at, d.id))


async def _audit(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    api_key_id: uuid.UUID | None,
    scope: ReplayScope,
    filters: dict[str, Any],
    replayed: int,
    has_more: bool,
) -> ReplayRequest:
    request = ReplayRequest(
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        scope=scope.value,
        filters=filters,
        replayed=replayed,
        has_more=has_more,
    )
    session.add(request)
    await session.flush()
    logger.info(
        "replay.accepted",
        tenant_id=str(tenant_id),
        replay_id=str(request.id),
        scope=scope.value,
        replayed=replayed,
        has_more=has_more,
    )
    return request


def _raise_if_purged(purged_at: datetime | None, event_id: uuid.UUID) -> None:
    if purged_at is not None:
        raise ConflictError(
            "The event's payload was purged by the retention policy; it cannot be replayed",
            code="payload_purged",
            details={"event_id": str(event_id), "payload_purged_at": purged_at.isoformat()},
        )


def _candidates(tenant_id: uuid.UUID, flt: ReplayFilter, limit: int) -> Any:
    """Terminal deliveries of live endpoints matching ``flt``, oldest first, locked."""
    stmt = (
        select(Delivery.id)
        .join(
            Endpoint,
            and_(Endpoint.tenant_id == Delivery.tenant_id, Endpoint.id == Delivery.endpoint_id),
        )
        .where(
            Delivery.tenant_id == tenant_id,
            Delivery.status.in_([s.value for s in flt.statuses]),
            Endpoint.enabled.is_(True),
            Endpoint.deleted_at.is_(None),
        )
    )
    if flt.created_after is not None:
        stmt = stmt.where(Delivery.created_at >= flt.created_after)
    if flt.created_before is not None:
        stmt = stmt.where(Delivery.created_at < flt.created_before)
    if flt.endpoint_id is not None:
        stmt = stmt.where(Delivery.endpoint_id == flt.endpoint_id)
    if flt.event_id is not None:
        stmt = stmt.where(Delivery.event_id == flt.event_id)
    if flt.event_type is not None:
        stmt = stmt.where(Delivery.event_type == flt.event_type)
    # Purged payloads cannot be sent again.
    stmt = stmt.where(
        select(Event.id)
        .where(
            Event.tenant_id == Delivery.tenant_id,
            Event.id == Delivery.event_id,
            Event.payload_purged_at.is_(None),
        )
        .exists()
    )
    return (
        stmt.order_by(Delivery.created_at, Delivery.id)
        .limit(limit)
        .with_for_update(of=Delivery, skip_locked=True)
    )


async def replay_delivery(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    api_key_id: uuid.UUID | None,
    delivery_id: uuid.UUID,
    limits: ReplayLimits,
) -> ReplayResult:
    """Replay one terminal delivery (404 for unknown or another tenant's id)."""
    delivery = await session.scalar(
        select(Delivery)
        .where(Delivery.tenant_id == tenant_id, Delivery.id == delivery_id)
        .with_for_update()
    )
    if delivery is None:
        raise NotFoundError("Delivery not found", code="delivery_not_found")
    if DeliveryStatus(delivery.status) not in REPLAYABLE_STATUSES:
        raise ConflictError(
            "Only succeeded, failed or dead deliveries can be replayed",
            code="delivery_not_replayable",
            details={"status": delivery.status},
        )
    # Read after the delivery lock: a concurrent payload purge holds a share lock on the row
    # until it commits, so this sees its result.
    _raise_if_purged(
        await session.scalar(
            select(Event.payload_purged_at).where(
                Event.tenant_id == tenant_id, Event.id == delivery.event_id
            )
        ),
        delivery.event_id,
    )
    endpoint = await session.scalar(
        select(Endpoint).where(Endpoint.tenant_id == tenant_id, Endpoint.id == delivery.endpoint_id)
    )
    if endpoint is None or not endpoint.enabled or endpoint.deleted_at is not None:
        raise ConflictError(
            "The delivery's endpoint is disabled or deleted; enable it before replaying",
            code="endpoint_unavailable",
            details={"endpoint_id": str(delivery.endpoint_id)},
        )
    await check_rate_limit(session, tenant_id, per_minute=limits.per_minute)
    deliveries = await _requeue(session, [delivery.id])
    request = await _audit(
        session,
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        scope=ReplayScope.DELIVERY,
        filters={"delivery_id": str(delivery_id)},
        replayed=len(deliveries),
        has_more=False,
    )
    return ReplayResult(request, deliveries)


async def replay_event(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    api_key_id: uuid.UUID | None,
    event_id: uuid.UUID,
    statuses: Sequence[DeliveryStatus],
    endpoint_id: uuid.UUID | None,
    limits: ReplayLimits,
) -> ReplayResult:
    """Replay an event's terminal deliveries (optionally one endpoint's, or some statuses)."""
    event = (
        await session.execute(
            select(Event.id, Event.payload_purged_at).where(
                Event.tenant_id == tenant_id, Event.id == event_id
            )
        )
    ).one_or_none()
    if event is None:
        raise NotFoundError("Event not found", code="event_not_found")
    _raise_if_purged(event.payload_purged_at, event_id)
    flt = ReplayFilter(statuses=statuses, endpoint_id=endpoint_id, event_id=event_id)
    return await _replay_matching(
        session,
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        scope=ReplayScope.EVENT,
        flt=flt,
        limits=limits,
    )


async def replay_filtered(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    api_key_id: uuid.UUID | None,
    flt: ReplayFilter,
    limits: ReplayLimits,
) -> ReplayResult:
    """Bulk replay by time range and filters (the "receiver was down for an hour" case)."""
    if flt.created_after is None or flt.created_before is None:
        raise UnprocessableEntityError(
            "created_after and created_before are required", code="replay_window_required"
        )
    if flt.created_before <= flt.created_after:
        raise UnprocessableEntityError(
            "created_before must be after created_after",
            code="invalid_replay_window",
            details=[{"loc": ["body", "created_before"]}],
        )
    window_s = (flt.created_before - flt.created_after).total_seconds()
    if window_s > limits.max_window_s:
        raise UnprocessableEntityError(
            f"The replay window may span at most {limits.max_window_s} seconds",
            code="replay_window_too_large",
            details=[{"loc": ["body", "created_before"]}],
        )
    return await _replay_matching(
        session,
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        scope=ReplayScope.FILTER,
        flt=flt,
        limits=limits,
    )


async def _replay_matching(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    api_key_id: uuid.UUID | None,
    scope: ReplayScope,
    flt: ReplayFilter,
    limits: ReplayLimits,
) -> ReplayResult:
    await check_rate_limit(session, tenant_id, per_minute=limits.per_minute)
    # One extra row tells whether more matched than one request may replay.
    ids = list(
        (await session.scalars(_candidates(tenant_id, flt, limits.max_deliveries + 1))).all()
    )
    has_more = len(ids) > limits.max_deliveries
    deliveries = await _requeue(session, ids[: limits.max_deliveries])
    request = await _audit(
        session,
        tenant_id=tenant_id,
        api_key_id=api_key_id,
        scope=scope,
        filters=flt.as_json(),
        replayed=len(deliveries),
        has_more=has_more,
    )
    return ReplayResult(request, deliveries)


async def list_replays(
    session: AsyncSession, tenant_id: uuid.UUID, *, cursor: str | None, limit: int
) -> Page[ReplayRequest]:
    return await paginate(
        session,
        select(ReplayRequest).where(ReplayRequest.tenant_id == tenant_id),
        created_col=ReplayRequest.created_at,
        id_col=ReplayRequest.id,
        cursor=cursor,
        limit=limit,
    )
