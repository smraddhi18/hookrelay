"""Per-endpoint circuit breaker: pure transitions plus the Postgres side (Milestone 4).

Why a breaker on top of retries: retries protect one delivery, the breaker protects everyone
else. Without it, an endpoint that is down soaks up worker slots on doomed attempts (each one
waits for a connect timeout) and every one of its deliveries burns its retry budget during the
outage. With it, the endpoint's backlog is simply *not leased* while the breaker is open, so
the outage costs one probe per cooldown instead of N attempts, and no attempts at all from the
backlog.

States (stored in ``endpoint_states``, one row per endpoint)::

    closed --N consecutive failures--> open --cooldown elapsed, 1 probe leased--> half_open
      ^                                  ^                                            |
      |                                  +-------------- probe failed ----------------+
      +------------------------------------------- probe succeeded -------------------+

* Only *availability* signals count (:func:`classify`): timeouts, connection/DNS/TLS/protocol
  errors, a blocked destination, 5xx, 408 and 429. A 400 or 410 is the receiver answering, so it
  is a success for the breaker even though it fails the delivery.
* The cooldown doubles on every consecutive trip (``base * 2**trips``, capped), so a long
  outage is probed less and less often.
* Sustained failure (``consecutive_failures >= threshold`` for ``auto_disable_after_s``)
  disables the endpoint and enqueues an ``endpoint.disabled`` notification in the same
  transaction.

Lock order everywhere is delivery -> endpoint_states -> endpoints, which, together with
``SKIP LOCKED`` in the lease query, rules out deadlocks between workers and the API.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Final

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.core.config import Settings
from hookrelay.db.models import BreakerState
from hookrelay.services import notifications
from hookrelay.services.delivery_policy import (
    HTTP_SERVER_ERROR_MIN,
    ErrorClass,
)

logger = structlog.stdlib.get_logger("hookrelay.circuit")

# Status codes that say "the receiver is unavailable or overloaded", not "bad request".
AVAILABILITY_STATUSES: Final = frozenset({408, 429})
# Signals that say nothing about the receiver: our own key, or our own worker dying.
_NEUTRAL_ERRORS: Final = frozenset({ErrorClass.SECRET_UNAVAILABLE, ErrorClass.LEASE_EXPIRED})
_MAX_COOLDOWN_EXPONENT: Final = 30
ENDPOINT_DISABLED: Final = "endpoint.disabled"


class Signal(StrEnum):
    SUCCESS = "success"
    FAILURE = "failure"
    NEUTRAL = "neutral"


def classify(error_class: ErrorClass | None, status_code: int | None) -> Signal:
    """What one attempt says about the endpoint's availability."""
    if error_class is None:
        return Signal.SUCCESS
    if error_class in _NEUTRAL_ERRORS:
        return Signal.NEUTRAL
    if error_class is ErrorClass.HTTP_STATUS:
        if status_code is not None and (
            status_code >= HTTP_SERVER_ERROR_MIN or status_code in AVAILABILITY_STATUSES
        ):
            return Signal.FAILURE
        return Signal.SUCCESS  # the receiver is up and answered (e.g. 400, 404, 410)
    if error_class is ErrorClass.REDIRECT:
        return Signal.SUCCESS
    return Signal.FAILURE


@dataclass(frozen=True, slots=True)
class BreakerPolicy:
    failure_threshold: int = 5
    cooldown_base_s: float = 30.0
    cooldown_max_s: float = 600.0
    auto_disable_after_s: int = 3 * 86_400

    @classmethod
    def from_settings(cls, settings: Settings) -> BreakerPolicy:
        return cls(
            failure_threshold=settings.breaker_failure_threshold,
            cooldown_base_s=settings.breaker_cooldown_base_s,
            cooldown_max_s=settings.breaker_cooldown_max_s,
            auto_disable_after_s=settings.endpoint_auto_disable_after_s,
        )

    def cooldown(self, trips: int) -> timedelta:
        """Open duration after ``trips`` earlier trips since the breaker last closed."""
        exponent = min(max(trips, 0), _MAX_COOLDOWN_EXPONENT)
        return timedelta(seconds=min(self.cooldown_max_s, self.cooldown_base_s * 2**exponent))


@dataclass(frozen=True, slots=True)
class BreakerSnapshot:
    state: BreakerState
    consecutive_failures: int = 0
    failing_since: datetime | None = None
    opened_at: datetime | None = None
    open_until: datetime | None = None
    trips: int = 0
    last_failure_at: datetime | None = None


CLOSED: Final = BreakerSnapshot(BreakerState.CLOSED)


def on_failure(snap: BreakerSnapshot, *, now: datetime, policy: BreakerPolicy) -> BreakerSnapshot:
    failures = snap.consecutive_failures + 1
    counted = replace(
        snap,
        consecutive_failures=failures,
        failing_since=snap.failing_since or now,
        last_failure_at=now,
    )
    trip = snap.state is BreakerState.HALF_OPEN or (
        snap.state is BreakerState.CLOSED and failures >= policy.failure_threshold
    )
    if not trip:
        # Closed below the threshold, or a straggler from before the trip while open.
        return counted
    return replace(
        counted,
        state=BreakerState.OPEN,
        opened_at=now,
        open_until=now + policy.cooldown(snap.trips),
        trips=snap.trips + 1,
    )


def on_success(snap: BreakerSnapshot) -> BreakerSnapshot:
    return replace(CLOSED, last_failure_at=snap.last_failure_at)


def should_auto_disable(snap: BreakerSnapshot, *, now: datetime, policy: BreakerPolicy) -> bool:
    if policy.auto_disable_after_s <= 0 or snap.failing_since is None:
        return False
    return snap.consecutive_failures >= policy.failure_threshold and (
        now - snap.failing_since
    ) >= timedelta(seconds=policy.auto_disable_after_s)


@dataclass(frozen=True, slots=True)
class OutcomeResult:
    before: BreakerState
    after: BreakerState
    auto_disabled: bool = False


_CLOSE_SQL = text(
    """
    WITH prev AS (
        SELECT endpoint_id, breaker_state FROM endpoint_states
        WHERE endpoint_id = :endpoint_id AND tenant_id = :tenant_id
          AND (breaker_state <> 'closed' OR consecutive_failures > 0)
        FOR UPDATE
    )
    UPDATE endpoint_states AS s
    SET breaker_state = 'closed', consecutive_failures = 0, failing_since = NULL,
        opened_at = NULL, open_until = NULL, trips = 0
    FROM prev
    WHERE s.endpoint_id = prev.endpoint_id
    RETURNING prev.breaker_state
    """
)

_LOCK_SQL = text(
    """
    SELECT breaker_state, consecutive_failures, failing_since, opened_at, open_until, trips,
           last_failure_at
    FROM endpoint_states
    WHERE endpoint_id = :endpoint_id AND tenant_id = :tenant_id
    FOR UPDATE
    """
)

_STORE_SQL = text(
    """
    UPDATE endpoint_states
    SET breaker_state = :state, consecutive_failures = :failures,
        failing_since = :failing_since, opened_at = :opened_at, open_until = :open_until,
        trips = :trips, last_failure_at = :last_failure_at
    WHERE endpoint_id = :endpoint_id
    """
)

_DISABLE_SQL = text(
    """
    UPDATE endpoints
    SET enabled = false, auto_disabled_at = :now, version = version + 1
    WHERE tenant_id = :tenant_id AND id = :endpoint_id AND enabled AND deleted_at IS NULL
    RETURNING url
    """
)

# Only for an endpoint that is currently disabled: re-enabling it is the operator saying "the
# receiver is fixed", so it starts with a closed breaker. Re-sending enabled=true to a healthy
# endpoint must not wipe a failure streak that is still building.
RESET_SQL = text(
    """
    UPDATE endpoint_states AS s
    SET breaker_state = 'closed', consecutive_failures = 0, failing_since = NULL,
        opened_at = NULL, open_until = NULL, trips = 0
    FROM endpoints AS e
    WHERE s.endpoint_id = :endpoint_id AND s.tenant_id = :tenant_id
      AND e.tenant_id = s.tenant_id AND e.id = s.endpoint_id
      AND NOT e.enabled AND e.deleted_at IS NULL
    RETURNING s.endpoint_id
    """
)


async def record_outcome(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    signal: Signal,
    now: datetime,
    policy: BreakerPolicy,
) -> OutcomeResult | None:
    """Fold one attempt's signal into the endpoint's breaker; ``None`` if nothing changed.

    A success on a healthy endpoint is a no-op that takes no lock (the hot path); only the
    first success after failures, or a failure, writes the row.
    """
    log = logger.bind(tenant_id=str(tenant_id), endpoint_id=str(endpoint_id))
    params = {"tenant_id": tenant_id, "endpoint_id": endpoint_id}
    if signal is Signal.NEUTRAL:
        return None
    if signal is Signal.SUCCESS:
        before = await session.scalar(_CLOSE_SQL, params)
        if before is None:
            return None
        if before != BreakerState.CLOSED:
            log.info("circuit.closed", previous=before)
        return OutcomeResult(BreakerState(before), BreakerState.CLOSED)

    row = (await session.execute(_LOCK_SQL, params)).one_or_none()
    if row is None:  # endpoint deleted under us (cascade): nothing to protect
        return None
    snap = BreakerSnapshot(
        state=BreakerState(row.breaker_state),
        consecutive_failures=row.consecutive_failures,
        failing_since=row.failing_since,
        opened_at=row.opened_at,
        open_until=row.open_until,
        trips=row.trips,
        last_failure_at=row.last_failure_at,
    )
    after = on_failure(snap, now=now, policy=policy)
    await session.execute(
        _STORE_SQL,
        {
            **params,
            "state": after.state.value,
            "failures": after.consecutive_failures,
            "failing_since": after.failing_since,
            "opened_at": after.opened_at,
            "open_until": after.open_until,
            "trips": after.trips,
            "last_failure_at": after.last_failure_at,
        },
    )
    if after.state is BreakerState.OPEN and after.trips != snap.trips:
        log.warning(
            "circuit.opened",
            previous=snap.state.value,
            consecutive_failures=after.consecutive_failures,
            trips=after.trips,
            open_until=after.open_until.isoformat() if after.open_until else None,
        )
    disabled = False
    if should_auto_disable(after, now=now, policy=policy):
        disabled = await _auto_disable(session, tenant_id, endpoint_id, after, now=now)
    return OutcomeResult(snap.state, after.state, auto_disabled=disabled)


async def _auto_disable(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    endpoint_id: uuid.UUID,
    snap: BreakerSnapshot,
    *,
    now: datetime,
) -> bool:
    url = await session.scalar(
        _DISABLE_SQL, {"tenant_id": tenant_id, "endpoint_id": endpoint_id, "now": now}
    )
    if url is None:  # already disabled or deleted
        return False
    payload = {
        "endpoint_id": str(endpoint_id),
        "url": url,
        "reason": "sustained_failure",
        "consecutive_failures": snap.consecutive_failures,
        "failing_since": snap.failing_since.isoformat() if snap.failing_since else None,
        "disabled_at": now.isoformat(),
    }
    await notifications.enqueue(
        session,
        tenant_id=tenant_id,
        endpoint_id=endpoint_id,
        kind=ENDPOINT_DISABLED,
        payload=payload,
    )
    logger.warning(
        "endpoint.auto_disabled",
        tenant_id=str(tenant_id),
        endpoint_id=str(endpoint_id),
        consecutive_failures=snap.consecutive_failures,
    )
    return True


async def reset_if_disabled(
    session: AsyncSession, *, tenant_id: uuid.UUID, endpoint_id: uuid.UUID
) -> bool:
    """Close the breaker and forget the failure streak of a disabled endpoint that is about to
    be re-enabled. Returns whether a reset happened."""
    reset = await session.scalar(RESET_SQL, {"tenant_id": tenant_id, "endpoint_id": endpoint_id})
    if reset is not None:
        logger.info("circuit.reset", tenant_id=str(tenant_id), endpoint_id=str(endpoint_id))
    return reset is not None
