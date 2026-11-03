"""The worker's side of the deliveries table: lease, renew, complete, release, reclaim.

Invariants (each enforced by a WHERE clause or a CHECK, not by convention):

* **Disjoint leases.** :func:`lease_due` locks due rows with ``FOR UPDATE OF d SKIP LOCKED``
  and flips them to ``delivering`` in the same statement, so N workers never lease one row
  twice and never wait on each other.
* **Fencing.** Every lease mints a new ``lease_token``. Renewal, completion and release all
  match ``lease_token = :mine AND status = 'delivering'``; a worker whose lease expired and was
  reclaimed gets zero rows back and must drop its result (``worker.lease_lost``).
* **Attempt accounting.** ``attempt_count`` is incremented when the lease is taken (so a
  delivery that crashes workers still runs out of attempts) and an attempt row is written by
  whichever transition ends the lease: completion, or the reaper for an expired lease. Drain
  release gives the attempt back. Hence, outside ``delivering``,
  ``attempt_count == count(delivery_attempts)``.
* **Per-endpoint back-pressure (Milestone 4).** :func:`lease_due` first locks the
  ``endpoint_states`` rows of endpoints that may start work (breaker closed, or a half-open
  probe; below their in-flight cap; holding a rate-limit token), computes a per-endpoint quota
  and only then leases deliveries, all in one transaction. Because the state row stays locked
  until commit, two workers can never both decide the same endpoint has a free slot, so
  ``max_in_flight`` and ``rate_limit_per_s`` hold across any number of workers.
* **Retry budget per run.** Retry limits count attempts since ``attempt_offset`` and age since
  ``run_started_at``, so a replayed delivery gets a fresh budget while ``attempt_number`` keeps
  numbering the attempt log monotonically.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.models import AttemptOutcome, BreakerState, DeliveryAttempt, DeliveryStatus
from hookrelay.services import circuit_breaker
from hookrelay.services.circuit_breaker import BreakerPolicy
from hookrelay.services.delivery_policy import ErrorClass, RetryPolicy, decide_retry

logger = structlog.stdlib.get_logger("hookrelay.queue")

# In-flight cap for endpoints without max_in_flight when the caller passes none.
DEFAULT_MAX_IN_FLIGHT: Final = 10


@dataclass(frozen=True, slots=True)
class Lease:
    delivery_id: uuid.UUID
    token: uuid.UUID


@dataclass(frozen=True, slots=True)
class LeasedDelivery:
    """Everything one attempt needs, fetched in the same statement that took the lease."""

    id: uuid.UUID
    tenant_id: uuid.UUID
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    lease_token: uuid.UUID
    attempt_number: int
    created_at: datetime
    event_type: str
    payload: Any
    occurred_at: datetime
    url: str
    headers: dict[str, str]
    timeout_ms: int
    secret_ciphertext: str
    # Replay bookkeeping: attempts before this run, and when the run started.
    attempt_offset: int = 0
    run_started_at: datetime | None = None

    @property
    def lease(self) -> Lease:
        return Lease(self.id, self.lease_token)

    @property
    def run_attempt(self) -> int:
        """Attempt number within the current run (what the retry budget counts)."""
        return self.attempt_number - self.attempt_offset


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """The outcome of one attempt, as the worker observed it."""

    status_code: int | None
    error_class: ErrorClass | None
    error_message: str | None
    retryable: bool
    retry_after_s: float | None
    latency_ms: int | None
    request_url: str
    request_headers: dict[str, str]
    response_body: str | None
    response_truncated: bool
    started_at: datetime
    finished_at: datetime


@dataclass(frozen=True, slots=True)
class Completion:
    status: DeliveryStatus
    outcome: AttemptOutcome
    next_attempt_at: datetime | None


# Rate-limit tokens an endpoint holds right now: the stored bucket refilled lazily for the time
# since the last lease, capped at one second's worth (bucket size = rate, i.e. burst = 1 s).
_TOKENS = """
    LEAST(e.rate_limit_per_s::float8,
          COALESCE(s.rate_tokens, e.rate_limit_per_s::float8)
          + e.rate_limit_per_s * GREATEST(0, EXTRACT(EPOCH FROM
                now() - COALESCE(s.rate_refilled_at, now()))))
"""

# Step 1: endpoints that may start work, locked so no other worker decides for them meanwhile.
# Least-recently-leased first, so a deep backlog on one endpoint cannot starve the others.
_ELIGIBLE_SQL = text(
    f"""
    SELECT s.endpoint_id, s.breaker_state,
           COALESCE(e.max_in_flight, :default_max_in_flight) - f.n AS capacity,
           CASE WHEN e.rate_limit_per_s IS NULL THEN NULL ELSE {_TOKENS} END AS tokens,
           q.n AS due
    FROM endpoint_states AS s
    JOIN endpoints AS e ON e.tenant_id = s.tenant_id AND e.id = s.endpoint_id
    JOIN tenants AS t ON t.id = s.tenant_id
    CROSS JOIN LATERAL (
        SELECT count(*) AS n FROM deliveries AS x
        WHERE x.endpoint_id = s.endpoint_id AND x.status = 'delivering'
    ) AS f
    CROSS JOIN LATERAL (
        SELECT count(*) AS n FROM (
            SELECT 1 FROM deliveries AS p
            WHERE p.endpoint_id = s.endpoint_id AND p.status = 'pending'
              AND p.next_attempt_at <= now()
            LIMIT :limit
        ) AS l
    ) AS q
    WHERE e.enabled AND e.deleted_at IS NULL AND t.status = 'active'
      AND q.n > 0
      AND f.n < COALESCE(e.max_in_flight, :default_max_in_flight)
      AND (s.breaker_state = 'closed'
           OR (f.n = 0 AND (s.breaker_state = 'half_open' OR s.open_until <= now())))
      AND (e.rate_limit_per_s IS NULL OR {_TOKENS} >= 1)
    ORDER BY s.last_leased_at ASC NULLS FIRST, s.endpoint_id
    LIMIT :limit
    FOR UPDATE OF s SKIP LOCKED
    """  # noqa: S608 - interpolates a constant SQL fragment, never input
)

# Step 2: lease up to quota due rows per endpoint, oldest-due first within each endpoint.
_LEASE_SQL = text(
    """
    WITH due AS (
        SELECT picked.id
        FROM unnest(CAST(:endpoint_ids AS uuid[]), CAST(:quotas AS int[])) AS a(endpoint_id, quota)
        CROSS JOIN LATERAL (
            SELECT d.id FROM deliveries AS d
            WHERE d.endpoint_id = a.endpoint_id AND d.status = 'pending'
              AND d.next_attempt_at <= now()
            ORDER BY d.next_attempt_at, d.id
            LIMIT a.quota
            FOR UPDATE SKIP LOCKED
        ) AS picked
    )
    UPDATE deliveries AS d
    SET status = 'delivering',
        lease_token = gen_random_uuid(),
        leased_until = now() + make_interval(secs => :lease_s),
        leased_by = :worker_id,
        attempt_count = d.attempt_count + 1,
        last_attempt_at = now()
    FROM due, events AS ev, endpoints AS e
    WHERE d.id = due.id
      AND ev.tenant_id = d.tenant_id AND ev.id = d.event_id
      AND e.tenant_id = d.tenant_id AND e.id = d.endpoint_id
    RETURNING d.id, d.tenant_id, d.event_id, d.endpoint_id, d.lease_token, d.attempt_count,
              d.created_at, d.attempt_offset, d.run_started_at,
              ev.event_type, ev.payload, ev.occurred_at,
              e.url, e.headers, e.timeout_ms, e.secret_ciphertext
    """
)

# Step 3: spend tokens, stamp fairness, and move an open breaker whose probe left to half_open.
_SPEND_SQL = text(
    f"""
    UPDATE endpoint_states AS s
    SET last_leased_at = now(),
        rate_tokens = CASE WHEN e.rate_limit_per_s IS NULL THEN NULL
                           ELSE GREATEST(0, {_TOKENS} - u.used) END,
        rate_refilled_at = CASE WHEN e.rate_limit_per_s IS NULL THEN NULL ELSE now() END,
        breaker_state = CASE WHEN s.breaker_state = 'open' THEN 'half_open'
                             ELSE s.breaker_state END,
        open_until = CASE WHEN s.breaker_state = 'open' THEN NULL ELSE s.open_until END
    FROM unnest(CAST(:endpoint_ids AS uuid[]), CAST(:used AS int[])) AS u(endpoint_id, used),
         endpoints AS e
    WHERE s.endpoint_id = u.endpoint_id
      AND e.tenant_id = s.tenant_id AND e.id = s.endpoint_id
    """  # noqa: S608 - interpolates a constant SQL fragment, never input
)


@dataclass(frozen=True, slots=True)
class EndpointSlot:
    """One eligible endpoint as seen by the lease query."""

    endpoint_id: uuid.UUID
    breaker_state: BreakerState
    capacity: int  # in-flight slots left under max_in_flight
    tokens: float | None  # rate-limit tokens available; None = unlimited
    due: int  # due pending deliveries (capped at the lease limit)

    @property
    def probe(self) -> bool:
        return self.breaker_state is not BreakerState.CLOSED

    @property
    def limit(self) -> int:
        """Most deliveries this endpoint may start in this lease."""
        bound = min(self.capacity, self.due)
        if self.tokens is not None:
            bound = min(bound, math.floor(self.tokens))
        if self.probe:
            bound = min(bound, 1)  # half-open: exactly one probe
        return max(bound, 0)


def allocate(slots: Sequence[EndpointSlot], free: int) -> dict[uuid.UUID, int]:
    """Share ``free`` worker slots across endpoints round-robin, in the given (fairness) order,
    never exceeding an endpoint's own limit."""
    quotas = dict.fromkeys((slot.endpoint_id for slot in slots), 0)
    limits = {slot.endpoint_id: slot.limit for slot in slots}
    remaining = max(free, 0)
    while remaining > 0:
        progressed = False
        for endpoint_id, cap in limits.items():
            if remaining == 0:
                break
            if quotas[endpoint_id] < cap:
                quotas[endpoint_id] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            break
    return {endpoint_id: n for endpoint_id, n in quotas.items() if n > 0}


_RENEW_SQL = text(
    """
    UPDATE deliveries AS d
    SET leased_until = now() + make_interval(secs => :lease_s)
    FROM unnest(CAST(:ids AS uuid[]), CAST(:tokens AS uuid[])) AS l(id, token)
    WHERE d.id = l.id AND d.lease_token = l.token AND d.status = 'delivering'
    RETURNING d.id
    """
)

_COMPLETE_SQL = text(
    """
    UPDATE deliveries
    SET status = :status,
        lease_token = NULL, leased_until = NULL, leased_by = NULL,
        next_attempt_at = COALESCE(:next_attempt_at, next_attempt_at),
        completed_at = CASE WHEN :terminal THEN now() END,
        last_status_code = :status_code,
        last_error_class = :error_class
    WHERE id = :id AND lease_token = :token AND status = 'delivering'
    RETURNING attempt_count
    """
)

_RELEASE_SQL = text(
    """
    UPDATE deliveries AS d
    SET status = 'pending',
        lease_token = NULL, leased_until = NULL, leased_by = NULL,
        attempt_count = d.attempt_count - 1,
        next_attempt_at = now()
    FROM unnest(CAST(:ids AS uuid[]), CAST(:tokens AS uuid[])) AS l(id, token)
    WHERE d.id = l.id AND d.lease_token = l.token AND d.status = 'delivering'
    RETURNING d.id
    """
)

_EXPIRED_SQL = text(
    """
    SELECT d.id, d.tenant_id, d.attempt_count, d.attempt_offset, d.run_started_at,
           d.leased_by, d.last_attempt_at, e.url, now() AS now
    FROM deliveries AS d
    JOIN endpoints AS e ON e.tenant_id = d.tenant_id AND e.id = d.endpoint_id
    WHERE d.status = 'delivering' AND d.leased_until < now()
    ORDER BY d.leased_until
    LIMIT :limit
    FOR UPDATE OF d SKIP LOCKED
    """
)


async def lease_due(
    session: AsyncSession,
    *,
    worker_id: str,
    limit: int,
    lease_s: float,
    default_max_in_flight: int = DEFAULT_MAX_IN_FLIGHT,
) -> list[LeasedDelivery]:
    """Lease up to ``limit`` due deliveries for ``worker_id`` (commit to publish the lease).

    Only deliveries of enabled, non-deleted endpoints of active tenants are leased; the others
    wait in ``pending`` (disabling an endpoint pauses it, it does not fail its backlog). Within
    that, each endpoint is held to its breaker, in-flight cap and rate limit: an endpoint whose
    breaker is open keeps its backlog ``pending`` without spending attempts.
    """
    if limit <= 0:
        return []
    rows = (
        await session.execute(
            _ELIGIBLE_SQL, {"limit": limit, "default_max_in_flight": default_max_in_flight}
        )
    ).all()
    slots = [
        EndpointSlot(
            endpoint_id=r.endpoint_id,
            breaker_state=BreakerState(r.breaker_state),
            capacity=r.capacity,
            tokens=r.tokens,
            due=r.due,
        )
        for r in rows
    ]
    quotas = allocate(slots, limit)
    if not quotas:
        return []
    ids = list(quotas)
    leased_rows = (
        await session.execute(
            _LEASE_SQL,
            {
                "endpoint_ids": ids,
                "quotas": [quotas[i] for i in ids],
                "lease_s": float(lease_s),
                "worker_id": worker_id,
            },
        )
    ).all()
    leased = [
        LeasedDelivery(
            id=r.id,
            tenant_id=r.tenant_id,
            event_id=r.event_id,
            endpoint_id=r.endpoint_id,
            lease_token=r.lease_token,
            attempt_number=r.attempt_count,
            created_at=r.created_at,
            event_type=r.event_type,
            payload=r.payload,
            occurred_at=r.occurred_at,
            url=r.url,
            headers=dict(r.headers or {}),
            timeout_ms=r.timeout_ms,
            secret_ciphertext=r.secret_ciphertext,
            attempt_offset=r.attempt_offset,
            run_started_at=r.run_started_at,
        )
        for r in leased_rows
    ]
    used: dict[uuid.UUID, int] = {}
    for delivery in leased:
        used[delivery.endpoint_id] = used.get(delivery.endpoint_id, 0) + 1
    if used:
        used_ids = list(used)
        await session.execute(
            _SPEND_SQL, {"endpoint_ids": used_ids, "used": [used[i] for i in used_ids]}
        )
        for slot in slots:
            if slot.breaker_state is BreakerState.OPEN and slot.endpoint_id in used:
                logger.info("circuit.half_open", endpoint_id=str(slot.endpoint_id))
    # UPDATE ... RETURNING has no ORDER BY; hand them out oldest-due first anyway.
    leased.sort(key=lambda d: (d.created_at, d.id))
    return leased


def _lease_arrays(leases: Sequence[Lease]) -> dict[str, list[uuid.UUID]]:
    return {"ids": [lease.delivery_id for lease in leases], "tokens": [x.token for x in leases]}


async def renew_leases(
    session: AsyncSession, leases: Sequence[Lease], *, lease_s: float
) -> set[uuid.UUID]:
    """Extend the given leases; returns the delivery ids whose lease this worker still holds."""
    if not leases:
        return set()
    rows = await session.execute(_RENEW_SQL, {**_lease_arrays(leases), "lease_s": float(lease_s)})
    return {r.id for r in rows}


async def release_leases(session: AsyncSession, leases: Sequence[Lease]) -> set[uuid.UUID]:
    """Return unfinished leases to ``pending`` (due now) without spending an attempt."""
    if not leases:
        return set()
    rows = await session.execute(_RELEASE_SQL, _lease_arrays(leases))
    return {r.id for r in rows}


def plan_completion(
    record: AttemptRecord,
    *,
    attempt_number: int,
    created_at: datetime,
    now: datetime,
    policy: RetryPolicy,
) -> tuple[Completion, ErrorClass | None]:
    """Map an attempt record to the delivery's next state (and the final error class).

    ``attempt_number`` and ``created_at`` are the attempt number and start *of the current run*
    (they differ from the delivery's after a replay). A permanent error ends in ``failed``; a
    retryable one that ran out of budget ends in ``dead`` (the dead-letter queue).
    """
    if record.error_class is None:
        return Completion(DeliveryStatus.SUCCEEDED, AttemptOutcome.SUCCEEDED, None), None
    if not record.retryable:
        return Completion(DeliveryStatus.FAILED, AttemptOutcome.FAILED, None), record.error_class
    decision = decide_retry(
        policy=policy,
        attempt_number=attempt_number,
        created_at=created_at,
        now=now,
        retry_after_s=record.retry_after_s,
    )
    if decision.retry:
        return (
            Completion(DeliveryStatus.PENDING, AttemptOutcome.RETRYING, decision.next_attempt_at),
            record.error_class,
        )
    # Out of attempts or time: dead-lettered. The delivery's reason is max_age_exceeded when the
    # age ran out; the attempt keeps its own status code and error class.
    final = decision.error_class or record.error_class
    return Completion(DeliveryStatus.DEAD, AttemptOutcome.FAILED, None), final


async def complete(
    session: AsyncSession,
    delivery: LeasedDelivery,
    record: AttemptRecord,
    *,
    worker_id: str,
    now: datetime,
    policy: RetryPolicy,
    breaker: BreakerPolicy | None = None,
) -> Completion | None:
    """Apply one attempt's result under the lease fence; ``None`` if the lease was lost.

    With a ``breaker`` policy the attempt's availability signal is folded into the endpoint's
    circuit breaker in the same transaction (only if the lease was still ours).
    """
    completion, error_class = plan_completion(
        record,
        attempt_number=delivery.run_attempt,
        created_at=delivery.run_started_at or delivery.created_at,
        now=now,
        policy=policy,
    )
    updated = await session.scalar(
        _COMPLETE_SQL,
        {
            "status": completion.status.value,
            "next_attempt_at": completion.next_attempt_at,
            "terminal": completion.status.is_terminal,
            "status_code": record.status_code,
            "error_class": None if error_class is None else error_class.value,
            "id": delivery.id,
            "token": delivery.lease_token,
        },
    )
    if updated is None:
        return None
    session.add(
        DeliveryAttempt(
            tenant_id=delivery.tenant_id,
            delivery_id=delivery.id,
            attempt_number=delivery.attempt_number,
            outcome=completion.outcome.value,
            status_code=record.status_code,
            error_class=None if error_class is None else error_class.value,
            error_message=record.error_message,
            latency_ms=record.latency_ms,
            request_url=record.request_url,
            request_headers=record.request_headers,
            response_body=record.response_body,
            response_truncated=record.response_truncated,
            worker_id=worker_id,
            started_at=record.started_at,
            finished_at=max(record.finished_at, record.started_at),
            next_attempt_at=completion.next_attempt_at,
        )
    )
    await session.flush()
    if breaker is not None:
        await circuit_breaker.record_outcome(
            session,
            tenant_id=delivery.tenant_id,
            endpoint_id=delivery.endpoint_id,
            signal=circuit_breaker.classify(record.error_class, record.status_code),
            now=now,
            policy=breaker,
        )
    return completion


@dataclass(frozen=True, slots=True)
class ReclaimResult:
    retried: int = 0
    failed: int = 0  # dead-lettered: the lost attempt was the last one the budget allowed

    @property
    def total(self) -> int:
        return self.retried + self.failed


async def reclaim_expired(
    session: AsyncSession, *, limit: int, policy: RetryPolicy
) -> ReclaimResult:
    """Turn deliveries whose lease ran out (crashed or partitioned worker) into a recorded
    ``lease_expired`` attempt, then retry them with backoff or dead-letter them like any
    exhausted attempt. A lost lease says nothing about the receiver, so the breaker is not
    touched."""
    rows = (await session.execute(_EXPIRED_SQL, {"limit": limit})).all()
    retried = failed = 0
    for r in rows:
        decision = decide_retry(
            policy=policy,
            attempt_number=r.attempt_count - r.attempt_offset,
            created_at=r.run_started_at,
            now=r.now,
            retry_after_s=None,
        )
        status = DeliveryStatus.PENDING if decision.retry else DeliveryStatus.DEAD
        error_class = decision.error_class or ErrorClass.LEASE_EXPIRED
        await session.execute(
            text(
                """
                UPDATE deliveries
                SET status = :status,
                    lease_token = NULL, leased_until = NULL, leased_by = NULL,
                    next_attempt_at = COALESCE(:next_attempt_at, next_attempt_at),
                    completed_at = CASE WHEN :terminal THEN now() END,
                    last_status_code = NULL,
                    last_error_class = :error_class
                WHERE id = :id
                """
            ),
            {
                "status": status.value,
                "next_attempt_at": decision.next_attempt_at,
                "terminal": status.is_terminal,
                "error_class": error_class.value,
                "id": r.id,
            },
        )
        started = r.last_attempt_at or r.now
        session.add(
            DeliveryAttempt(
                tenant_id=r.tenant_id,
                delivery_id=r.id,
                attempt_number=r.attempt_count,
                outcome=(
                    AttemptOutcome.RETRYING if decision.retry else AttemptOutcome.FAILED
                ).value,
                error_class=error_class.value,
                error_message=f"lease held by {r.leased_by} expired before the attempt finished",
                request_url=r.url,
                request_headers={},
                worker_id=r.leased_by,
                started_at=started,
                finished_at=max(started, r.now),
                next_attempt_at=decision.next_attempt_at,
            )
        )
        if decision.retry:
            retried += 1
        else:
            failed += 1
    await session.flush()
    return ReclaimResult(retried=retried, failed=failed)
