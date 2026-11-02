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
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.models import AttemptOutcome, DeliveryAttempt, DeliveryStatus
from hookrelay.services.delivery_policy import ErrorClass, RetryPolicy, decide_retry


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

    @property
    def lease(self) -> Lease:
        return Lease(self.id, self.lease_token)


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


_LEASE_SQL = text(
    """
    WITH due AS (
        SELECT d.id
        FROM deliveries AS d
        JOIN endpoints AS e ON e.tenant_id = d.tenant_id AND e.id = d.endpoint_id
        JOIN tenants AS t ON t.id = d.tenant_id
        WHERE d.status = 'pending'
          AND d.next_attempt_at <= now()
          AND e.enabled AND e.deleted_at IS NULL
          AND t.status = 'active'
        ORDER BY d.next_attempt_at, d.id
        LIMIT :limit
        FOR UPDATE OF d SKIP LOCKED
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
              d.created_at, ev.event_type, ev.payload, ev.occurred_at,
              e.url, e.headers, e.timeout_ms, e.secret_ciphertext
    """
)

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
    SELECT d.id, d.tenant_id, d.attempt_count, d.created_at, d.leased_by,
           d.last_attempt_at, e.url, now() AS now
    FROM deliveries AS d
    JOIN endpoints AS e ON e.tenant_id = d.tenant_id AND e.id = d.endpoint_id
    WHERE d.status = 'delivering' AND d.leased_until < now()
    ORDER BY d.leased_until
    LIMIT :limit
    FOR UPDATE OF d SKIP LOCKED
    """
)


async def lease_due(
    session: AsyncSession, *, worker_id: str, limit: int, lease_s: float
) -> list[LeasedDelivery]:
    """Lease up to ``limit`` due deliveries for ``worker_id`` (commit to publish the lease).

    Only deliveries of enabled, non-deleted endpoints of active tenants are leased; the others
    wait in ``pending`` (disabling an endpoint pauses it, it does not fail its backlog).
    """
    if limit <= 0:
        return []
    rows = (
        await session.execute(
            _LEASE_SQL, {"limit": limit, "lease_s": float(lease_s), "worker_id": worker_id}
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
        )
        for r in rows
    ]
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
    """Map an attempt record to the delivery's next state (and the final error class)."""
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
    # Out of time: the delivery's reason is max_age_exceeded; the attempt keeps its status code.
    final = decision.error_class or record.error_class
    return Completion(DeliveryStatus.FAILED, AttemptOutcome.FAILED, None), final


async def complete(
    session: AsyncSession,
    delivery: LeasedDelivery,
    record: AttemptRecord,
    *,
    worker_id: str,
    now: datetime,
    policy: RetryPolicy,
) -> Completion | None:
    """Apply one attempt's result under the lease fence; ``None`` if the lease was lost."""
    completion, error_class = plan_completion(
        record,
        attempt_number=delivery.attempt_number,
        created_at=delivery.created_at,
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
    return completion


@dataclass(frozen=True, slots=True)
class ReclaimResult:
    retried: int = 0
    failed: int = 0

    @property
    def total(self) -> int:
        return self.retried + self.failed


async def reclaim_expired(
    session: AsyncSession, *, limit: int, policy: RetryPolicy
) -> ReclaimResult:
    """Turn deliveries whose lease ran out (crashed or partitioned worker) into a recorded
    ``lease_expired`` attempt, then retry them with backoff or fail them like any attempt."""
    rows = (await session.execute(_EXPIRED_SQL, {"limit": limit})).all()
    retried = failed = 0
    for r in rows:
        decision = decide_retry(
            policy=policy,
            attempt_number=r.attempt_count,
            created_at=r.created_at,
            now=r.now,
            retry_after_s=None,
        )
        status = DeliveryStatus.PENDING if decision.retry else DeliveryStatus.FAILED
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
