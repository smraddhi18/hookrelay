"""Milestone 9: group-committed completions on real Postgres.

The batch path must keep every guarantee of the per-attempt path: each row is fenced by its
own lease token (a lost lease in the middle of a batch skips that row only), a transient
failure re-runs the whole batch idempotently, a poisonous row cannot take the rest down,
drain writes finished attempts instead of releasing (and redelivering) them, and concurrent
batched workers with aggressive lease renewal never deadlock or acknowledge anything twice.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import text

from hookrelay.core.config import Settings
from hookrelay.db.engine import Database
from hookrelay.db.models import DeliveryStatus
from hookrelay.mock_receiver import RunningReceiver
from hookrelay.observability.metrics import SERVICE_WORKER, build_metrics
from hookrelay.services import delivery_queue
from hookrelay.services.clock import db_now
from hookrelay.services.delivery_policy import ErrorClass
from hookrelay.services.delivery_queue import AttemptRecord, CompletionItem, LeasedDelivery
from hookrelay.worker import Worker
from tests.integration.test_chaos import (  # noqa: F401 - fixtures
    all_settled,
    api,
    assert_at_least_once,
    chaos_settings,
    fan_out,
    mock,
    provision,
    publish_reliably,
    query,
    start_pipeline,
    wait_until,
)
from tests.integration.test_worker import db_url, worker_template  # noqa: F401

pytestmark = pytest.mark.integration


def _record(
    url: str,
    *,
    status: int | None = 204,
    error: ErrorClass | None = None,
    retryable: bool = False,
) -> AttemptRecord:
    now = datetime.now(UTC)
    return AttemptRecord(
        status_code=status,
        error_class=error,
        error_message=None if error is None else f"{error.value} {status}",
        retryable=retryable,
        retry_after_s=None,
        latency_ms=3,
        request_url=url,
        request_headers={"content-type": "application/json"},
        response_body=None,
        response_truncated=False,
        started_at=now,
        finished_at=now,
    )


async def _lease(settings: Settings, n: int) -> list[LeasedDelivery]:
    db = Database.from_settings(settings)
    try:
        async with db.session() as session:
            leased = await delivery_queue.lease_due(
                session, worker_id="batch-test", limit=n, lease_s=60, default_max_in_flight=50
            )
    finally:
        await db.dispose()
    assert len(leased) == n
    return leased


async def _steal(db_url: str, delivery_id: uuid.UUID) -> None:
    """What a reaper + another worker's lease look like to the original holder."""
    await query(
        db_url, "UPDATE deliveries SET lease_token = gen_random_uuid() WHERE id = $1", delivery_id
    )


async def _complete_many(
    settings: Settings, items: list[CompletionItem]
) -> list[delivery_queue.Completion | None]:
    db = Database.from_settings(settings)
    worker = Worker(db, settings)
    try:
        async with db.session() as session:
            return await delivery_queue.complete_many(
                session,
                items,
                worker_id="batch-test",
                now=await db_now(session),
                policy=worker.policy,
                breaker=worker.breaker,
            )
    finally:
        await worker.aclose()
        await db.dispose()


# -- the service: complete_many ---------------------------------------------------------------


async def test_a_lost_lease_in_the_middle_of_a_batch_skips_only_that_row(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    tenant, _ = await provision(api, mock, mock.url("/svc"))
    await publish_reliably(api, tenant, 5, prefix="fence")
    settings = chaos_settings()
    await fan_out(settings)
    leased = await _lease(settings, 5)
    await _steal(db_url, leased[2].id)
    url = mock.url("/svc")
    records = [
        _record(url),
        _record(url, status=503, error=ErrorClass.HTTP_STATUS, retryable=True),
        _record(url),  # its lease is gone: must be skipped
        _record(url, status=400, error=ErrorClass.HTTP_STATUS),
        _record(url),
    ]
    results = await _complete_many(
        settings, [CompletionItem(d, r) for d, r in zip(leased, records, strict=True)]
    )

    statuses = [None if c is None else c.status for c in results]
    assert statuses == [
        DeliveryStatus.SUCCEEDED,
        DeliveryStatus.PENDING,
        None,
        DeliveryStatus.FAILED,
        DeliveryStatus.SUCCEEDED,
    ]
    rows = {
        r["id"]: r
        for r in await query(
            db_url,
            "SELECT d.id, d.status, d.lease_token, d.attempt_count, d.last_status_code, "
            "d.last_error_class, d.completed_at, count(a.id) AS attempts "
            "FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id = d.id "
            "GROUP BY d.id",
        )
    }
    fenced = rows[leased[2].id]
    assert (fenced["status"], fenced["attempts"]) == ("delivering", 0)
    assert fenced["lease_token"] is not None  # the thief's lease is untouched
    for delivery, status in zip(leased, statuses, strict=True):
        if status is None:
            continue
        row = rows[delivery.id]
        assert row["status"] == status.value
        assert row["lease_token"] is None
        assert row["attempts"] == row["attempt_count"] == 1
        assert (row["completed_at"] is not None) == status.is_terminal
    assert rows[leased[1].id]["last_status_code"] == 503
    assert rows[leased[3].id]["last_error_class"] == "http_status"


async def test_a_stale_token_and_a_duplicate_of_the_same_delivery_complete_it_exactly_once(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    """Fencing is per (delivery, lease token), not per delivery id.

    A batch can hold an old attempt of a delivery (its lease since reclaimed and re-leased by
    this very worker) next to the current one: only the current token may land. A repeated
    (delivery, token) pair must never produce a second attempt row.
    """
    tenant, _ = await provision(api, mock, mock.url("/dup"))
    await publish_reliably(api, tenant, 2, prefix="dup")
    settings = chaos_settings()
    await fan_out(settings)
    current, other = await _lease(settings, 2)
    stale = dataclasses.replace(current, lease_token=uuid.uuid4())
    url = mock.url("/dup")
    items = [
        CompletionItem(stale, _record(url, status=500, error=ErrorClass.HTTP_STATUS)),
        CompletionItem(current, _record(url)),
        CompletionItem(other, _record(url)),
        CompletionItem(current, _record(url)),  # duplicate of item 1
    ]
    results = await _complete_many(settings, items)

    assert [None if c is None else c.status for c in results] == [
        None,
        DeliveryStatus.SUCCEEDED,
        DeliveryStatus.SUCCEEDED,
        None,
    ]
    rows = await query(
        db_url,
        "SELECT d.id, d.status, d.last_status_code, count(a.id) AS attempts "
        "FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id = d.id GROUP BY d.id",
    )
    by_id = {r["id"]: r for r in rows}
    assert by_id[current.id]["attempts"] == by_id[other.id]["attempts"] == 1
    assert by_id[current.id]["status"] == "succeeded"
    assert by_id[current.id]["last_status_code"] == 204  # the stale 500 never landed


async def test_a_batch_folds_breaker_signals_like_single_completions(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    tenant, [down_id, up_id] = await provision(api, mock, mock.url("/down"), mock.url("/up"))
    await publish_reliably(api, tenant, 3, prefix="breaker")
    settings = chaos_settings(breaker_failure_threshold=3)
    await fan_out(settings)
    leased = await _lease(settings, 6)
    items = []
    for d in leased:
        if str(d.endpoint_id) == down_id:
            record = _record(d.url, status=503, error=ErrorClass.HTTP_STATUS, retryable=True)
        else:
            record = _record(d.url)
        items.append(CompletionItem(d, record))
    await _complete_many(settings, items)

    states = {
        str(r["endpoint_id"]): r
        for r in await query(
            db_url,
            "SELECT endpoint_id, breaker_state, consecutive_failures, trips FROM endpoint_states",
        )
    }
    # Three failures in one batch count as three, in order: the breaker opens exactly once.
    assert dict(states[down_id]) | {"endpoint_id": None} == {
        "endpoint_id": None,
        "breaker_state": "open",
        "consecutive_failures": 3,
        "trips": 1,
    }
    assert (states[up_id]["breaker_state"], states[up_id]["consecutive_failures"]) == ("closed", 0)


# -- the worker: group commit end to end ------------------------------------------------------


async def test_worker_batch_with_a_fenced_out_lease_records_the_rest(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _ = await provision(api, mock, mock.url("/batch"))
    await publish_reliably(api, tenant, 6, prefix="wfence")
    settings = chaos_settings(worker_complete_batch_max=32, worker_complete_batch_linger_ms=1000)
    await fan_out(settings)
    real = delivery_queue.complete_many
    stolen: list[uuid.UUID] = []

    async def steal_one_then_write(session: Any, items: Any, **kwargs: Any) -> Any:
        if not stolen:
            stolen.append(items[len(items) // 2].delivery.id)
            await _steal(db_url, stolen[0])
        return await real(session, items, **kwargs)

    monkeypatch.setattr(delivery_queue, "complete_many", steal_one_then_write)
    metrics = build_metrics(SERVICE_WORKER)
    db = Database.from_settings(settings)
    worker = Worker(db, settings, metrics=metrics)
    try:
        stats = await worker.run_once()
    finally:
        await worker.aclose()
        await db.dispose()

    assert (stats.succeeded, stats.lease_lost, stats.loop_errors) == (5, 1, 0)
    assert stats.completion_batches == 1  # all six attempts went out in one transaction
    sample = metrics.registry.get_sample_value
    assert sample("hookrelay_worker_completion_batch_size_count", {"service": "worker"}) == 1
    assert sample("hookrelay_worker_completion_batch_size_sum", {"service": "worker"}) == 6
    assert sample("hookrelay_worker_completion_flush_seconds_count", {"service": "worker"}) == 1
    [row] = await query(
        db_url,
        "SELECT count(*) FILTER (WHERE status = 'succeeded') AS ok, "
        "count(*) FILTER (WHERE status = 'delivering') AS leased, "
        "(SELECT count(*) FROM delivery_attempts) AS attempts FROM deliveries",
    )
    assert (row["ok"], row["leased"], row["attempts"]) == (5, 1, 5)


async def test_an_ambiguous_batch_commit_is_retried_without_applying_anything_twice(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The commit lands but its acknowledgement is lost: the retried batch is fenced out row by
    row, so nothing is written twice and nothing is redelivered."""
    tenant, _ = await provision(api, mock, mock.url("/ambiguous"))
    await publish_reliably(api, tenant, 4, prefix="ambiguous")
    settings = chaos_settings(worker_complete_batch_linger_ms=1000)
    await fan_out(settings)
    real = delivery_queue.complete_many
    calls = Counter[str]()

    async def commit_then_lose_the_ack(session: Any, items: Any, **kwargs: Any) -> Any:
        calls["complete_many"] += 1
        result = await real(session, items, **kwargs)
        if calls["complete_many"] == 1:
            await session.commit()
            raise ConnectionResetError("connection lost while reading the COMMIT reply")
        return result

    monkeypatch.setattr(delivery_queue, "complete_many", commit_then_lose_the_ack)
    db = Database.from_settings(settings)
    worker = Worker(db, settings)
    try:
        stats = await worker.run_once()
    finally:
        await worker.aclose()
        await db.dispose()

    assert calls["complete_many"] == 2
    # Every row of the retry is fenced out: our own earlier commit, reported as lost leases.
    assert (stats.completion_retries, stats.lease_lost, stats.loop_errors) == (1, 4, 0)
    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.deliveries, semantics.attempts, semantics.acked_duplicates) == (4, 4, 0)


async def test_a_transient_failure_retries_the_whole_batch(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _ = await provision(api, mock, mock.url("/storm"))
    await publish_reliably(api, tenant, 6, prefix="transient")
    settings = chaos_settings(worker_complete_batch_linger_ms=1000)
    await fan_out(settings)
    real = delivery_queue.complete_many
    calls = Counter[str]()

    async def killed_then_real(session: Any, items: Any, **kwargs: Any) -> Any:
        calls["complete_many"] += 1
        calls["items"] += len(items)
        if calls["complete_many"] == 1:
            pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            await query(db_url, "SELECT pg_terminate_backend($1)", pid)
        return await real(session, items, **kwargs)

    monkeypatch.setattr(delivery_queue, "complete_many", killed_then_real)
    db = Database.from_settings(settings)
    worker = Worker(db, settings)
    try:
        stats = await worker.run_once()
    finally:
        await worker.aclose()
        await db.dispose()

    assert (calls["complete_many"], calls["items"]) == (2, 12)  # one batch of 6, written twice
    assert (stats.succeeded, stats.completion_retries, stats.loop_errors) == (6, 1, 0)
    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.attempts, semantics.acked_duplicates, semantics.lease_expired) == (6, 0, 0)


async def test_a_poisonous_row_falls_back_to_one_transaction_per_attempt(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant, _ = await provision(api, mock, mock.url("/poison"))
    await publish_reliably(api, tenant, 4, prefix="poison")
    settings = chaos_settings(worker_complete_batch_linger_ms=1000)
    await fan_out(settings)
    real_complete = delivery_queue.complete
    poisoned: list[uuid.UUID] = []

    async def batch_rejected(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("check constraint violated")  # not transient

    async def one_row_rejected(session: Any, delivery: Any, *args: Any, **kwargs: Any) -> Any:
        if not poisoned:
            poisoned.append(delivery.id)
        if delivery.id == poisoned[0]:
            raise RuntimeError("check constraint violated")
        return await real_complete(session, delivery, *args, **kwargs)

    monkeypatch.setattr(delivery_queue, "complete_many", batch_rejected)
    monkeypatch.setattr(delivery_queue, "complete", one_row_rejected)
    db = Database.from_settings(settings)
    worker = Worker(db, settings)
    try:
        stats = await worker.run_once()
    finally:
        await worker.aclose()
        await db.dispose()

    # Only the culprit fails (its lease is left for the reaper); the other three are recorded.
    assert (stats.completion_fallbacks, stats.succeeded, stats.loop_errors) == (1, 3, 1)
    rows = await query(db_url, "SELECT id, status FROM deliveries")
    assert {r["id"]: r["status"] for r in rows}[poisoned[0]] == "delivering"
    assert Counter(r["status"] for r in rows) == {"succeeded": 3, "delivering": 1}


async def test_drain_writes_finished_attempts_instead_of_releasing_them(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    tenant, [fast_id, slow_id] = await provision(
        api, mock, mock.url("/fast", latency_ms=300), mock.url("/slow", latency_ms=60_000)
    )
    await publish_reliably(api, tenant, 4, prefix="drain")
    # A long linger and slow attempts still in flight keep the fast results queued. The fast
    # receiver answers after 300 ms so the slow attempts are surely in flight by then (otherwise
    # the batcher would rightly flush the fast results as soon as nothing else is outstanding).
    settings = chaos_settings(worker_complete_batch_linger_ms=1000, worker_drain_timeout_s=0.1)
    await fan_out(settings)
    running = await start_pipeline(settings, relay=False)
    recorder = mock.app.recorder
    batcher = running.worker.batcher
    assert batcher is not None
    try:

        async def fast_queued_slow_held() -> bool:
            # The receiver counts an ack before the worker has read the response, so wait for
            # the worker side too: all four fast results acknowledged and queued, not written.
            fast = sum(n for (path, _), n in recorder.acked.items() if path == "/fast")
            slow = sum(n for (path, _), n in recorder.received.items() if path == "/slow")
            return fast == 4 and slow == 4 and batcher.queued == 4

        await wait_until(fast_queued_slow_held, within_s=10, interval=0.01)
    finally:
        await running.stop()

    stats = running.worker.stats
    assert (stats.succeeded, stats.released) == (4, 4)
    rows = await query(
        db_url,
        "SELECT d.endpoint_id, d.status, d.attempt_count, count(a.id) AS attempts "
        "FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id = d.id "
        "GROUP BY d.id",
    )
    by_endpoint: dict[str, Counter[tuple[str, int, int]]] = {}
    for r in rows:
        key = (r["status"], r["attempt_count"], r["attempts"])
        by_endpoint.setdefault(str(r["endpoint_id"]), Counter())[key] += 1
    assert by_endpoint[fast_id] == {("succeeded", 1, 1): 4}  # written, never redelivered
    assert by_endpoint[slow_id] == {("pending", 0, 0): 4}  # released, attempt refunded
    assert recorder.stats()["acked_duplicates"] == 0


async def test_concurrent_batched_workers_with_constant_renewals_never_deadlock(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    """Renewals and batched completions lock overlapping delivery rows; both lock in id
    order, and breaker rows in endpoint order, so three workers renewing every 50 ms while
    group-committing never hit a deadlock (it would surface as a loop error or a fallback)."""
    tenant, _ = await provision(
        api,
        mock,
        *(mock.url(f"/c{i}", latency_ms=15, jitter_ms=15) for i in range(4)),
        mock.url("/flaky", failure_rate=0.3, failure_status=503),
    )
    await publish_reliably(api, tenant, 40, prefix="deadlock")
    settings = chaos_settings(
        worker_lease_renew_interval_s=0.05,
        worker_complete_batch_linger_ms=20,
        worker_concurrency=24,
    )
    await fan_out(settings)
    pipelines = [await start_pipeline(settings, relay=False) for _ in range(3)]
    try:
        await wait_until(lambda: all_settled(db_url, 200), within_s=60)
    finally:
        for p in pipelines:
            await p.stop()

    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.deliveries, semantics.acked_duplicates, semantics.lease_expired) == (
        200,
        0,
        0,
    )
    for p in pipelines:
        stats = p.worker.stats
        assert (stats.loop_errors, stats.completion_fallbacks, stats.lease_lost) == (0, 0, 0)
    assert sum(p.worker.stats.completion_batches for p in pipelines) < sum(
        p.worker.stats.succeeded + p.worker.stats.retried for p in pipelines
    )
