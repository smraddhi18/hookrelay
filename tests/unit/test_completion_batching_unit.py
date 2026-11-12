"""Milestone 9 worker wiring for group-committed completions (no database)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.exc import DBAPIError, IntegrityError

from hookrelay import worker as worker_module
from hookrelay.observability.metrics import SERVICE_WORKER, build_metrics
from hookrelay.services import delivery_queue
from hookrelay.services.circuit_breaker import Signal
from hookrelay.services.completion_batcher import FlushReport
from hookrelay.services.delivery_policy import ErrorClass
from hookrelay.services.delivery_queue import AttemptRecord, CompletionItem, breaker_signals
from tests.conftest import make_settings
from tests.unit.test_worker_unit import (
    _completing_worker,
    _delivery,
    _disconnect,
    _FlakyDatabase,
    _worker,
)


def _record(status: int | None = 204, error: ErrorClass | None = None) -> AttemptRecord:
    now = datetime.now(UTC)
    return AttemptRecord(
        status_code=status,
        error_class=error,
        error_message=None,
        retryable=error is not None,
        retry_after_s=None,
        latency_ms=1,
        request_url="http://receiver.test/hook",
        request_headers={},
        response_body=None,
        response_truncated=False,
        started_at=now,
        finished_at=now,
    )


def _item(endpoint_id: uuid.UUID, tenant_id: uuid.UUID, record: AttemptRecord) -> CompletionItem:
    return CompletionItem(_delivery(endpoint_id=endpoint_id, tenant_id=tenant_id), record)


OK = _record()
DOWN = _record(503, ErrorClass.HTTP_STATUS)
NEUTRAL = _record(None, ErrorClass.SECRET_UNAVAILABLE)


def test_breaker_signals_keep_order_per_endpoint_sort_endpoints_and_collapse_successes() -> None:
    tenant = uuid.uuid4()
    low, high = sorted([uuid.uuid4(), uuid.uuid4()])
    items = [
        _item(high, tenant, OK),
        _item(low, tenant, DOWN),
        _item(high, tenant, OK),  # repeated success on high: a no-op, collapsed
        _item(low, tenant, NEUTRAL),  # says nothing about the receiver: dropped
        _item(low, tenant, OK),
        _item(low, tenant, OK),  # collapsed
        _item(low, tenant, DOWN),
        _item(high, tenant, DOWN),
    ]
    assert breaker_signals(items) == [
        (low, tenant, Signal.FAILURE),
        (low, tenant, Signal.SUCCESS),
        (low, tenant, Signal.FAILURE),
        (high, tenant, Signal.SUCCESS),
        (high, tenant, Signal.FAILURE),
    ]
    assert breaker_signals([]) == []
    assert breaker_signals([_item(low, tenant, NEUTRAL)]) == []


def test_batch_size_one_is_the_per_attempt_path(tmp_path: Path) -> None:
    assert _worker(tmp_path, worker_complete_batch_max=1).batcher is None
    batched = _worker(tmp_path, worker_complete_batch_max=8, worker_complete_batch_linger_ms=3)
    assert batched.batcher is not None
    assert (batched.batcher.max_size, batched.batcher.linger_s) == (8, 0.003)


def test_batch_settings_defaults_and_bounds() -> None:
    settings = make_settings()
    assert (settings.worker_complete_batch_max, settings.worker_complete_batch_linger_ms) == (32, 5)
    for bad in ({"worker_complete_batch_max": 0}, {"worker_complete_batch_linger_ms": 1_001}):
        with pytest.raises(ValueError, match="worker_complete_batch"):
            make_settings(**bad)


@pytest.fixture
def fake_now(monkeypatch: pytest.MonkeyPatch) -> None:
    async def now(_session: Any) -> datetime:
        return datetime.now(UTC)

    monkeypatch.setattr(worker_module, "db_now", now)


@pytest.mark.usefixtures("fake_now")
async def test_a_batch_is_retried_on_transient_errors_then_applied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FlakyDatabase(_disconnect())
    worker = _completing_worker(tmp_path, db)
    batches: list[int] = []

    async def fake_many(_session: Any, items: Any, **_k: Any) -> list[Any]:
        batches.append(len(items))
        return ["done"] * len(items)

    monkeypatch.setattr(delivery_queue, "complete_many", fake_many)
    items = [CompletionItem(_delivery(), OK) for _ in range(3)]
    try:
        results = await worker._write_batch(items)
    finally:
        await worker.aclose()
    assert cast("Any", results) == [("done", 1)] * 3
    assert (db.sessions, batches, worker.stats.completion_retries) == (2, [3], 1)
    assert worker.stats.completion_batches == 1


@pytest.mark.usefixtures("fake_now")
async def test_an_exhausted_transient_retry_fails_every_item_of_the_batch(tmp_path: Path) -> None:
    db = _FlakyDatabase(*(_disconnect() for _ in range(3)))
    worker = _completing_worker(tmp_path, db, worker_complete_retries=2)
    try:
        results = await worker._write_batch([CompletionItem(_delivery(), OK) for _ in range(2)])
    finally:
        await worker.aclose()
    assert all(isinstance(r, DBAPIError) for r in results)
    assert (db.sessions, worker.stats.completion_retries, worker.stats.completion_fallbacks) == (
        3,
        2,
        0,
    )


@pytest.mark.usefixtures("fake_now")
async def test_a_non_transient_batch_error_falls_back_to_single_completions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FlakyDatabase(IntegrityError("INSERT", {}, Exception("check violation")))
    worker = _completing_worker(tmp_path, db)
    poisoned = _delivery()

    async def fake_one(_session: Any, delivery: Any, *_a: Any, **_k: Any) -> str:
        if delivery is poisoned:
            raise IntegrityError("INSERT", {}, Exception("check violation"))
        return "single"

    monkeypatch.setattr(delivery_queue, "complete", fake_one)
    items = [CompletionItem(_delivery(), OK), CompletionItem(poisoned, OK)]
    try:
        first, second = await worker._write_batch(items)
    finally:
        await worker.aclose()
    assert cast("Any", first) == ("single", 0)
    assert isinstance(second, IntegrityError)
    assert (worker.stats.completion_fallbacks, worker.stats.completion_batches) == (1, 0)


async def test_flush_reports_and_retries_feed_the_metrics(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    worker._on_flush(FlushReport(size=5, seconds=0.002, reason="full"))  # no metrics: no-op
    metrics = build_metrics(SERVICE_WORKER)
    worker.metrics = metrics
    worker._on_flush(FlushReport(size=5, seconds=0.002, reason="full"))
    worker._on_flush(FlushReport(size=1, seconds=0.001, reason="idle"))
    metrics.record_worker_completion_retry()
    sample = metrics.registry.get_sample_value
    labels = {"service": "worker"}
    assert sample("hookrelay_worker_completion_batch_size_count", labels) == 2
    assert sample("hookrelay_worker_completion_batch_size_sum", labels) == 6
    assert sample("hookrelay_worker_completion_flush_seconds_count", labels) == 2
    assert sample("hookrelay_worker_completion_retries_total", labels) == 1
    await worker.aclose()


async def test_aclose_closes_the_batcher(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    batcher = worker.batcher
    assert batcher is not None
    batcher.start()
    await worker.aclose()
    assert batcher.closed
