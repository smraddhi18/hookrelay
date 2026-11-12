"""Flush policy of the completion group-commit batcher (no database involved)."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import pytest

from hookrelay.services.completion_batcher import CompletionBatcher, FlushReport


class Writer:
    """Records every batch; results are ``item * 10`` unless told to fail."""

    def __init__(self, *, delay_s: float = 0.0) -> None:
        self.batches: list[list[int]] = []
        self.delay_s = delay_s
        self.fail_with: BaseException | None = None
        self.per_item_errors: dict[int, BaseException] = {}
        self.wrong_length = False

    async def __call__(self, items: Sequence[int]) -> list[int | BaseException]:
        self.batches.append(list(items))
        if self.delay_s:
            await asyncio.sleep(self.delay_s)
        if self.fail_with is not None:
            raise self.fail_with
        if self.wrong_length:
            return []
        return [self.per_item_errors.get(i, i * 10) for i in items]


def _batcher(
    writer: Writer,
    *,
    max_size: int = 4,
    linger_s: float = 10.0,
    outstanding: int = 100,
    reports: list[FlushReport] | None = None,
) -> CompletionBatcher[int, int]:
    batcher: CompletionBatcher[int, int] = CompletionBatcher(
        writer,
        max_size=max_size,
        linger_s=linger_s,
        outstanding=lambda: outstanding,
        on_flush=None if reports is None else reports.append,
    )
    batcher.start()
    return batcher


async def test_a_full_batch_is_written_at_once_and_results_go_to_their_submitters() -> None:
    writer, reports = Writer(), list[FlushReport]()
    batcher = _batcher(writer, max_size=3, reports=reports)
    results = await asyncio.gather(*(batcher.submit(i) for i in range(3)))
    assert results == [0, 10, 20]
    assert writer.batches == [[0, 1, 2]]
    assert [(r.size, r.reason) for r in reports] == [(3, "full")]
    assert reports[0].seconds >= 0
    await batcher.aclose()


async def test_flushes_immediately_when_every_in_flight_attempt_is_waiting() -> None:
    writer, reports = Writer(), list[FlushReport]()
    batcher = _batcher(writer, outstanding=2, reports=reports)  # linger 10 s never matters
    results = await asyncio.wait_for(asyncio.gather(batcher.submit(1), batcher.submit(2)), 1)
    assert list(results) == [10, 20]
    assert [(r.size, r.reason) for r in reports] == [(2, "idle")]
    await batcher.aclose()


async def test_a_partial_batch_waits_at_most_the_linger() -> None:
    writer, reports = Writer(), list[FlushReport]()
    batcher = _batcher(writer, linger_s=0.05, reports=reports)
    loop = asyncio.get_running_loop()
    started = loop.time()
    assert await batcher.submit(7) == 70
    assert loop.time() - started >= 0.04
    assert [(r.size, r.reason) for r in reports] == [(1, "linger")]
    await batcher.aclose()


async def test_items_arriving_during_a_write_form_the_next_batch() -> None:
    writer = Writer(delay_s=0.05)
    batcher = _batcher(writer, max_size=10, linger_s=0.0)
    first = asyncio.create_task(batcher.submit(1))
    await asyncio.sleep(0.01)  # the first write is in progress
    rest = [asyncio.create_task(batcher.submit(i)) for i in (2, 3, 4)]
    assert await asyncio.gather(first, *rest) == [10, 20, 30, 40]
    assert writer.batches == [[1], [2, 3, 4]]
    await batcher.aclose()


async def test_flush_now_writes_everything_queued() -> None:
    writer, reports = Writer(), list[FlushReport]()
    batcher = _batcher(writer, reports=reports)
    pending = [asyncio.create_task(batcher.submit(i)) for i in range(6)]
    await asyncio.sleep(0)
    assert batcher.queued == 6
    await batcher.flush_now()
    assert batcher.queued == 0
    assert [await p for p in pending] == [0, 10, 20, 30, 40, 50]
    assert [(r.size, r.reason) for r in reports] == [(4, "full"), (2, "drain")]
    await batcher.flush_now()  # nothing queued: returns at once
    await batcher.aclose()


async def test_a_cancelled_submitter_keeps_its_item_queued_and_written() -> None:
    writer = Writer()
    batcher = _batcher(writer)
    victim = asyncio.create_task(batcher.submit(5))
    await asyncio.sleep(0)
    victim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await victim
    await batcher.aclose()  # the attempt happened; it is still recorded
    assert writer.batches == [[5]]


async def test_writer_failures_reach_every_submitter_and_per_item_errors_only_theirs() -> None:
    writer = Writer()
    batcher = _batcher(writer, max_size=2)
    writer.per_item_errors[1] = LookupError("fenced")
    ok, bad = await asyncio.gather(batcher.submit(0), batcher.submit(1), return_exceptions=True)
    assert ok == 0
    assert isinstance(bad, LookupError)

    writer.fail_with = ConnectionError("database down")
    results = await asyncio.gather(batcher.submit(2), batcher.submit(3), return_exceptions=True)
    assert all(isinstance(r, ConnectionError) for r in results)

    writer.fail_with, writer.wrong_length = None, True
    results = await asyncio.gather(batcher.submit(4), batcher.submit(5), return_exceptions=True)
    assert all(isinstance(r, RuntimeError) and "0 results" in str(r) for r in results)
    await batcher.aclose()


async def test_a_failing_metrics_hook_never_breaks_completions() -> None:
    def broken(_report: FlushReport) -> None:
        raise ValueError("exporter down")

    batcher: CompletionBatcher[int, int] = CompletionBatcher(
        Writer(), max_size=1, linger_s=0, outstanding=lambda: 1, on_flush=broken
    )
    batcher.start()
    assert await batcher.submit(3) == 30
    await batcher.aclose()


async def test_submit_without_a_running_writer_writes_inline() -> None:
    writer = Writer()
    batcher: CompletionBatcher[int, int] = CompletionBatcher(
        writer, max_size=4, linger_s=1, outstanding=lambda: 99
    )
    assert await batcher.submit(1) == 10  # never started
    batcher.start()
    await batcher.aclose()
    assert batcher.closed
    assert await batcher.submit(2) == 20  # after close
    await batcher.aclose()  # idempotent
    batcher.start()  # no-op once closed
    assert writer.batches == [[1], [2]]


async def test_flush_now_without_a_writer_task_and_notify() -> None:
    writer = Writer()
    batcher: CompletionBatcher[int, int] = CompletionBatcher(
        writer, max_size=4, linger_s=10, outstanding=lambda: 99
    )
    await batcher.flush_now()  # nothing to do, not started
    batcher.notify()
    assert writer.batches == []


async def test_notify_re_evaluates_the_idle_condition() -> None:
    writer = Writer()
    outstanding = [3]
    batcher: CompletionBatcher[int, int] = CompletionBatcher(
        writer, max_size=10, linger_s=10, outstanding=lambda: outstanding[0]
    )
    batcher.start()
    task = asyncio.create_task(batcher.submit(1))
    await asyncio.sleep(0.01)
    assert not task.done()  # two other attempts might still join
    outstanding[0] = 1  # they went away (lease lost / crashed)
    batcher.notify()
    assert await asyncio.wait_for(task, 1) == 10
    await batcher.aclose()


@pytest.mark.parametrize(("max_size", "linger_s"), [(0, 0.0), (1, -0.1)])
def test_invalid_bounds_are_rejected(max_size: int, linger_s: float) -> None:
    with pytest.raises(ValueError, match="must"):
        CompletionBatcher(Writer(), max_size=max_size, linger_s=linger_s, outstanding=lambda: 1)


async def test_a_cancelled_write_never_leaves_a_submitter_waiting_forever() -> None:
    started = asyncio.Event()

    async def hang(items: Sequence[int]) -> list[int]:
        started.set()
        await asyncio.Event().wait()
        return list(items)

    batcher: CompletionBatcher[int, int] = CompletionBatcher(
        hang, max_size=4, linger_s=0, outstanding=lambda: 1
    )
    batcher.start()
    submitter = asyncio.create_task(batcher.submit(7))
    await started.wait()
    assert batcher._task is not None
    batcher._task.cancel()
    with pytest.raises(RuntimeError, match="outcome unknown"):
        await asyncio.wait_for(submitter, timeout=1)
