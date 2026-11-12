"""Group commit for the worker's completion writes (Milestone 9).

Before Milestone 9 every finished attempt paid for its own transaction (``now()``, the fenced
``UPDATE deliveries``, the ``delivery_attempts`` insert, the breaker update, ``COMMIT``): at a
few hundred deliveries/s per worker that is a few hundred commits/s, each a WAL flush and a
pool checkout. :class:`CompletionBatcher` collects finished attempts from the worker's
delivery tasks and hands them to a writer in groups, so one transaction commits many.

The batcher is deliberately generic (``ItemT`` in, ``ResultT`` out per item) and knows nothing
about Postgres: the worker supplies the write function, which owns fencing, retries and
fallbacks. That keeps the flush policy unit-testable without a database.

Flush policy: a batch is written as soon as

* it holds ``max_size`` items, or
* every attempt still in flight is waiting on it (``outstanding()`` items queued: nobody else
  can join, so lingering would only add latency, which is what makes batching free at low
  load), or
* ``linger_s`` has passed since its *oldest* item arrived (bounded extra latency), or
* :meth:`flush_now` / :meth:`aclose` is called (drain).

One writer runs at a time: items that arrive while a batch is being written form the next
batch, so a busy worker's batches grow towards ``max_size`` by themselves (classic group
commit) while an idle one writes each item immediately.

Each submitter awaits its own future. Cancelling a submitter (lease lost, drain timeout) does
not withdraw its item: the attempt already happened, so recording it is still the right
thing to do, and the write is fenced by the lease token anyway.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

import structlog

logger = structlog.stdlib.get_logger("hookrelay.completions")

# The writer returns one entry per item, in order: a result, or the exception for that item.
type WriteFn[ItemT, ResultT] = Callable[
    [Sequence[ItemT]], Awaitable[Sequence[ResultT | BaseException]]
]


@dataclass(frozen=True, slots=True)
class FlushReport:
    """What one flush did (for metrics and logs)."""

    size: int
    seconds: float
    reason: str  # full | idle | linger | drain | closed


@dataclass(slots=True)
class _Pending[ItemT, ResultT]:
    item: ItemT
    future: asyncio.Future[ResultT]
    enqueued_at: float = field(default=0.0)


class CompletionBatcher[ItemT, ResultT]:
    def __init__(
        self,
        write: WriteFn[ItemT, ResultT],
        *,
        max_size: int,
        linger_s: float,
        outstanding: Callable[[], int],
        on_flush: Callable[[FlushReport], None] | None = None,
    ) -> None:
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        if linger_s < 0:
            raise ValueError("linger_s must not be negative")
        self._write = write
        self.max_size = max_size
        self.linger_s = linger_s
        self._outstanding = outstanding
        self._on_flush = on_flush
        self._queue: list[_Pending[ItemT, ResultT]] = []
        self._changed = asyncio.Event()
        self._flush_requested = False
        self._closed = False
        self._writing = 0
        self._idle = asyncio.Event()
        self._idle.set()
        self._task: asyncio.Task[None] | None = None

    # -- introspection ----------------------------------------------------------------------

    @property
    def queued(self) -> int:
        return len(self._queue)

    @property
    def closed(self) -> bool:
        return self._closed

    # -- lifecycle --------------------------------------------------------------------------

    def start(self) -> None:
        if self._task is None and not self._closed:
            self._task = asyncio.create_task(self._run(), name="completion-batcher")

    async def flush_now(self) -> None:
        """Write everything queued right now and wait until it is written (drain)."""
        if self._task is None or self._task.done():
            await self._write_inline(reason="drain")
            return
        while self._queue or self._writing:
            self._flush_requested = True
            self._changed.set()
            self._idle.clear()
            await self._idle.wait()

    async def aclose(self) -> None:
        """Flush whatever is queued, then stop the writer task. Idempotent."""
        if self._closed:
            return
        await self.flush_now()
        self._closed = True
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self._write_inline(reason="closed")  # anything that raced the shutdown

    def notify(self) -> None:
        """Re-evaluate the flush condition (e.g. an in-flight attempt went away)."""
        self._changed.set()

    # -- producers --------------------------------------------------------------------------

    async def submit(self, item: ItemT) -> ResultT:
        """Queue ``item`` for the next batch and wait for its own result (or exception)."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[ResultT] = loop.create_future()
        self._queue.append(_Pending(item, future, loop.time()))
        if self._closed or self._task is None or self._task.done():
            # Not running (closed, or never started): write synchronously, never strand it.
            await self._write_inline(reason="closed")
        else:
            self._changed.set()
        return await future

    # -- the writer -------------------------------------------------------------------------

    def _ready(self, now: float) -> tuple[bool, float | None, str]:
        """Should the head of the queue be written now? Returns (ready, wait_s, reason)."""
        n = len(self._queue)
        if n == 0:
            return False, None, ""
        if n >= self.max_size:
            return True, None, "full"
        if self._flush_requested:
            return True, None, "drain"
        if n >= self._outstanding():
            return True, None, "idle"
        remaining = self._queue[0].enqueued_at + self.linger_s - now
        if remaining <= 0:
            return True, None, "linger"
        return False, remaining, ""

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            ready, wait_s, reason = self._ready(loop.time())
            if not ready:
                if not self._queue:
                    self._flush_requested = False
                    self._idle.set()
                self._changed.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self._changed.wait(), timeout=wait_s)
                continue
            batch = self._queue[: self.max_size]
            del self._queue[: self.max_size]
            await self._flush(batch, reason)

    async def _write_inline(self, *, reason: str) -> None:
        while self._queue:
            batch = self._queue[: self.max_size]
            del self._queue[: self.max_size]
            await self._flush(batch, reason)

    async def _call_writer(
        self, batch: list[_Pending[ItemT, ResultT]]
    ) -> Sequence[ResultT | BaseException]:
        """The writer's per-item results; if the writer itself fails, every item gets its error."""
        try:
            results = await self._write([p.item for p in batch])
        except Exception as exc:
            return [exc] * len(batch)
        if len(results) != len(batch):
            error = RuntimeError(
                f"completion writer returned {len(results)} results for {len(batch)} items"
            )
            return [error] * len(batch)
        return results

    async def _flush(self, batch: list[_Pending[ItemT, ResultT]], reason: str) -> None:
        self._writing += 1
        started = time.perf_counter()
        try:
            results = await self._call_writer(batch)
            for pending, result in zip(batch, results, strict=True):
                if pending.future.done():  # the submitter was cancelled; result is recorded
                    continue
                if isinstance(result, BaseException):
                    pending.future.set_exception(result)
                else:
                    pending.future.set_result(result)
        finally:
            # If the write itself was cancelled (event loop teardown), nobody may be left
            # awaiting forever: the outcome is unknown, which the caller treats like any
            # other failed completion (the lease expires into the reaper).
            for pending in batch:
                if not pending.future.done():
                    pending.future.set_exception(
                        RuntimeError("completion batch write was cancelled; outcome unknown")
                    )
            self._writing -= 1
            report = FlushReport(len(batch), time.perf_counter() - started, reason)
            if self._on_flush is not None:
                try:
                    self._on_flush(report)
                except Exception:  # metrics must never break completions
                    logger.exception("completions.on_flush_failed")
            if not self._queue and not self._writing:
                self._flush_requested = False
                self._idle.set()
