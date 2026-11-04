"""Delivery worker: ``python -m hookrelay.worker`` (or ``hookrelay-worker``).

A separate, horizontally scalable process that turns ``pending`` deliveries into signed HTTP
requests:

1. **lease** due deliveries with ``FOR UPDATE SKIP LOCKED`` (only as many as there are free
   concurrency slots, so one worker never hoards work it cannot start), each with a fresh
   fencing token, and only within each endpoint's circuit breaker, ``max_in_flight`` cap and
   ``rate_limit_per_s`` token bucket (enforced in Postgres across all workers);
2. **deliver** each one in its own task, bounded by a semaphore of ``WORKER_CONCURRENCY``:
   decrypt the endpoint secret, sign (Standard Webhooks), POST through the SSRF-guarded client;
3. **complete** under the fence: success, retry with jittered backoff (honouring a capped
   ``Retry-After``), ``failed`` (permanent error) or ``dead`` (budget exhausted), plus an
   attempt-log row and the endpoint's circuit-breaker update, in one transaction.

Background loops renew in-flight leases every ``WORKER_LEASE_RENEW_INTERVAL_S`` (a task whose
lease was lost is cancelled) and reclaim leases that expired on crashed workers.

``SIGTERM``/``SIGINT`` drain: stop leasing, give in-flight attempts ``WORKER_DRAIN_TIMEOUT_S``
to finish, then cancel the rest and release their leases (without spending an attempt) so
another worker picks them up immediately. A heartbeat file is touched every loop;
``--healthcheck`` checks its age.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import signal
import socket
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

from hookrelay.core.config import Settings, get_settings
from hookrelay.core.logging import configure_logging, redact_text
from hookrelay.core.security import SecretBox, SecretDecryptionError
from hookrelay.core.signing import HEADER_SIGNATURE, sign
from hookrelay.db.engine import Database
from hookrelay.db.models import DeliveryStatus
from hookrelay.relay import heartbeat_is_fresh
from hookrelay.services import delivery_queue
from hookrelay.services.circuit_breaker import BreakerPolicy
from hookrelay.services.clock import db_now
from hookrelay.services.delivery_policy import (
    ErrorClass,
    RetryPolicy,
    is_retryable_error,
    truncate_message,
)
from hookrelay.services.delivery_queue import AttemptRecord, LeasedDelivery
from hookrelay.services.endpoints import secret_aad
from hookrelay.services.ssrf import AddressPolicy, Resolver, system_resolver
from hookrelay.services.webhook_client import (
    USER_AGENT,
    SenderConfig,
    SendResult,
    WebhookRequest,
    WebhookSender,
)

logger = structlog.stdlib.get_logger("hookrelay.worker")

REDACTED_SIGNATURE = "v1,[redacted]"
MASKED_HEADER_VALUE = "********"
CONTENT_TYPE = "application/json"
RECLAIM_BATCH_SIZE = 100


@dataclass(slots=True)
class WorkerStats:
    """Monotonic counters (the shape a Prometheus exporter will publish in a later milestone)."""

    leased: int = 0
    succeeded: int = 0
    retried: int = 0
    failed: int = 0  # ended the run: permanent error (failed) or budget exhausted (dead)
    dead_lettered: int = 0  # the subset of ``failed`` that went to the dead-letter queue
    lease_lost: int = 0
    released: int = 0
    reclaimed_retried: int = 0
    reclaimed_failed: int = 0
    loop_errors: int = 0


def default_worker_id() -> str:
    """``host:pid:random``: unique per process, readable in attempt logs."""
    return f"{socket.gethostname()[:80]}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def render_body(delivery: LeasedDelivery) -> bytes:
    """Standard Webhooks payload ``{"type", "timestamp", "data"}`` as compact, sorted JSON.

    The bytes are rendered once per attempt and signed as-is; receivers must verify the raw
    bytes, not a re-serialisation.
    """
    occurred = delivery.occurred_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    envelope = {"type": delivery.event_type, "timestamp": occurred, "data": delivery.payload}
    return json.dumps(
        envelope, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode()


def _local_failure(error_class: ErrorClass, message: str) -> SendResult:
    """An attempt that failed before any request was sent."""
    return SendResult(
        status_code=None,
        error_class=error_class,
        error_message=truncate_message(message),
        latency_ms=None,
        response_body=None,
        response_truncated=False,
        retry_after_s=None,
    )


def loggable_headers(headers: dict[str, str], custom: dict[str, str]) -> dict[str, str]:
    """What the attempt log stores: custom header values masked, the signature redacted."""
    custom_names = {name.lower() for name in custom}
    stored: dict[str, str] = {}
    for name, value in headers.items():
        lowered = name.lower()
        if lowered == HEADER_SIGNATURE:
            stored[lowered] = REDACTED_SIGNATURE
        elif lowered in custom_names:
            stored[lowered] = MASKED_HEADER_VALUE
        else:
            stored[lowered] = value
    return stored


@dataclass(slots=True)
class _InFlight:
    delivery: LeasedDelivery
    task: asyncio.Task[None]
    settled: bool = False  # the fenced completion committed; the lease no longer exists


class Worker:
    def __init__(
        self,
        database: Database,
        settings: Settings,
        *,
        resolver: Resolver = system_resolver,
        sender: WebhookSender | None = None,
    ) -> None:
        self.database = database
        self.settings = settings
        self.worker_id = settings.worker_id or default_worker_id()
        self.stats = WorkerStats()
        self.policy = RetryPolicy(
            max_attempts=settings.delivery_max_attempts,
            max_age=timedelta(seconds=settings.delivery_max_age_s),
            backoff_base_s=settings.delivery_backoff_base_s,
            backoff_max_s=settings.delivery_backoff_max_s,
            retry_after_max_s=settings.delivery_retry_after_max_s,
        )
        self.breaker = BreakerPolicy.from_settings(settings)
        self.sender = sender or WebhookSender(
            SenderConfig(
                allow_http=settings.endpoint_allow_http,
                allowed_ports=settings.delivery_allowed_ports,
                connect_timeout_s=settings.delivery_connect_timeout_ms / 1000,
                response_max_bytes=settings.delivery_response_max_bytes,
                retry_after_max_s=settings.delivery_retry_after_max_s,
            ),
            policy=AddressPolicy(allow_private_networks=settings.delivery_allow_private_networks),
            resolver=resolver,
            max_connections=settings.worker_concurrency,
        )
        self._secrets = SecretBox.from_settings(settings)
        self._slots = asyncio.BoundedSemaphore(settings.worker_concurrency)
        self._inflight: dict[uuid.UUID, _InFlight] = {}
        self._slot_freed = asyncio.Event()
        self._stop = asyncio.Event()
        self._heartbeat = Path(settings.worker_heartbeat_file)
        self._heartbeat_warned = False

    # -- lifecycle --------------------------------------------------------------------------

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    @property
    def in_flight(self) -> int:
        return len(self._inflight)

    def stop(self) -> None:
        if not self._stop.is_set():
            logger.info("worker.stopping", in_flight=self.in_flight)
        self._stop.set()

    async def aclose(self) -> None:
        await self.sender.aclose()

    # -- leasing ----------------------------------------------------------------------------

    @property
    def free_slots(self) -> int:
        return self.settings.worker_concurrency - len(self._inflight)

    async def poll_once(self) -> int:
        """Lease up to the number of free slots and start one task per delivery."""
        free = min(self.free_slots, self.settings.worker_batch_size)
        if free <= 0 or self.stopping:
            return 0
        async with self.database.session() as session:
            leased = await delivery_queue.lease_due(
                session,
                worker_id=self.worker_id,
                limit=free,
                lease_s=self.settings.worker_lease_s,
                default_max_in_flight=self.settings.endpoint_default_max_in_flight,
            )
        for delivery in leased:
            await self._slots.acquire()  # never blocks: we leased at most `free`
            task = asyncio.create_task(self._run_attempt(delivery), name=f"delivery-{delivery.id}")
            self._inflight[delivery.id] = _InFlight(delivery, task)
        self.stats.leased += len(leased)
        if leased:
            logger.debug("worker.leased", count=len(leased))
        return len(leased)

    async def _run_attempt(self, delivery: LeasedDelivery) -> None:
        try:
            await self.deliver(delivery)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # The lease stays; if it is not completed it expires and the reaper records it.
            self.stats.loop_errors += 1
            logger.exception(
                "worker.attempt_crashed",
                delivery_id=str(delivery.id),
                error=redact_text(type(exc).__name__),
            )
        finally:
            self._inflight.pop(delivery.id, None)
            self._slots.release()
            self._slot_freed.set()

    # -- one attempt ------------------------------------------------------------------------

    def _previous_secret(self, delivery: LeasedDelivery) -> tuple[str, ...]:
        """The replaced secret during a rotation's overlap window, as extra signing keys.

        Best effort: if it cannot be decrypted (e.g. its encryption key was retired) the
        attempt is still signed with the current secret, which receivers must move to anyway.
        """
        if delivery.previous_secret_ciphertext is None:
            return ()
        try:
            return (
                self._secrets.decrypt(
                    delivery.previous_secret_ciphertext,
                    aad=secret_aad(delivery.tenant_id, delivery.endpoint_id),
                ),
            )
        except SecretDecryptionError as exc:
            logger.warning(
                "worker.previous_secret_unavailable",
                endpoint_id=str(delivery.endpoint_id),
                error=str(exc),
            )
            return ()

    def _build_request(
        self, delivery: LeasedDelivery, secret: str, *previous: str
    ) -> WebhookRequest:
        body = render_body(delivery)
        signed = sign(secret, str(delivery.event_id), int(time.time()), body, *previous)
        headers = {
            **delivery.headers,
            "content-type": CONTENT_TYPE,
            "user-agent": USER_AGENT,
            **signed.as_dict(),
        }
        return WebhookRequest(
            url=delivery.url, headers=headers, body=body, timeout_s=delivery.timeout_ms / 1000
        )

    async def attempt(self, delivery: LeasedDelivery) -> AttemptRecord:
        """Sign and send one attempt; never raises for receiver or network failures."""
        started = datetime.now(UTC)
        if delivery.payload is None:
            # The retention policy purged the payload (a replay raced the purge): there is
            # nothing to send, and retrying cannot help.
            result = _local_failure(ErrorClass.PAYLOAD_PURGED, "event payload was purged")
            return self._record(delivery, result, {}, started)
        try:
            secret = self._secrets.decrypt(
                delivery.secret_ciphertext, aad=secret_aad(delivery.tenant_id, delivery.endpoint_id)
            )
        except SecretDecryptionError as exc:
            result = _local_failure(
                ErrorClass.SECRET_UNAVAILABLE, f"signing secret unavailable: {exc}"
            )
            headers: dict[str, str] = {}
        else:
            request = self._build_request(delivery, secret, *self._previous_secret(delivery))
            headers = loggable_headers(dict(request.headers), delivery.headers)
            result = await self.sender.send(request, now=started)
        return self._record(delivery, result, headers, started)

    def _record(
        self,
        delivery: LeasedDelivery,
        result: SendResult,
        headers: dict[str, str],
        started: datetime,
    ) -> AttemptRecord:
        retryable = result.error_class is not None and is_retryable_error(
            result.error_class, result.status_code
        )
        return AttemptRecord(
            status_code=result.status_code,
            error_class=result.error_class,
            error_message=None
            if result.error_message is None
            else redact_text(result.error_message),
            retryable=retryable,
            retry_after_s=result.retry_after_s,
            latency_ms=result.latency_ms,
            request_url=delivery.url,
            request_headers=headers,
            response_body=result.response_body,
            response_truncated=result.response_truncated,
            started_at=started,
            finished_at=datetime.now(UTC),
        )

    async def deliver(self, delivery: LeasedDelivery) -> delivery_queue.Completion | None:
        record = await self.attempt(delivery)
        async with self.database.session() as session:
            now = await db_now(session)
            completion = await delivery_queue.complete(
                session,
                delivery,
                record,
                worker_id=self.worker_id,
                now=now,
                policy=self.policy,
                breaker=self.breaker,
            )
        flight = self._inflight.get(delivery.id)
        if flight is not None:
            flight.settled = True
        fields: dict[str, Any] = {
            "delivery_id": str(delivery.id),
            "tenant_id": str(delivery.tenant_id),
            "event_id": str(delivery.event_id),
            "endpoint_id": str(delivery.endpoint_id),
            "attempt": delivery.attempt_number,
            "status_code": record.status_code,
            "error_class": None if record.error_class is None else record.error_class.value,
            "latency_ms": record.latency_ms,
        }
        if completion is None:
            self.stats.lease_lost += 1
            logger.warning("worker.lease_lost", **fields)
            return None
        match completion.outcome.value:
            case "succeeded":
                self.stats.succeeded += 1
            case "retrying":
                self.stats.retried += 1
            case _:
                self.stats.failed += 1
                if completion.status is DeliveryStatus.DEAD:
                    self.stats.dead_lettered += 1
        logger.info(
            "worker.attempt",
            outcome=completion.outcome.value,
            status=completion.status.value,
            next_attempt_at=None
            if completion.next_attempt_at is None
            else completion.next_attempt_at.isoformat(),
            **fields,
        )
        return completion

    # -- background maintenance -------------------------------------------------------------

    async def renew_once(self) -> int:
        """Extend every in-flight lease; cancel attempts whose lease was taken over."""
        current = [f for f in self._inflight.values() if not f.settled]
        if not current:
            return 0
        async with self.database.session() as session:
            held = await delivery_queue.renew_leases(
                session, [f.delivery.lease for f in current], lease_s=self.settings.worker_lease_s
            )
        lost = 0
        for flight in current:
            if flight.delivery.id not in held and not flight.settled and not flight.task.done():
                lost += 1
                self.stats.lease_lost += 1
                logger.warning("worker.lease_lost", delivery_id=str(flight.delivery.id))
                flight.task.cancel()
        return lost

    async def reclaim_once(self) -> delivery_queue.ReclaimResult:
        async with self.database.session() as session:
            result = await delivery_queue.reclaim_expired(
                session, limit=RECLAIM_BATCH_SIZE, policy=self.policy
            )
        self.stats.reclaimed_retried += result.retried
        self.stats.reclaimed_failed += result.failed
        if result.total:
            logger.warning("worker.leases_reclaimed", retried=result.retried, failed=result.failed)
        return result

    async def _every(self, interval_s: float, name: str, fn: Any) -> None:
        while True:
            await asyncio.sleep(interval_s)
            try:
                await fn()
            except Exception as exc:
                self.stats.loop_errors += 1
                logger.exception(f"worker.{name}_failed", error=type(exc).__name__)

    # -- drain ------------------------------------------------------------------------------

    async def drain(self, timeout_s: float) -> int:
        """Wait up to ``timeout_s`` for in-flight attempts, then cancel and release the rest.

        Returns the number of leases released back to ``pending``.
        """
        tasks = [f.task for f in self._inflight.values()]
        if tasks:
            logger.info("worker.draining", in_flight=len(tasks), timeout_s=timeout_s)
            await asyncio.wait(tasks, timeout=timeout_s)
        unfinished = list(self._inflight.values())
        if not unfinished:
            return 0
        for flight in unfinished:
            flight.task.cancel()
        await asyncio.gather(*(f.task for f in unfinished), return_exceptions=True)
        async with self.database.session() as session:
            released = await delivery_queue.release_leases(
                session, [f.delivery.lease for f in unfinished]
            )
        self.stats.released += len(released)
        logger.warning("worker.leases_released", count=len(released))
        return len(released)

    # -- main loops -------------------------------------------------------------------------

    def touch_heartbeat(self) -> None:
        try:
            self._heartbeat.touch()
        except OSError as exc:
            if not self._heartbeat_warned:
                logger.warning(
                    "worker.heartbeat_unwritable", path=str(self._heartbeat), error=str(exc)
                )
                self._heartbeat_warned = True

    async def _wait(self, seconds: float) -> None:
        """Sleep until ``seconds`` pass, a slot frees up (when full) or stop is requested."""
        self._slot_freed.clear()
        waiters = [asyncio.ensure_future(self._stop.wait())]
        if self.free_slots <= 0:
            waiters.append(asyncio.ensure_future(self._slot_freed.wait()))
        try:
            await asyncio.wait(waiters, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()

    async def run_once(self) -> WorkerStats:
        """Reclaim expired leases, lease one batch, finish it, exit (``--once``, tests, CI)."""
        await self.reclaim_once()
        await self.poll_once()
        tasks = [f.task for f in self._inflight.values()]
        if tasks:
            await asyncio.wait(tasks)
        self.touch_heartbeat()
        return self.stats

    async def run(self) -> WorkerStats:
        """Lease and deliver until :meth:`stop` (or SIGTERM/SIGINT), then drain."""
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop)
                installed.append(sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - non-Unix / thread
                pass
        settings = self.settings
        min_s = settings.worker_poll_min_ms / 1000
        max_s = settings.worker_poll_max_ms / 1000
        delay = min_s
        background = [
            asyncio.create_task(
                self._every(settings.worker_lease_renew_interval_s, "renew", self.renew_once)
            ),
            asyncio.create_task(
                self._every(settings.worker_reclaim_interval_s, "reclaim", self.reclaim_once)
            ),
        ]
        logger.info(
            "worker.started",
            worker_id=self.worker_id,
            concurrency=settings.worker_concurrency,
            lease_s=settings.worker_lease_s,
            max_attempts=settings.delivery_max_attempts,
            allow_private_networks=settings.delivery_allow_private_networks,
        )
        try:
            with contextlib.suppress(Exception):
                await self.reclaim_once()
            while not self.stopping:
                leased = 0
                try:
                    leased = await self.poll_once()
                except Exception as exc:
                    self.stats.loop_errors += 1
                    logger.exception("worker.poll_failed", error=type(exc).__name__)
                self.touch_heartbeat()
                if self._stop.is_set():
                    break
                if leased and self.free_slots > 0:
                    delay = min_s
                    continue  # there was work and room for more: look again right away
                delay = min_s if leased else min(delay * 2, max_s)
                await self._wait(max_s if self.free_slots <= 0 else delay)
            await self.drain(settings.worker_drain_timeout_s)
        finally:
            for task in background:
                task.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            for sig in installed:
                loop.remove_signal_handler(sig)
            logger.info("worker.stopped", worker_id=self.worker_id, **asdict(self.stats))
        return self.stats


async def serve(settings: Settings, *, once: bool = False) -> WorkerStats:
    database = Database.from_settings(settings)
    worker = Worker(database, settings)
    try:
        if once:
            return await worker.run_once()
        return await worker.run()
    finally:
        await worker.aclose()
        await database.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="hookrelay-worker", description="hookrelay webhook delivery worker"
    )
    parser.add_argument(
        "--once", action="store_true", help="deliver one batch of due deliveries and exit"
    )
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="exit 0 if the heartbeat file is fresh (container health probe)",
    )
    args = parser.parse_args(argv)
    settings = get_settings()
    if args.healthcheck:
        fresh = heartbeat_is_fresh(
            settings.worker_heartbeat_file, settings.worker_heartbeat_max_age_s
        )
        return 0 if fresh else 1
    configure_logging(settings)
    asyncio.run(serve(settings, once=args.once))
    return 0


if __name__ == "__main__":
    sys.exit(main())
