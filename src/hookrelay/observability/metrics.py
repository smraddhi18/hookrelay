"""Prometheus metrics registry for the API, relay and worker processes.

Each process builds one :class:`Metrics` bound to a private ``CollectorRegistry`` (so tests
never share state with each other or with a previous run). Counters and histograms are updated
inline on the hot path; queue-depth / breaker / lag gauges are refreshed from Postgres on scrape
(API) or on a timer (relay / worker) so a Prometheus poll never walks tenant payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

# Buckets cover sub-ms local receivers through multi-second remote ones.
_LATENCY_BUCKETS_S: Final = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
)

# Completion group-commit sizes (WORKER_COMPLETE_BATCH_MAX is at most 500).
_BATCH_SIZE_BUCKETS: Final = (1, 2, 4, 8, 16, 32, 64, 128, 256, 500)
# Flush transactions: sub-millisecond local commits through retried ones during an outage.
_FLUSH_BUCKETS_S: Final = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0, 5.0)

SERVICE_API: Final = "api"
SERVICE_RELAY: Final = "relay"
SERVICE_WORKER: Final = "worker"


@dataclass(slots=True)
class Metrics:
    """Process-local Prometheus instruments. Construct once per process via :func:`get_metrics`."""

    registry: CollectorRegistry
    service: str
    events_ingested: Counter
    outbox_processed: Counter
    outbox_failed: Counter
    deliveries_created: Counter
    delivery_attempts: Counter
    delivery_latency_seconds: Histogram
    outbox_pending: Gauge
    outbox_lag_seconds: Gauge
    deliveries_pending: Gauge
    deliveries_due: Gauge
    deliveries_delivering: Gauge
    breaker_endpoints: Gauge
    relay_batches: Counter
    relay_loop_errors: Counter
    worker_leased: Counter
    worker_lease_lost: Counter
    worker_loop_errors: Counter
    worker_completion_batch_size: Histogram
    worker_completion_flush_seconds: Histogram
    worker_completion_retries: Counter

    def record_event_ingested(self) -> None:
        self.events_ingested.labels(service=self.service).inc()

    def record_outbox_processed(self, *, processed: int, deliveries: int, failed: int = 0) -> None:
        if processed:
            self.outbox_processed.labels(service=self.service).inc(processed)
        if failed:
            self.outbox_failed.labels(service=self.service).inc(failed)
        if deliveries:
            self.deliveries_created.labels(service=self.service).inc(deliveries)

    def record_delivery_attempt(self, *, outcome: str, latency_ms: int | None) -> None:
        self.delivery_attempts.labels(service=self.service, outcome=outcome).inc()
        if latency_ms is not None:
            self.delivery_latency_seconds.labels(service=self.service, outcome=outcome).observe(
                max(latency_ms, 0) / 1000.0
            )

    def record_relay_batch(self) -> None:
        self.relay_batches.labels(service=self.service).inc()

    def record_relay_loop_error(self) -> None:
        self.relay_loop_errors.labels(service=self.service).inc()

    def record_worker_leased(self, n: int) -> None:
        if n:
            self.worker_leased.labels(service=self.service).inc(n)

    def record_worker_lease_lost(self) -> None:
        self.worker_lease_lost.labels(service=self.service).inc()

    def record_worker_loop_error(self) -> None:
        self.worker_loop_errors.labels(service=self.service).inc()

    def record_worker_completion_flush(self, *, size: int, seconds: float) -> None:
        self.worker_completion_batch_size.labels(service=self.service).observe(size)
        self.worker_completion_flush_seconds.labels(service=self.service).observe(max(seconds, 0))

    def record_worker_completion_retry(self) -> None:
        self.worker_completion_retries.labels(service=self.service).inc()

    def set_queue_depth(
        self,
        *,
        outbox_pending: int,
        outbox_lag_seconds: float,
        deliveries_pending: int,
        deliveries_due: int,
        deliveries_delivering: int,
    ) -> None:
        self.outbox_pending.labels(service=self.service).set(outbox_pending)
        self.outbox_lag_seconds.labels(service=self.service).set(max(outbox_lag_seconds, 0.0))
        self.deliveries_pending.labels(service=self.service).set(deliveries_pending)
        # The worker autoscaling signal: ``pending`` also counts retries scheduled hours ahead
        # and backlogs behind an open circuit breaker, which no number of workers can speed up.
        self.deliveries_due.labels(service=self.service).set(deliveries_due)
        self.deliveries_delivering.labels(service=self.service).set(deliveries_delivering)

    def set_breaker_counts(self, *, closed: int, open_: int, half_open: int) -> None:
        self.breaker_endpoints.labels(service=self.service, state="closed").set(closed)
        self.breaker_endpoints.labels(service=self.service, state="open").set(open_)
        self.breaker_endpoints.labels(service=self.service, state="half_open").set(half_open)


def build_metrics(service: str) -> Metrics:
    """Create a fresh registry + instruments labelled with ``service`` (api|relay|worker)."""
    registry = CollectorRegistry()
    return Metrics(
        registry=registry,
        service=service,
        events_ingested=Counter(
            "hookrelay_events_ingested_total",
            "Events accepted by POST /v1/events (idempotent replays are not counted).",
            ["service"],
            registry=registry,
        ),
        outbox_processed=Counter(
            "hookrelay_outbox_rows_processed_total",
            "Outbox rows the relay marked processed.",
            ["service"],
            registry=registry,
        ),
        outbox_failed=Counter(
            "hookrelay_outbox_rows_failed_total",
            "Outbox rows parked after fan-out failure (poison / transient).",
            ["service"],
            registry=registry,
        ),
        deliveries_created=Counter(
            "hookrelay_deliveries_created_total",
            "Delivery rows inserted by the fan-out relay.",
            ["service"],
            registry=registry,
        ),
        delivery_attempts=Counter(
            "hookrelay_delivery_attempts_total",
            "Webhook delivery attempts by outcome (succeeded|retrying|failed|dead).",
            ["service", "outcome"],
            registry=registry,
        ),
        delivery_latency_seconds=Histogram(
            "hookrelay_delivery_latency_seconds",
            "Outbound webhook attempt latency in seconds.",
            ["service", "outcome"],
            buckets=_LATENCY_BUCKETS_S,
            registry=registry,
        ),
        outbox_pending=Gauge(
            "hookrelay_outbox_pending",
            "Unprocessed outbox rows (fan-out backlog).",
            ["service"],
            registry=registry,
        ),
        outbox_lag_seconds=Gauge(
            "hookrelay_outbox_lag_seconds",
            "Age in seconds of the oldest unprocessed outbox row (0 when empty).",
            ["service"],
            registry=registry,
        ),
        deliveries_pending=Gauge(
            "hookrelay_deliveries_pending",
            "Deliveries waiting in status=pending.",
            ["service"],
            registry=registry,
        ),
        deliveries_due=Gauge(
            "hookrelay_deliveries_due",
            "Pending deliveries a worker could lease now (due, endpoint breaker not cooling down).",
            ["service"],
            registry=registry,
        ),
        deliveries_delivering=Gauge(
            "hookrelay_deliveries_delivering",
            "Deliveries currently leased (status=delivering).",
            ["service"],
            registry=registry,
        ),
        breaker_endpoints=Gauge(
            "hookrelay_circuit_breaker_endpoints",
            "Endpoints in each circuit-breaker state.",
            ["service", "state"],
            registry=registry,
        ),
        relay_batches=Counter(
            "hookrelay_relay_batches_total",
            "Fan-out batches the relay completed.",
            ["service"],
            registry=registry,
        ),
        relay_loop_errors=Counter(
            "hookrelay_relay_loop_errors_total",
            "Unhandled errors in the relay main loop.",
            ["service"],
            registry=registry,
        ),
        worker_leased=Counter(
            "hookrelay_worker_leased_total",
            "Deliveries leased by this worker.",
            ["service"],
            registry=registry,
        ),
        worker_lease_lost=Counter(
            "hookrelay_worker_lease_lost_total",
            "Attempts whose fencing token was lost before completion.",
            ["service"],
            registry=registry,
        ),
        worker_loop_errors=Counter(
            "hookrelay_worker_loop_errors_total",
            "Unhandled errors in the worker main loop.",
            ["service"],
            registry=registry,
        ),
        worker_completion_batch_size=Histogram(
            "hookrelay_worker_completion_batch_size",
            "Attempts written per completion transaction (group commit).",
            ["service"],
            buckets=_BATCH_SIZE_BUCKETS,
            registry=registry,
        ),
        worker_completion_flush_seconds=Histogram(
            "hookrelay_worker_completion_flush_seconds",
            "Wall time to write one completion batch, including transient-error retries.",
            ["service"],
            buckets=_FLUSH_BUCKETS_S,
            registry=registry,
        ),
        worker_completion_retries=Counter(
            "hookrelay_worker_completion_retries_total",
            "Completion transactions retried after a transient database error.",
            ["service"],
            registry=registry,
        ),
    )


# One Metrics per process; tests build their own and pass it in.
_PROCESS_METRICS: Metrics | None = None


def get_metrics(service: str = SERVICE_API) -> Metrics:
    """Process-wide metrics singleton (tests call :func:`build_metrics` instead)."""
    global _PROCESS_METRICS  # noqa: PLW0603 - process singleton
    if _PROCESS_METRICS is None:
        _PROCESS_METRICS = build_metrics(service)
    return _PROCESS_METRICS


def reset_metrics_for_tests() -> None:
    """Drop the process singleton so the next :func:`get_metrics` builds a fresh registry."""
    global _PROCESS_METRICS  # noqa: PLW0603 - test reset
    _PROCESS_METRICS = None


def render_metrics(metrics: Metrics) -> tuple[bytes, str]:
    """Return ``(body, content_type)`` for a Prometheus scrape response."""
    return generate_latest(metrics.registry), CONTENT_TYPE_LATEST
