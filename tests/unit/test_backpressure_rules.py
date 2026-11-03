"""Milestone 4 pure logic: breaker transitions, lease allocation, limits and replay contracts."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from hookrelay.api.contracts.replays import BulkReplay, EventReplay
from hookrelay.core.errors import UnprocessableEntityError
from hookrelay.db.models import BreakerState, DeliveryStatus
from hookrelay.services import notifications
from hookrelay.services.circuit_breaker import (
    CLOSED,
    BreakerPolicy,
    BreakerSnapshot,
    Signal,
    classify,
    on_failure,
    on_success,
    should_auto_disable,
)
from hookrelay.services.delivery_policy import ErrorClass
from hookrelay.services.delivery_queue import EndpointSlot, LeasedDelivery, allocate
from hookrelay.services.endpoints import EndpointPolicy
from hookrelay.services.replays import ReplayFilter
from tests.conftest import make_settings

NOW = datetime(2026, 11, 3, 12, 0, tzinfo=UTC)
POLICY = BreakerPolicy(failure_threshold=3, cooldown_base_s=10, cooldown_max_s=60)


# -- classify ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("error_class", "status_code", "signal"),
    [
        (None, 200, Signal.SUCCESS),
        (ErrorClass.HTTP_STATUS, 500, Signal.FAILURE),
        (ErrorClass.HTTP_STATUS, 503, Signal.FAILURE),
        (ErrorClass.HTTP_STATUS, 429, Signal.FAILURE),
        (ErrorClass.HTTP_STATUS, 408, Signal.FAILURE),
        (ErrorClass.HTTP_STATUS, 404, Signal.SUCCESS),
        (ErrorClass.HTTP_STATUS, 410, Signal.SUCCESS),
        (ErrorClass.HTTP_STATUS, None, Signal.SUCCESS),
        (ErrorClass.REDIRECT, 302, Signal.SUCCESS),
        (ErrorClass.TIMEOUT, None, Signal.FAILURE),
        (ErrorClass.CONNECT_ERROR, None, Signal.FAILURE),
        (ErrorClass.DNS_ERROR, None, Signal.FAILURE),
        (ErrorClass.TLS_ERROR, None, Signal.FAILURE),
        (ErrorClass.SECRET_UNAVAILABLE, None, Signal.NEUTRAL),
        (ErrorClass.LEASE_EXPIRED, None, Signal.NEUTRAL),
    ],
)
def test_classify(error_class: ErrorClass | None, status_code: int | None, signal: Signal) -> None:
    assert classify(error_class, status_code) is signal


# -- transitions -------------------------------------------------------------------------------


def test_failures_below_the_threshold_keep_the_breaker_closed() -> None:
    snap = on_failure(CLOSED, now=NOW, policy=POLICY)
    snap = on_failure(snap, now=NOW + timedelta(seconds=5), policy=POLICY)
    assert snap.state is BreakerState.CLOSED
    assert snap.consecutive_failures == 2
    assert snap.failing_since == NOW  # the streak started at the first failure
    assert snap.last_failure_at == NOW + timedelta(seconds=5)


def test_threshold_trips_and_cooldown_grows_per_trip() -> None:
    snap = CLOSED
    for _ in range(3):
        snap = on_failure(snap, now=NOW, policy=POLICY)
    assert snap.state is BreakerState.OPEN
    assert (snap.opened_at, snap.open_until, snap.trips) == (NOW, NOW + timedelta(seconds=10), 1)

    # A straggler finishing while open is counted but does not extend the cooldown.
    straggler = on_failure(snap, now=NOW + timedelta(seconds=1), policy=POLICY)
    assert straggler.state is BreakerState.OPEN
    assert straggler.open_until == snap.open_until
    assert straggler.consecutive_failures == 4

    probe = replace(straggler, state=BreakerState.HALF_OPEN)
    reopened = on_failure(probe, now=NOW + timedelta(seconds=20), policy=POLICY)
    assert reopened.state is BreakerState.OPEN
    assert reopened.trips == 2
    assert reopened.open_until == NOW + timedelta(seconds=40)


def test_success_closes_and_resets_the_streak() -> None:
    snap = BreakerSnapshot(
        BreakerState.HALF_OPEN,
        consecutive_failures=7,
        failing_since=NOW,
        trips=3,
        last_failure_at=NOW,
    )
    closed = on_success(snap)
    assert closed.state is BreakerState.CLOSED
    assert (closed.consecutive_failures, closed.trips, closed.failing_since) == (0, 0, None)
    assert closed.last_failure_at == NOW  # kept for operators


@pytest.mark.parametrize(("trips", "seconds"), [(0, 10), (1, 20), (2, 40), (3, 60), (99, 60)])
def test_cooldown_is_exponential_and_capped(trips: int, seconds: float) -> None:
    assert POLICY.cooldown(trips) == timedelta(seconds=seconds)
    assert POLICY.cooldown(-1) == timedelta(seconds=10)


def test_should_auto_disable_needs_a_long_failing_streak() -> None:
    policy = BreakerPolicy(failure_threshold=3, auto_disable_after_s=3600)
    failing = BreakerSnapshot(
        BreakerState.OPEN, consecutive_failures=3, failing_since=NOW - timedelta(hours=2)
    )
    assert should_auto_disable(failing, now=NOW, policy=policy)
    young = BreakerSnapshot(
        BreakerState.OPEN, consecutive_failures=3, failing_since=NOW - timedelta(minutes=5)
    )
    assert not should_auto_disable(young, now=NOW, policy=policy)
    few = BreakerSnapshot(
        BreakerState.CLOSED, consecutive_failures=2, failing_since=NOW - timedelta(hours=2)
    )
    assert not should_auto_disable(few, now=NOW, policy=policy)
    assert not should_auto_disable(CLOSED, now=NOW, policy=policy)
    off = BreakerPolicy(failure_threshold=3, auto_disable_after_s=0)
    assert not should_auto_disable(failing, now=NOW, policy=off)


def test_policy_from_settings() -> None:
    settings = make_settings(
        breaker_failure_threshold=7,
        breaker_cooldown_base_s=5,
        breaker_cooldown_max_s=50,
        endpoint_auto_disable_after_s=0,
    )
    assert BreakerPolicy.from_settings(settings) == BreakerPolicy(7, 5, 50, 0)


# -- lease allocation --------------------------------------------------------------------------


def _slot(
    *,
    capacity: int = 10,
    due: int = 10,
    tokens: float | None = None,
    state: BreakerState = BreakerState.CLOSED,
) -> EndpointSlot:
    return EndpointSlot(uuid.uuid4(), state, capacity, tokens, due)


@pytest.mark.parametrize(
    ("slot", "limit"),
    [
        (_slot(), 10),
        (_slot(capacity=2), 2),
        (_slot(due=3), 3),
        (_slot(tokens=4.9), 4),
        (_slot(tokens=0.5), 0),
        (_slot(capacity=0), 0),
        (_slot(state=BreakerState.HALF_OPEN), 1),
        (_slot(state=BreakerState.OPEN, tokens=0.2), 0),
        (_slot(capacity=-1), 0),
    ],
)
def test_slot_limit(slot: EndpointSlot, limit: int) -> None:
    assert slot.limit == limit


def test_allocate_round_robins_in_fairness_order() -> None:
    a, b, c = _slot(due=5), _slot(due=1), _slot(due=5)
    ids = a.endpoint_id, b.endpoint_id, c.endpoint_id
    assert allocate([a, b, c], 6) == dict(zip(ids, [3, 1, 2], strict=True))
    # The first in fairness order wins ties; endpoints that get nothing are left out.
    assert allocate([a, b, c], 2) == {a.endpoint_id: 1, b.endpoint_id: 1}
    assert allocate([a, b, c], 100) == dict(zip(ids, [5, 1, 5], strict=True))  # never above
    assert allocate([a], 0) == {}
    assert allocate([a], -3) == {}
    assert allocate([], 5) == {}
    assert allocate([_slot(capacity=0)], 5) == {}


def test_run_attempt_counts_from_the_last_replay() -> None:
    delivery = LeasedDelivery(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        event_id=uuid.uuid4(),
        endpoint_id=uuid.uuid4(),
        lease_token=uuid.uuid4(),
        attempt_number=5,
        created_at=NOW,
        event_type="invoice.paid",
        payload={},
        occurred_at=NOW,
        url="https://example.com",
        headers={},
        timeout_ms=1000,
        secret_ciphertext="x",
        attempt_offset=3,
    )
    assert delivery.run_attempt == 2
    assert delivery.lease.delivery_id == delivery.id


# -- limits and settings -----------------------------------------------------------------------


def test_endpoint_policy_limits() -> None:
    policy = EndpointPolicy.from_settings(
        make_settings(endpoint_max_in_flight=20, endpoint_max_rate_limit_per_s=50)
    )
    assert policy.check_max_in_flight(None) is None
    assert policy.check_max_in_flight(20) == 20
    assert policy.check_rate_limit(None) is None
    assert policy.check_rate_limit(50) == 50
    with pytest.raises(UnprocessableEntityError) as too_many:
        policy.check_max_in_flight(21)
    assert too_many.value.code == "max_in_flight_too_high"
    with pytest.raises(UnprocessableEntityError) as too_fast:
        policy.check_rate_limit(51)
    assert too_fast.value.code == "rate_limit_too_high"


def test_settings_reject_inconsistent_backpressure() -> None:
    with pytest.raises(ValidationError, match="ENDPOINT_DEFAULT_MAX_IN_FLIGHT"):
        make_settings(endpoint_default_max_in_flight=50, endpoint_max_in_flight=10)
    with pytest.raises(ValidationError, match="BREAKER_COOLDOWN_BASE_S"):
        make_settings(breaker_cooldown_base_s=100, breaker_cooldown_max_s=10)


# -- notifications -----------------------------------------------------------------------------


@pytest.mark.parametrize(("attempts", "seconds"), [(0, 2), (1, 2), (2, 4), (5, 32), (40, 300)])
def test_notification_retry_delay(attempts: int, seconds: int) -> None:
    assert notifications.retry_delay(attempts) == timedelta(seconds=seconds)


async def test_logging_notifier_logs_a_warning() -> None:
    message = notifications.NotificationMessage(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        endpoint_id=None,
        kind="endpoint.disabled",
        payload={"reason": "sustained_failure"},
        attempt=1,
    )
    with capture_logs() as logs:
        await notifications.LoggingNotifier().send(message)
    [entry] = logs
    assert entry["log_level"] == "warning"
    assert entry["kind"] == "endpoint.disabled"
    assert entry["endpoint_id"] is None


# -- replay contracts --------------------------------------------------------------------------


def test_replay_status_filters_are_terminal_deduped_and_sorted() -> None:
    assert set(EventReplay().statuses) == {DeliveryStatus.DEAD, DeliveryStatus.FAILED}
    replay = EventReplay(statuses=["succeeded", "dead", "dead"])  # type: ignore[list-item]
    assert replay.statuses == sorted({DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD})
    with pytest.raises(ValidationError, match="pending"):
        EventReplay(statuses=["pending"])  # type: ignore[list-item]
    with pytest.raises(ValidationError):
        EventReplay(statuses=[])
    with pytest.raises(ValidationError):
        EventReplay.model_validate({"unknown": 1})


def test_bulk_replay_requires_an_aware_window() -> None:
    ok = BulkReplay.model_validate(
        {"created_after": "2026-11-03T08:00:00Z", "created_before": "2026-11-03T09:00:00Z"}
    )
    assert ok.created_after.tzinfo is not None
    for bad in (
        {"created_after": "2026-11-03T08:00:00", "created_before": "2026-11-03T09:00:00"},
        {"created_after": "2026-11-03T08:00:00Z"},
        {
            "created_after": "2026-11-03T08:00:00Z",
            "created_before": "2026-11-03T09:00:00Z",
            "event_type": "Not A Type!",
        },
        {
            "created_after": "2026-11-03T08:00:00Z",
            "created_before": "2026-11-03T09:00:00Z",
            "statuses": ["delivering"],
        },
    ):
        with pytest.raises(ValidationError):
            BulkReplay.model_validate(bad)


def test_replay_filter_as_json_keeps_only_set_filters() -> None:
    endpoint_id = uuid.uuid4()
    flt = ReplayFilter(
        statuses=(DeliveryStatus.FAILED, DeliveryStatus.DEAD),
        created_after=NOW,
        endpoint_id=endpoint_id,
        event_type="invoice.paid",
    )
    assert flt.as_json() == {
        "statuses": ["dead", "failed"],
        "created_after": NOW.isoformat(),
        "endpoint_id": str(endpoint_id),
        "event_type": "invoice.paid",
    }
