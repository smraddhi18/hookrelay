"""Milestone 5 rules without a database: config validation, multi-secret signing, the rotation
/ health / search policies, the retention runner's budget and stop handling, and the CLI."""

from __future__ import annotations

import base64
import contextlib
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from hookrelay import retention as retention_cli
from hookrelay.core.errors import UnprocessableEntityError
from hookrelay.core.security import generate_signing_secret
from hookrelay.core.signing import WebhookVerificationError, sign, verify
from hookrelay.services import retention as retention_service
from hookrelay.services.circuit_breaker import Signal, classify
from hookrelay.services.deliveries import DeliverySearch
from hookrelay.services.delivery_policy import ErrorClass, is_retryable_error
from hookrelay.services.endpoint_health import HealthPolicy
from hookrelay.services.endpoint_secrets import SecretRotationPolicy, check_custom_secret
from hookrelay.services.retention import (
    PayloadBatch,
    RetentionPolicy,
    RetentionReport,
    RetentionRunner,
)
from tests.conftest import make_settings

# --- config ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        (
            {"endpoint_secret_rotation_overlap_s": 7200, "endpoint_secret_max_overlap_s": 3600},
            "ENDPOINT_SECRET_ROTATION_OVERLAP_S",
        ),
        (
            {"endpoint_health_default_window_s": 7200, "endpoint_health_max_window_s": 3600},
            "ENDPOINT_HEALTH_DEFAULT_WINDOW_S",
        ),
        (
            {"event_payload_retention_days": 7, "event_retention_days": 7},
            "EVENT_PAYLOAD_RETENTION_DAYS",
        ),
    ],
)
def test_operability_settings_are_cross_validated(overrides: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        make_settings(**overrides)


def test_payload_retention_seconds() -> None:
    assert make_settings(event_payload_retention_days=0).event_payload_retention_s is None
    settings = make_settings(event_payload_retention_days=2, event_retention_days=7)
    assert settings.event_payload_retention_s == 2 * 86_400
    assert RetentionPolicy.from_settings(settings).payload_retention_s == 2 * 86_400


# --- signing --------------------------------------------------------------------------------


def test_sign_with_extra_secrets_verifies_with_each() -> None:
    current, previous, stranger = (generate_signing_secret() for _ in range(3))
    now = int(time.time())
    headers = sign(current, "msg_1", now, b"{}", previous).as_dict()
    assert headers["webhook-signature"].count("v1,") == 2
    assert verify(current, headers, b"{}", now=now) == "msg_1"
    assert verify(previous, headers, b"{}", now=now) == "msg_1"
    with pytest.raises(WebhookVerificationError, match="no matching signature"):
        verify(stranger, headers, b"{}", now=now)
    single = sign(current, "msg_1", now, b"{}").as_dict()
    assert " " not in single["webhook-signature"]


# --- policies -------------------------------------------------------------------------------


def test_rotation_overlap_policy() -> None:
    policy = SecretRotationPolicy.from_settings(
        make_settings(endpoint_secret_rotation_overlap_s=60, endpoint_secret_max_overlap_s=120)
    )
    assert policy.overlap(None) == 60
    assert policy.overlap(0) == 0
    assert policy.overlap(120) == 120
    with pytest.raises(UnprocessableEntityError) as exc:
        policy.overlap(121)
    assert exc.value.code == "overlap_too_long"


@pytest.mark.parametrize(
    "secret",
    [
        "abc" + generate_signing_secret(),
        "whsec_!!not-base64!!",
        "whsec_" + base64.b64encode(b"x" * 23).decode(),
        "whsec_" + base64.b64encode(b"x" * 65).decode(),
    ],
)
def test_custom_secret_is_rejected(secret: str) -> None:
    with pytest.raises(UnprocessableEntityError) as exc:
        check_custom_secret(secret)
    assert exc.value.code == "invalid_signing_secret"


def test_custom_secret_bounds_are_inclusive() -> None:
    for size in (24, 64):
        secret = "whsec_" + base64.b64encode(b"k" * size).decode()
        assert check_custom_secret(secret) == secret


def test_health_window_policy() -> None:
    policy = HealthPolicy(
        default_window_s=3600, max_window_s=7200, max_attempts=10, default_max_in_flight=4
    )
    assert policy.window(None) == 3600
    assert policy.window(7200) == 7200
    with pytest.raises(UnprocessableEntityError) as exc:
        policy.window(7201)
    assert exc.value.code == "window_too_large"


def test_delivery_search_time_range() -> None:
    t = datetime(2026, 11, 4, tzinfo=UTC)
    DeliverySearch(created_after=t, created_before=t + timedelta(microseconds=1))
    DeliverySearch(created_after=t)
    DeliverySearch(created_before=t)
    for before in (t, t - timedelta(seconds=1)):
        with pytest.raises(UnprocessableEntityError) as exc:
            DeliverySearch(created_after=t, created_before=before)
        assert exc.value.code == "invalid_time_range"


def test_payload_purged_is_terminal_and_breaker_neutral() -> None:
    assert not is_retryable_error(ErrorClass.PAYLOAD_PURGED)
    assert classify(ErrorClass.PAYLOAD_PURGED, None) is Signal.NEUTRAL


# --- retention runner -----------------------------------------------------------------------


class _FakeDatabase:
    def __init__(self) -> None:
        self.sessions = 0

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[object]:
        self.sessions += 1
        yield object()


def _runner(
    monkeypatch: pytest.MonkeyPatch,
    *,
    events: list[int],
    payloads: list[int] | None = None,
    secrets: list[int] | None = None,
    stop_after: int | None = None,
    **policy: Any,
) -> tuple[RetentionRunner, list[float]]:
    events_left, payloads_left, secrets_left = (
        list(events),
        list(payloads or []),
        list(secrets or []),
    )

    async def purge_events(_s: object, *, batch_size: int) -> int:
        return events_left.pop(0) if events_left else 0

    async def purge_payloads(
        _s: object, *, older_than_s: int, batch_size: int, after: object
    ) -> PayloadBatch:
        scanned = payloads_left.pop(0) if payloads_left else 0
        return PayloadBatch(
            scanned,
            scanned - 1 if scanned else 0,
            1,
            1 if scanned else 0,
            (datetime.now(UTC), uuid.uuid4()) if scanned else None,
        )

    async def expire_secrets(_s: object, *, batch_size: int) -> int:
        return secrets_left.pop(0) if secrets_left else 0

    monkeypatch.setattr(retention_service, "purge_expired_events", purge_events)
    monkeypatch.setattr(retention_service, "purge_payloads", purge_payloads)
    monkeypatch.setattr(retention_service, "expire_previous_secrets", expire_secrets)
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)

    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return stop_after is not None and calls["n"] > stop_after

    params: dict[str, Any] = {"batch_size": 10, "max_batches": 5, **policy}
    runner = RetentionRunner(
        _FakeDatabase(),  # type: ignore[arg-type]
        RetentionPolicy(**params),
        should_stop=should_stop,
        sleep=sleep,
    )
    return runner, sleeps


async def test_runner_stops_on_short_batch_and_pauses_between(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, sleeps = _runner(
        monkeypatch,
        events=[10, 10, 3],
        payloads=[10, 4],
        secrets=[2],
        pause_s=0.25,
        payload_retention_s=60,
    )
    report = await runner.run()
    assert (report.events, report.payloads, report.previous_secrets) == (23, 12, 2)
    assert report.responses_scrubbed == 2
    assert report.payloads_skipped_busy == 2
    assert report.batches == 3 + 2 + 1
    assert report.complete
    assert report.total == 23 + 12 + 2
    assert sleeps == [0.25, 0.25, 0.25]  # between batches of a task, never before the first


async def test_runner_budget_marks_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, sleeps = _runner(
        monkeypatch,
        events=[10, 10, 10],
        payloads=[10, 10, 10],
        max_batches=2,
        payload_retention_s=60,
    )
    report = await runner.run()
    assert report.events == 20
    assert report.batches == 2 + 2 + 1
    assert not report.complete
    assert sleeps == []  # pause_s = 0


async def test_runner_honours_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, _ = _runner(
        monkeypatch, events=[10, 10], payloads=[10], stop_after=1, payload_retention_s=60
    )
    report = await runner.run()
    assert report.events == 10
    assert report.batches == 1
    assert report.payloads == 0
    assert not report.complete


async def test_runner_skips_disabled_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    runner, _ = _runner(monkeypatch, events=[10], secrets=[1], expire_events=False)
    report = await runner.run()
    assert (report.events, report.payloads, report.previous_secrets, report.batches) == (0, 0, 1, 1)


# --- CLI ------------------------------------------------------------------------------------


def test_cli_rejects_non_positive_max_batches(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        retention_cli.main(["--max-batches", "0"])
    assert exc.value.code == 2
    assert "--max-batches must be >= 1" in capsys.readouterr().err


@pytest.mark.parametrize(("complete", "code"), [(True, 0), (False, 3)])
def test_cli_exit_code(monkeypatch: pytest.MonkeyPatch, complete: bool, code: int) -> None:
    seen: dict[str, Any] = {}

    async def fake_run(_settings: object, *, max_batches: int | None = None) -> RetentionReport:
        seen["max_batches"] = max_batches
        return RetentionReport(complete=complete)

    monkeypatch.setattr(retention_cli, "run", fake_run)
    assert retention_cli.main(["--max-batches", "7"]) == code
    assert seen["max_batches"] == 7
