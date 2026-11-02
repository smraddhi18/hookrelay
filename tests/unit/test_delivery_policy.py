"""Retry policy: classification, full-jitter bounds, Retry-After parsing, give-up rules."""

from __future__ import annotations

import itertools
import random
from datetime import UTC, datetime, timedelta

import pytest

from hookrelay.services.delivery_policy import (
    ErrorClass,
    RetryPolicy,
    backoff_ceiling,
    decide_retry,
    error_class_for_status,
    full_jitter_delay,
    is_retryable_error,
    is_retryable_status,
    is_success,
    parse_retry_after,
    truncate_body,
    truncate_message,
)

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _policy(**overrides: object) -> RetryPolicy:
    base: dict[str, object] = {
        "max_attempts": 5,
        "max_age": timedelta(days=3),
        "backoff_base_s": 5.0,
        "backoff_max_s": 3600.0,
        "retry_after_max_s": 600.0,
    }
    base.update(overrides)
    return RetryPolicy(**base)  # type: ignore[arg-type]


# -- classification --------------------------------------------------------------------------


@pytest.mark.parametrize("code", [200, 201, 202, 204, 299])
def test_2xx_is_success(code: int) -> None:
    assert is_success(code)
    assert error_class_for_status(code) is None


@pytest.mark.parametrize("code", [301, 302, 307, 308])
def test_3xx_is_a_terminal_redirect(code: int) -> None:
    assert error_class_for_status(code) is ErrorClass.REDIRECT
    assert not is_retryable_error(ErrorClass.REDIRECT, code)


@pytest.mark.parametrize(
    ("code", "retryable"),
    [
        (400, False),
        (401, False),
        (403, False),
        (404, False),
        (410, False),
        (413, False),
        (422, False),
        (408, True),
        (409, True),
        (425, True),
        (429, True),
        (500, True),
        (502, True),
        (503, True),
        (504, True),
        (599, True),
        (100, False),
    ],
)
def test_status_classification(code: int, retryable: bool) -> None:
    assert is_retryable_status(code) is retryable
    assert not is_success(code)
    if code >= 400:
        assert error_class_for_status(code) is ErrorClass.HTTP_STATUS
        assert is_retryable_error(ErrorClass.HTTP_STATUS, code) is retryable


@pytest.mark.parametrize(
    ("error_class", "retryable"),
    [
        (ErrorClass.TIMEOUT, True),
        (ErrorClass.CONNECT_ERROR, True),
        (ErrorClass.DNS_ERROR, True),
        (ErrorClass.TLS_ERROR, True),
        (ErrorClass.PROTOCOL_ERROR, True),
        (ErrorClass.DESTINATION_BLOCKED, False),
        (ErrorClass.SECRET_UNAVAILABLE, True),
        (ErrorClass.LEASE_EXPIRED, True),
        (ErrorClass.REDIRECT, False),
    ],
)
def test_error_classification(error_class: ErrorClass, retryable: bool) -> None:
    assert is_retryable_error(error_class) is retryable


def test_http_status_without_code_is_not_retryable() -> None:
    assert not is_retryable_error(ErrorClass.HTTP_STATUS, None)


# -- backoff ---------------------------------------------------------------------------------


def test_backoff_ceiling_doubles_then_caps() -> None:
    ceilings = [backoff_ceiling(n, 5.0, 3600.0) for n in range(1, 12)]
    assert ceilings[:4] == [5.0, 10.0, 20.0, 40.0]
    assert ceilings[-1] == 3600.0
    assert all(a <= b for a, b in itertools.pairwise(ceilings))


def test_backoff_ceiling_never_overflows() -> None:
    assert backoff_ceiling(10_000, 5.0, 3600.0) == 3600.0
    assert backoff_ceiling(1, 3600.0, 3600.0) == 3600.0


def test_backoff_rejects_attempt_zero() -> None:
    with pytest.raises(ValueError, match="start at 1"):
        backoff_ceiling(0, 5.0, 60.0)


@pytest.mark.parametrize("attempt", [1, 2, 3, 6, 20])
def test_full_jitter_stays_within_bounds(attempt: int) -> None:
    rng = random.Random(attempt)
    ceiling = backoff_ceiling(attempt, 5.0, 3600.0)
    delays = [full_jitter_delay(attempt, 5.0, 3600.0, rng) for _ in range(500)]
    assert all(0.0 <= d <= ceiling for d in delays)
    # Full jitter actually spreads: both halves of the window get used.
    assert min(delays) < ceiling / 2 < max(delays)


def test_full_jitter_uses_global_rng_by_default() -> None:
    assert 0.0 <= full_jitter_delay(3, 1.0, 10.0) <= 4.0


@pytest.mark.parametrize(
    "overrides",
    [
        {"max_attempts": 0},
        {"backoff_base_s": 0.0},
        {"backoff_base_s": 10.0, "backoff_max_s": 5.0},
        {"retry_after_max_s": -1.0},
    ],
)
def test_retry_policy_validates(overrides: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="must"):
        _policy(**overrides)


# -- Retry-After -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("120", 120.0),
        (" 7 ", 7.0),
        ("0", 0.0),
        ("999999999999999999", 600.0),  # capped
        ("Mon, 02 Nov 2026 12:01:30 GMT", 90.0),
        ("Mon, 02 Nov 2026 11:00:00 GMT", 0.0),  # past -> now
        ("Mon, 02 Nov 2026 13:00:00 GMT", 600.0),  # capped
        ("Mon, 02 Nov 2026 12:00:10 +0000", 10.0),
        ("Mon, 02 Nov 2026 12:00:10 -0000", 10.0),  # "-0000" parses as naive -> treated as UTC
        (None, None),
        ("", None),
        ("soon", None),
        ("-5", None),
        ("1.5", None),
        ("٣", None),  # non-ASCII digit
    ],
)
def test_parse_retry_after(value: str | None, expected: float | None) -> None:
    assert parse_retry_after(value, NOW, 600.0) == expected


# -- decide_retry ----------------------------------------------------------------------------


def test_retry_is_scheduled_within_the_jitter_window() -> None:
    decision = decide_retry(
        policy=_policy(),
        attempt_number=3,
        created_at=NOW,
        now=NOW,
        retry_after_s=None,
        rng=random.Random(1),
    )
    assert decision.retry
    assert decision.error_class is None
    assert decision.next_attempt_at is not None
    assert NOW <= decision.next_attempt_at <= NOW + timedelta(seconds=20)


def test_retry_after_is_a_floor() -> None:
    decision = decide_retry(
        policy=_policy(),
        attempt_number=1,
        created_at=NOW,
        now=NOW,
        retry_after_s=300.0,
        rng=random.Random(1),
    )
    assert decision.next_attempt_at == NOW + timedelta(seconds=300)


def test_retry_after_is_capped_by_policy() -> None:
    decision = decide_retry(
        policy=_policy(retry_after_max_s=60.0),
        attempt_number=1,
        created_at=NOW,
        now=NOW,
        retry_after_s=10_000.0,
        rng=random.Random(1),
    )
    assert decision.next_attempt_at == NOW + timedelta(seconds=60)


def test_gives_up_after_max_attempts() -> None:
    decision = decide_retry(
        policy=_policy(max_attempts=3),
        attempt_number=3,
        created_at=NOW,
        now=NOW,
        retry_after_s=None,
    )
    assert decision == decision.__class__(retry=False, next_attempt_at=None)


def test_gives_up_past_max_age() -> None:
    created = NOW - timedelta(hours=1)
    decision = decide_retry(
        policy=_policy(max_age=timedelta(hours=1), backoff_base_s=60.0),
        attempt_number=1,
        created_at=created,
        now=NOW,
        retry_after_s=None,
        rng=random.Random(3),
    )
    assert not decision.retry
    assert decision.error_class is ErrorClass.MAX_AGE_EXCEEDED


# -- truncation ------------------------------------------------------------------------------


def test_truncate_body() -> None:
    assert truncate_body(b"", 10) == (None, False)
    assert truncate_body(b"abc", 0) == (None, True)
    assert truncate_body(b"hello", 10) == ("hello", False)
    assert truncate_body(b"hello world", 5) == ("hello", True)
    assert truncate_body(b"a\x00b", 10) == ("a\ufffdb", False)


def test_truncate_body_never_exceeds_limit_in_bytes() -> None:
    text, truncated = truncate_body("é".encode() * 10, 5)  # cuts a 2-byte char in half
    assert truncated
    assert text is not None
    assert len(text.encode()) <= 5
    text, _ = truncate_body(b"\xff\xfe\xfd", 3)  # each invalid byte -> 3-byte U+FFFD
    assert text is not None
    assert len(text.encode()) <= 3


def test_truncate_message() -> None:
    assert truncate_message("line1\r\nline2\x00") == "line1  line2"
    long = truncate_message("x" * 2000, limit=10)
    assert len(long) <= 10
