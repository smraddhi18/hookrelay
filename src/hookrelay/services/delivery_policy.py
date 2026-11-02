"""Pure delivery policy: error classes, retry classification, backoff, ``Retry-After``, truncation.

Nothing here touches the network or the database, so every rule is unit-tested exhaustively and
the worker just composes them.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Final

# 408 Request Timeout, 409 Conflict, 425 Too Early and 429 Too Many Requests describe a
# transient receiver state; every other 4xx means "this request will never be accepted".
RETRYABLE_4XX: Final = frozenset({408, 409, 425, 429})
HTTP_OK_MIN: Final = 200
HTTP_REDIRECT_MIN: Final = 300
HTTP_CLIENT_ERROR_MIN: Final = 400
HTTP_SERVER_ERROR_MIN: Final = 500
HTTP_STATUS_MAX: Final = 599
MAX_ERROR_MESSAGE_CHARS: Final = 1_024


class ErrorClass(StrEnum):
    """Why an attempt did not succeed (stored on attempts and on the delivery)."""

    HTTP_STATUS = "http_status"  # non-2xx response other than a redirect
    REDIRECT = "redirect"  # 3xx: redirects are never followed (SSRF), so this is terminal
    TIMEOUT = "timeout"  # connect, read, write or total deadline exceeded
    CONNECT_ERROR = "connect_error"  # refused, reset, unreachable
    DNS_ERROR = "dns_error"  # name did not resolve
    TLS_ERROR = "tls_error"  # handshake or certificate failure
    PROTOCOL_ERROR = "protocol_error"  # malformed HTTP from the receiver
    DESTINATION_BLOCKED = "destination_blocked"  # SSRF guard: scheme, port or address refused
    SECRET_UNAVAILABLE = "secret_unavailable"  # noqa: S105 - signing secret could not be decrypted
    LEASE_EXPIRED = "lease_expired"  # the worker died or stalled past its lease
    MAX_AGE_EXCEEDED = "max_age_exceeded"  # not delivered within DELIVERY_MAX_AGE_S


# Transport/infrastructure failures worth retrying; HTTP status codes are classified separately.
_RETRYABLE_ERRORS: Final = frozenset(
    {
        ErrorClass.TIMEOUT,
        ErrorClass.CONNECT_ERROR,
        ErrorClass.DNS_ERROR,
        ErrorClass.TLS_ERROR,
        ErrorClass.PROTOCOL_ERROR,
        # An operator fixing SECRETS_ENCRYPTION_KEY recovers these; nothing was sent yet.
        ErrorClass.SECRET_UNAVAILABLE,
        ErrorClass.LEASE_EXPIRED,
    }
)


def is_success(status_code: int) -> bool:
    return HTTP_OK_MIN <= status_code < HTTP_REDIRECT_MIN


def is_retryable_status(status_code: int) -> bool:
    """5xx and the transient 4xx codes are retryable; 3xx and other 4xx are terminal."""
    if status_code >= HTTP_SERVER_ERROR_MIN:
        return True
    return status_code in RETRYABLE_4XX


def is_retryable_error(error_class: ErrorClass, status_code: int | None = None) -> bool:
    if error_class is ErrorClass.HTTP_STATUS:
        return status_code is not None and is_retryable_status(status_code)
    return error_class in _RETRYABLE_ERRORS


def error_class_for_status(status_code: int) -> ErrorClass | None:
    """``None`` for success, else the error class of a received status code."""
    if is_success(status_code):
        return None
    if HTTP_REDIRECT_MIN <= status_code < HTTP_CLIENT_ERROR_MIN:
        return ErrorClass.REDIRECT
    return ErrorClass.HTTP_STATUS


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int
    max_age: timedelta
    backoff_base_s: float
    backoff_max_s: float
    retry_after_max_s: float

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if not 0 < self.backoff_base_s <= self.backoff_max_s:
            raise ValueError("backoff must satisfy 0 < base <= max")
        if self.retry_after_max_s < 0:
            raise ValueError("retry_after_max_s must not be negative")


def backoff_ceiling(attempt: int, base_s: float, max_s: float) -> float:
    """``min(max, base * 2^(attempt-1))`` without overflowing for large attempt numbers."""
    if attempt < 1:
        raise ValueError("attempt numbers start at 1")
    exponent = attempt - 1
    # 2**1100 overflows a float; anything past the cap is the cap anyway.
    if exponent >= math.log2(max_s / base_s) + 1:
        return max_s
    return min(max_s, base_s * (2.0**exponent))


def full_jitter_delay(
    attempt: int, base_s: float, max_s: float, rng: random.Random | None = None
) -> float:
    """AWS "full jitter": uniform in ``[0, ceiling]``, so retries of many deliveries that failed
    together (a receiver outage) spread out instead of arriving in synchronized waves."""
    ceiling = backoff_ceiling(attempt, base_s, max_s)
    # Not security-sensitive randomness: only spreads retries over time.
    return (rng or random).uniform(0.0, ceiling)


def parse_retry_after(value: str | None, now: datetime, max_s: float) -> float | None:
    """``Retry-After`` as seconds from ``now``: delta-seconds or an HTTP-date, capped at ``max_s``.

    Returns ``None`` for a missing or unparseable header (a receiver must not be able to break
    the worker with a bad header); past dates and negative values mean "now" (0).
    """
    if value is None:
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.isascii() and raw.isdigit():
        seconds = float(int(raw[:12]))  # 12 digits is ~31,000 years; enough before capping
    else:
        try:
            when = parsedate_to_datetime(raw)
        except (TypeError, ValueError, IndexError):
            return None
        if when.tzinfo is None:  # RFC 9110 dates are GMT; treat naive as UTC
            when = when.replace(tzinfo=UTC)
        seconds = (when - now).total_seconds()
    return max(0.0, min(seconds, max_s))


@dataclass(frozen=True, slots=True)
class RetryDecision:
    retry: bool
    next_attempt_at: datetime | None
    error_class: ErrorClass | None = None  # set when the decision itself is the failure reason


def decide_retry(
    *,
    policy: RetryPolicy,
    attempt_number: int,
    created_at: datetime,
    now: datetime,
    retry_after_s: float | None,
    rng: random.Random | None = None,
) -> RetryDecision:
    """Schedule the next attempt after a retryable failure, or give up.

    Gives up when ``attempt_number`` reached ``max_attempts`` or when the next attempt would be
    later than ``created_at + max_age`` (an endpoint down for days gets ``failed`` rather than
    a webhook about a week-old event).
    """
    if attempt_number >= policy.max_attempts:
        return RetryDecision(retry=False, next_attempt_at=None)
    delay = full_jitter_delay(attempt_number, policy.backoff_base_s, policy.backoff_max_s, rng)
    if retry_after_s is not None:
        # The receiver asked for at least this long; never retry sooner than it asked.
        delay = max(delay, min(retry_after_s, policy.retry_after_max_s))
    next_at = now + timedelta(seconds=delay)
    if next_at > created_at + policy.max_age:
        return RetryDecision(
            retry=False, next_attempt_at=None, error_class=ErrorClass.MAX_AGE_EXCEEDED
        )
    return RetryDecision(retry=True, next_attempt_at=next_at)


def truncate_body(data: bytes, limit: int) -> tuple[str | None, bool]:
    """Response body for the attempt log: at most ``limit`` bytes of valid UTF-8, no NULs.

    Returns ``(text, truncated)``; an empty body is ``None``. Invalid UTF-8 is replaced (it is
    a debugging aid, not data), then the text is re-trimmed so it never exceeds ``limit`` bytes.
    """
    if not data or limit <= 0:
        return None, len(data) > 0
    truncated = len(data) > limit
    text = data[:limit].decode("utf-8", errors="replace").replace("\x00", "\ufffd")
    encoded = text.encode()
    if len(encoded) > limit:
        text = encoded[:limit].decode("utf-8", errors="ignore")
    return text, truncated


def truncate_message(message: str, limit: int = MAX_ERROR_MESSAGE_CHARS) -> str:
    cleaned = message.replace("\x00", "").replace("\r", " ").replace("\n", " ")
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."
