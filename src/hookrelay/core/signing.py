"""Standard Webhooks signatures (https://www.standardwebhooks.com): sign and verify.

Every delivery carries three headers:

* ``webhook-id``: the message id, identical across retries of one event (receivers dedupe on
  it, since delivery is at-least-once);
* ``webhook-timestamp``: Unix seconds when this attempt was signed (receivers reject stale
  ones, which bounds replay of a captured request);
* ``webhook-signature``: ``v1,<base64(HMAC-SHA256(key, "{id}.{timestamp}.{body}"))>``, where
  ``key`` is the base64-decoded part of the ``whsec_...`` endpoint secret. Several
  space-separated signatures may be present: during a secret rotation's overlap window the
  sender signs with the new and the previous secret, and a receiver holding either verifies.

This module has no hookrelay dependencies beyond the standard library, so a receiver written in
Python can vendor it as-is; :func:`verify` is the receiver-side helper.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

SECRET_PREFIX: Final = "whsec_"  # noqa: S105 - format prefix, not a secret
SIGNATURE_VERSION: Final = "v1"
HEADER_ID: Final = "webhook-id"
HEADER_TIMESTAMP: Final = "webhook-timestamp"
HEADER_SIGNATURE: Final = "webhook-signature"
DEFAULT_TOLERANCE_S: Final = 300
MAX_SIGNATURE_HEADER_LENGTH: Final = 4_096


class WebhookVerificationError(Exception):
    """The request is not a valid, fresh, correctly signed webhook (do not process it)."""


@dataclass(frozen=True, slots=True)
class SignedHeaders:
    webhook_id: str
    timestamp: int
    signature: str

    def as_dict(self) -> dict[str, str]:
        return {
            HEADER_ID: self.webhook_id,
            HEADER_TIMESTAMP: str(self.timestamp),
            HEADER_SIGNATURE: self.signature,
        }


def decode_secret(secret: str) -> bytes:
    """``whsec_<base64>`` (prefix optional) -> raw HMAC key bytes."""
    raw = secret.removeprefix(SECRET_PREFIX)
    try:
        key = base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("signing secret is not valid base64") from exc
    if not key:
        raise ValueError("signing secret is empty")
    return key


def compute_signature(key: bytes, webhook_id: str, timestamp: int, body: bytes) -> str:
    """Bare base64 HMAC-SHA256 over ``{id}.{timestamp}.{body}`` (no version prefix)."""
    to_sign = f"{webhook_id}.{timestamp}.".encode() + body
    return base64.b64encode(hmac.new(key, to_sign, hashlib.sha256).digest()).decode()


def sign(
    secret: str, webhook_id: str, timestamp: int, body: bytes, *extra_secrets: str
) -> SignedHeaders:
    """Headers for one delivery attempt of ``body`` signed with ``secret`` (and, space-separated
    after it, with every secret in ``extra_secrets``, e.g. the previous one during a
    rotation's overlap window)."""
    if not webhook_id or any(c in webhook_id for c in " .,\r\n"):
        raise ValueError("webhook id must be non-empty and contain no spaces, dots or commas")
    signatures = [
        f"{SIGNATURE_VERSION},{compute_signature(decode_secret(s), webhook_id, timestamp, body)}"
        for s in (secret, *extra_secrets)
    ]
    return SignedHeaders(webhook_id=webhook_id, timestamp=timestamp, signature=" ".join(signatures))


def _header(headers: Mapping[str, str], name: str) -> str:
    # Header names are case-insensitive; plain dicts from frameworks may use any casing.
    for key, value in headers.items():
        if key.lower() == name:
            return value
    raise WebhookVerificationError(f"missing {name} header")


def verify(
    secret: str,
    headers: Mapping[str, str],
    body: bytes,
    *,
    tolerance_s: int = DEFAULT_TOLERANCE_S,
    now: float | None = None,
) -> str:
    """Verify a received webhook; return its ``webhook-id`` or raise ``WebhookVerificationError``.

    Pass the **raw** request body bytes (re-serialising parsed JSON changes the bytes and breaks
    the signature). The timestamp must be within ``tolerance_s`` of ``now`` in either direction.
    """
    webhook_id = _header(headers, HEADER_ID)
    raw_timestamp = _header(headers, HEADER_TIMESTAMP)
    raw_signatures = _header(headers, HEADER_SIGNATURE)
    if len(raw_signatures) > MAX_SIGNATURE_HEADER_LENGTH:
        raise WebhookVerificationError("webhook-signature header is too long")
    if not raw_timestamp.isascii() or not raw_timestamp.isdigit():
        raise WebhookVerificationError("invalid webhook-timestamp")
    timestamp = int(raw_timestamp)
    current = time.time() if now is None else now
    if timestamp < current - tolerance_s:
        raise WebhookVerificationError("webhook-timestamp is too old")
    if timestamp > current + tolerance_s:
        raise WebhookVerificationError("webhook-timestamp is too far in the future")
    try:
        key = decode_secret(secret)
    except ValueError as exc:
        raise WebhookVerificationError("invalid signing secret") from exc
    expected = compute_signature(key, webhook_id, timestamp, body).encode()
    for candidate in raw_signatures.split(" "):
        version, _, value = candidate.partition(",")
        # Compare every v1 candidate in constant time; unknown versions are skipped.
        if version == SIGNATURE_VERSION and hmac.compare_digest(value.encode(), expected):
            return webhook_id
    raise WebhookVerificationError("no matching signature")
