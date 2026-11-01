"""Pure rules behind ingestion and fan-out: idempotency keys, fingerprints, backoff, config."""

from __future__ import annotations

import hashlib
import os
import time
from pathlib import Path
from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError

from hookrelay.core.config import EVENT_ENVELOPE_HEADROOM_BYTES
from hookrelay.core.errors import BadRequestError
from hookrelay.db.integrity import LOCK_NOT_AVAILABLE, sqlstate
from hookrelay.relay import heartbeat_is_fresh
from hookrelay.services import idempotency
from hookrelay.services.fanout import retry_delay_s
from tests.conftest import make_settings


@pytest.mark.parametrize(
    "key", ["a", "x" * 255, "0190f2a4-7c1e-7d4b-9f1a-2c3d4e5f6a7b", "order-42:attempt/1", "~!@#$%"]
)
def test_valid_keys(key: str) -> None:
    assert idempotency.validate_key(key) == key


@pytest.mark.parametrize("key", ["", " ", "a b", "x" * 256, "tab\t", "é", "line\n", "\x7f"])
def test_invalid_keys(key: str) -> None:
    with pytest.raises(BadRequestError) as info:
        idempotency.validate_key(key)
    assert info.value.code == "invalid_idempotency_key"


def test_fingerprint_covers_method_path_and_body() -> None:
    base = idempotency.fingerprint("POST", "/v1/events", b"{}")
    assert len(base) == 32
    assert base == hashlib.sha256(b"POST\n/v1/events\n{}").digest()
    assert idempotency.fingerprint("post", "/v1/events", b"{}") == base
    assert idempotency.fingerprint("PUT", "/v1/events", b"{}") != base
    assert idempotency.fingerprint("POST", "/v1/other", b"{}") != base
    assert idempotency.fingerprint("POST", "/v1/events", b"[]") != base


def test_retry_delay_is_exponential_and_capped() -> None:
    assert [retry_delay_s(a, max_s=30) for a in range(0, 8)] == [1, 1, 2, 4, 8, 16, 30, 30]
    assert retry_delay_s(10_000, max_s=300) == 300


def test_heartbeat_freshness(tmp_path: Path) -> None:
    path = tmp_path / "hb"
    assert not heartbeat_is_fresh(str(path), 60)
    path.touch()
    assert heartbeat_is_fresh(str(path), 60)
    old = time.time() - 120
    os.utime(path, (old, old))
    assert not heartbeat_is_fresh(str(path), 60)
    assert heartbeat_is_fresh(str(path), 60, now=old + 30)


def test_relay_poll_bounds_are_validated() -> None:
    with pytest.raises(ValidationError, match="RELAY_POLL_MIN_MS"):
        make_settings(relay_poll_min_ms=500, relay_poll_max_ms=100)
    with pytest.raises(ValidationError):
        make_settings(event_max_payload_bytes=100)
    settings = make_settings(event_max_payload_bytes=2_048)
    assert settings.event_max_request_bytes == 2_048 + EVENT_ENVELOPE_HEADROOM_BYTES


def test_sqlstate_reads_driver_codes() -> None:
    direct = DBAPIError("SELECT", {}, Mock(sqlstate=LOCK_NOT_AVAILABLE))
    assert sqlstate(direct) == LOCK_NOT_AVAILABLE
    cause = Mock(spec=[])
    cause.__cause__ = Mock(spec=["pgcode"], pgcode="23505")
    assert sqlstate(DBAPIError("SELECT", {}, cause)) == "23505"
    assert sqlstate(DBAPIError("SELECT", {}, Exception("plain"))) is None
