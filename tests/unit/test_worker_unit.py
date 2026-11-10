"""Worker pieces that need no database: payload rendering, header hygiene, signing, the CLI."""

from __future__ import annotations

import contextlib
import json
import os
import time
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy.exc import (
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
    PendingRollbackError,
)
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from hookrelay import worker as worker_module
from hookrelay.core.config import get_settings
from hookrelay.core.security import SecretBox
from hookrelay.core.signing import verify
from hookrelay.db.engine import Database
from hookrelay.db.integrity import (
    is_connection_lost,
    is_disconnect,
    is_transient_db_error,
    is_transient_sqlstate,
)
from hookrelay.services import delivery_queue
from hookrelay.services.delivery_policy import ErrorClass
from hookrelay.services.delivery_queue import LeasedDelivery
from hookrelay.services.endpoints import secret_aad
from hookrelay.worker import (
    MASKED_HEADER_VALUE,
    REDACTED_SIGNATURE,
    Worker,
    default_worker_id,
    loggable_headers,
    render_body,
)
from tests.conftest import TEST_ENCRYPTION_KEY, make_settings
from tests.receiver import Reply, local_resolver, running_receiver

SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"


def _delivery(**overrides: Any) -> LeasedDelivery:
    tenant_id, endpoint_id = uuid.uuid4(), uuid.uuid4()
    box = SecretBox.from_settings(make_settings(secrets_encryption_key=TEST_ENCRYPTION_KEY))
    base: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": tenant_id,
        "event_id": uuid.uuid4(),
        "endpoint_id": endpoint_id,
        "lease_token": uuid.uuid4(),
        "attempt_number": 1,
        "created_at": datetime.now(UTC),
        "event_type": "invoice.paid",
        "payload": {"amount": 4200, "currency": "INR", "note": "ünïcode"},
        "occurred_at": datetime(
            2026, 11, 2, 15, 30, tzinfo=timezone(timedelta(hours=5, minutes=30))
        ),
        "url": "http://receiver.test/hook",
        "headers": {"X-Acme-Tenant": "acme-secret-value"},
        "timeout_ms": 2000,
        "secret_ciphertext": box.encrypt(SECRET, aad=secret_aad(tenant_id, endpoint_id)),
    }
    base.update(overrides)
    return LeasedDelivery(**base)


def _worker(tmp_path: Path, **overrides: Any) -> Worker:
    base: dict[str, Any] = {
        "secrets_encryption_key": TEST_ENCRYPTION_KEY,
        "endpoint_allow_http": True,
        "delivery_allow_private_networks": True,
        "worker_heartbeat_file": str(tmp_path / "hb"),
        "worker_id": "unit-worker",
    }
    base.update(overrides)
    settings = make_settings(**base)
    return Worker(cast("Database", object()), settings, resolver=local_resolver)


def test_render_body_is_a_compact_sorted_standard_webhooks_envelope() -> None:
    body = render_body(_delivery())
    assert (
        body
        == (
            '{"data":{"amount":4200,"currency":"INR","note":"ünïcode"},'
            '"timestamp":"2026-11-02T10:00:00Z","type":"invoice.paid"}'
        ).encode()
    )
    assert json.loads(body)["timestamp"].endswith("Z")


def test_loggable_headers_mask_custom_values_and_redact_the_signature() -> None:
    stored = loggable_headers(
        {
            "X-Acme-Tenant": "secret",
            "content-type": "application/json",
            "webhook-signature": "v1,abc",
            "webhook-id": "id-1",
        },
        {"x-acme-tenant": "secret"},
    )
    assert stored == {
        "x-acme-tenant": MASKED_HEADER_VALUE,
        "content-type": "application/json",
        "webhook-signature": REDACTED_SIGNATURE,
        "webhook-id": "id-1",
    }


def test_default_worker_id_is_unique_and_bounded() -> None:
    first, second = default_worker_id(), default_worker_id()
    assert first != second
    assert f":{os.getpid()}:" in first
    assert len(first) <= 128


async def test_attempt_signs_with_the_event_id_and_stores_safe_headers(tmp_path: Path) -> None:
    async with running_receiver(lambda _r: Reply(204, b"")) as receiver:
        delivery = _delivery(url=receiver.url)
        worker = _worker(tmp_path, delivery_allowed_ports=[receiver.port])
        try:
            record = await worker.attempt(delivery)
        finally:
            await worker.aclose()
    assert record.status_code == 204
    assert record.error_class is None
    assert not record.retryable
    [received] = receiver.requests
    assert verify(SECRET, received.headers, received.body) == str(delivery.event_id)
    assert received.headers["x-acme-tenant"] == "acme-secret-value"
    assert received.headers["content-type"] == "application/json"
    assert abs(int(received.headers["webhook-timestamp"]) - time.time()) < 5
    assert record.request_headers["x-acme-tenant"] == MASKED_HEADER_VALUE
    assert record.request_headers["webhook-signature"] == REDACTED_SIGNATURE
    assert "acme-secret-value" not in json.dumps(record.request_headers)
    assert record.finished_at >= record.started_at


async def test_attempt_with_an_undecryptable_secret_sends_nothing(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    try:
        record = await worker.attempt(_delivery(secret_ciphertext="v1.k1.AAAA"))
    finally:
        await worker.aclose()
    assert record.error_class is ErrorClass.SECRET_UNAVAILABLE
    assert record.retryable
    assert record.status_code is None
    assert record.latency_ms is None
    assert record.request_headers == {}


async def test_attempt_classifies_a_retryable_failure(tmp_path: Path) -> None:
    async with running_receiver(lambda _r: Reply(429, b"slow down", {"retry-after": "30"})) as r:
        worker = _worker(tmp_path, delivery_allowed_ports=[r.port])
        try:
            record = await worker.attempt(_delivery(url=r.url))
        finally:
            await worker.aclose()
    assert record.error_class is ErrorClass.HTTP_STATUS
    assert record.retryable
    assert record.retry_after_s == 30.0
    assert record.response_body == "slow down"


async def test_heartbeat_unwritable_warns_once(tmp_path: Path) -> None:
    worker = _worker(tmp_path, worker_heartbeat_file=str(tmp_path / "missing" / "hb"))
    worker.touch_heartbeat()
    worker.touch_heartbeat()
    assert worker._heartbeat_warned
    await worker.aclose()


async def test_stop_is_idempotent(tmp_path: Path) -> None:
    worker = _worker(tmp_path)
    assert not worker.stopping
    worker.stop()
    worker.stop()
    assert worker._stop.is_set()
    assert await worker.poll_once() == 0  # never leases once stopping
    assert await worker.drain(0.1) == 0
    assert await worker.renew_once() == 0
    await worker.aclose()


def test_healthcheck_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    heartbeat = tmp_path / "worker.hb"
    monkeypatch.setenv("WORKER_HEARTBEAT_FILE", str(heartbeat))
    monkeypatch.setenv("WORKER_HEARTBEAT_MAX_AGE_S", "30")
    get_settings.cache_clear()
    try:
        assert worker_module.main(["--healthcheck"]) == 1  # no heartbeat yet
        heartbeat.touch()
        assert worker_module.main(["--healthcheck"]) == 0
        old = time.time() - 120
        os.utime(heartbeat, (old, old))
        assert worker_module.main(["--healthcheck"]) == 1  # stale
    finally:
        get_settings.cache_clear()


# -- Milestone 8: completion retries on transient database errors -----------------------------


class _FlakyDatabase:
    """``session()`` fails with the queued errors, then yields a dummy session."""

    def __init__(self, *errors: BaseException) -> None:
        self.errors = list(errors)
        self.sessions = 0

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[Any]:
        self.sessions += 1
        if self.errors:
            raise self.errors.pop(0)
        yield object()


def _disconnect() -> DBAPIError:
    return DBAPIError("UPDATE deliveries", {}, ConnectionError("gone"), connection_invalidated=True)


def test_transient_db_error_classification() -> None:
    assert is_disconnect(_disconnect())
    assert is_transient_db_error(_disconnect())
    assert is_transient_db_error(OperationalError("SELECT 1", {}, OSError("reset")))
    assert is_transient_db_error(InterfaceError("SELECT 1", {}, Exception("closed")))
    assert is_transient_db_error(PoolTimeoutError("QueuePool limit reached"))
    assert is_transient_db_error(ConnectionResetError())
    assert is_transient_db_error(TimeoutError())
    assert not is_disconnect(OperationalError("SELECT 1", {}, OSError("reset")))
    assert not is_transient_db_error(IntegrityError("INSERT", {}, Exception("duplicate key")))
    assert not is_transient_db_error(DBAPIError("SELECT", {}, Exception("syntax")))
    assert not is_transient_db_error(ValueError("bug"))


class _PgError(Exception):
    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    ("code", "transient"),
    [
        ("57P03", True),  # cannot_connect_now: "the database system is shutting down"
        ("57P01", True),  # admin_shutdown
        ("57P02", True),  # crash_shutdown
        ("53300", True),  # too_many_connections
        ("08006", True),  # connection_failure
        ("08003", True),  # connection_does_not_exist
        ("23505", False),  # unique_violation
        ("42P01", False),  # undefined_table
        ("57014", False),  # query_canceled (statement_timeout): the statement itself
        ("XX000", False),  # internal_error
    ],
)
def test_connection_level_sqlstates_are_transient(code: str, transient: bool) -> None:
    """A restart refuses *new* connections with 57P03 while it shuts down / starts up. That
    arrives as a plain DBAPIError (not ``connection_invalidated``) when the pool opens a
    connection, and must still count as transient (measured: a real ``pg_ctl restart``)."""
    cause = _PgError("the database system is shutting down", code)
    orig = Exception("wrapped")
    orig.__cause__ = cause
    exc = DBAPIError("SELECT now()", {}, orig)
    assert is_transient_sqlstate(code) is transient
    assert is_transient_db_error(exc) is transient
    assert is_connection_lost(exc) is transient
    assert not is_transient_sqlstate(None)


def test_connection_lost_excludes_statement_level_failures() -> None:
    assert is_connection_lost(_disconnect())
    assert is_connection_lost(PendingRollbackError("invalid transaction"))
    # Transient for a whole-unit retry, but not proof that the connection is gone: a poison
    # statement that times out must still be isolated by the relay.
    assert not is_connection_lost(OperationalError("SELECT", {}, Exception("canceling statement")))
    assert not is_connection_lost(PoolTimeoutError("QueuePool limit reached"))
    assert not is_connection_lost(ValueError("bug"))


def _completing_worker(tmp_path: Path, db: _FlakyDatabase, **overrides: Any) -> Worker:
    worker = _worker(
        tmp_path, worker_complete_retry_base_ms=1, worker_complete_retry_max_ms=2, **overrides
    )
    worker.database = cast("Database", db)
    return worker


async def test_completion_is_retried_on_transient_errors_then_applied(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _FlakyDatabase(_disconnect(), PoolTimeoutError("pool"))
    worker = _completing_worker(tmp_path, db)
    applied: list[Any] = []

    async def fake_complete(_session: Any, delivery: Any, *_a: Any, **_k: Any) -> str:
        applied.append(delivery)
        return "done"

    async def fake_now(_session: Any) -> datetime:
        return datetime.now(UTC)

    monkeypatch.setattr(delivery_queue, "complete", fake_complete)
    monkeypatch.setattr(worker_module, "db_now", fake_now)
    try:
        completion, retries = await worker._complete(_delivery(), cast("Any", None))
    finally:
        await worker.aclose()
    assert (cast("Any", completion), retries) == ("done", 2)
    assert (db.sessions, len(applied), worker.stats.completion_retries) == (3, 1, 2)


async def test_completion_gives_up_after_the_retry_budget(tmp_path: Path) -> None:
    db = _FlakyDatabase(*(_disconnect() for _ in range(5)))
    worker = _completing_worker(tmp_path, db, worker_complete_retries=2)
    try:
        with pytest.raises(DBAPIError):
            await worker._complete(_delivery(), cast("Any", None))
    finally:
        await worker.aclose()
    assert (db.sessions, worker.stats.completion_retries) == (3, 2)


async def test_completion_never_retries_a_non_transient_error(tmp_path: Path) -> None:
    db = _FlakyDatabase(IntegrityError("INSERT", {}, Exception("duplicate key")))
    worker = _completing_worker(tmp_path, db)
    try:
        with pytest.raises(IntegrityError):
            await worker._complete(_delivery(), cast("Any", None))
    finally:
        await worker.aclose()
    assert (db.sessions, worker.stats.completion_retries) == (1, 0)


def test_completion_retry_backoff_bounds_are_validated() -> None:
    with pytest.raises(ValueError, match="WORKER_COMPLETE_RETRY_BASE_MS"):
        make_settings(worker_complete_retry_base_ms=500, worker_complete_retry_max_ms=100)
    settings = make_settings()
    assert (settings.worker_complete_retries, settings.worker_complete_retry_max_ms) == (6, 2000)
