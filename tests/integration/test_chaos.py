"""Milestone 8 chaos tests: delivery semantics under failure, on real Postgres.

Every scenario drives real traffic (API ingest → relay fan-out → worker → the mock receiver)
and then checks the same invariants with :func:`assert_at_least_once`:

* nothing is lost: every fanned-out delivery ends ``succeeded`` and its receiver saw at
  least one acknowledged (2xx, client still connected) request;
* duplicates only where at-least-once allows them: a message acknowledged ``n`` times must
  have ``n - 1`` attempts that the reaper recorded as ``lease_expired`` (the worker's result
  was lost after the receiver processed it); with no lost results there are no duplicates;
* no phantom requests: a receiver never sees more requests for a delivery than it has
  attempt rows.

Faults are injected deterministically: a worker subprocess is ``SIGKILL``-ed while its
requests are held by the receiver, every Postgres backend of the test database is terminated
in a loop (what clients see during a restart or failover), a backend is killed between a
receiver's 2xx and the completion write, receivers are slow, fail, rate-limit or hang.
"""

from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import asyncpg
import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, PendingRollbackError

from hookrelay.core.config import Settings
from hookrelay.db.engine import Database
from hookrelay.mock_receiver import MockReceiver, RunningReceiver, serve_in_background
from hookrelay.relay import Relay
from hookrelay.services import delivery_queue
from hookrelay.services import fanout as fanout_service
from hookrelay.worker import Worker
from tests.conftest import TEST_ENCRYPTION_KEY
from tests.integration.conftest import (
    TenantCtx,
    _asyncpg_dsn,
    api_client,
    api_settings,
    create_tenant,
    publish,
    register_event_type,
    subscribe,
)
from tests.integration.test_worker import db_url, worker_template  # noqa: F401

EVENT_TYPE = "chaos.tick"


# -- fixtures -------------------------------------------------------------------------------


@pytest.fixture
async def mock() -> AsyncIterator[RunningReceiver]:
    async with serve_in_background(MockReceiver(seed=8)) as running:
        yield running


@pytest.fixture
def chaos_settings(db_url: str, tmp_path: Path, mock: RunningReceiver) -> Callable[..., Settings]:
    def _make(**overrides: Any) -> Settings:
        base: dict[str, Any] = {
            "endpoint_allow_http": True,
            "endpoint_allow_private_networks": True,
            "delivery_allow_private_networks": True,
            "delivery_allowed_ports": [mock.port],
            "relay_heartbeat_file": str(tmp_path / "relay.hb"),
            "worker_heartbeat_file": str(tmp_path / "worker.hb"),
            "relay_poll_min_ms": 10,
            "relay_poll_max_ms": 50,
            "worker_poll_min_ms": 10,
            "worker_poll_max_ms": 50,
            "worker_concurrency": 16,
            "worker_lease_s": 5,
            "worker_lease_renew_interval_s": 1.0,
            "worker_reclaim_interval_s": 0.2,
            "worker_complete_retry_base_ms": 20,
            "worker_complete_retry_max_ms": 200,
            "delivery_backoff_base_s": 0.05,
            "delivery_backoff_max_s": 0.2,
            "delivery_retry_after_max_s": 1,
            "delivery_max_attempts": 60,
            "breaker_failure_threshold": 1_000,
            "endpoint_default_max_in_flight": 50,
        }
        base.update(overrides)
        return api_settings(db_url, **base)

    return _make


@pytest.fixture
async def api(db_url: str, chaos_settings: Callable[..., Settings]) -> AsyncIterator[Any]:
    async with api_client(
        db_url, endpoint_allow_http=True, endpoint_allow_private_networks=True
    ) as client:
        yield client


@dataclass
class Running:
    """Relay and worker loops running on this event loop until :meth:`stop`."""

    relay: Relay
    worker: Worker
    databases: list[Database]
    tasks: list[asyncio.Task[Any]]

    async def stop(self) -> None:
        self.relay.stop()
        self.worker.stop()
        try:
            await asyncio.wait_for(asyncio.gather(*self.tasks), 30)
        finally:
            await self.worker.aclose()
            for db in self.databases:
                await db.dispose()


async def start_pipeline(settings: Settings, *, relay: bool = True) -> Running:
    relay_db, worker_db = Database.from_settings(settings), Database.from_settings(settings)
    r, w = Relay(relay_db, settings), Worker(worker_db, settings)
    tasks: list[asyncio.Task[Any]] = [asyncio.create_task(w.run())]
    if relay:
        tasks.append(asyncio.create_task(r.run()))
    return Running(relay=r, worker=w, databases=[relay_db, worker_db], tasks=tasks)


# -- helpers --------------------------------------------------------------------------------


async def provision(api: Any, mock: RunningReceiver, *urls: str) -> tuple[TenantCtx, list[str]]:
    tenant = await create_tenant(api)
    await register_event_type(api, tenant, EVENT_TYPE)
    ids = []
    for url in urls:
        created = await api.post("/v1/endpoints", json={"url": url}, headers=tenant.headers)
        assert created.status_code == 201, created.text
        ids.append(str(created.json()["id"]))
        await subscribe(api, tenant, ids[-1], "chaos.*")
    return tenant, ids


async def publish_reliably(api: Any, tenant: TenantCtx, n: int, *, prefix: str) -> int:
    """Publish ``n`` events like a well-behaved client: retry 5xx and transport errors with
    the same ``Idempotency-Key`` until a ``202`` arrives. Returns how many retries it took."""
    retries = 0
    for i in range(n):
        for _ in range(200):
            try:
                response = await publish(api, tenant, EVENT_TYPE, {"i": i}, key=f"{prefix}-{i}")
            except Exception:  # the in-process app raised (its DB connection was killed)
                response = None
            if response is not None and response.status_code == 202:
                break
            retries += 1
            await asyncio.sleep(0.02)
        else:  # pragma: no cover - diagnostic
            pytest.fail(f"event {i} was never accepted")
    return retries


async def fan_out(settings: Settings) -> None:
    db = Database.from_settings(settings)
    try:
        relay = Relay(db, settings)
        while (await relay.run_once()).claimed:
            pass
    finally:
        await db.dispose()


async def query(db_url: str, sql: str, *args: object) -> list[asyncpg.Record]:
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        return list(await conn.fetch(sql, *args))
    finally:
        await conn.close()


async def wait_until(
    predicate: Callable[[], Awaitable[bool]], *, within_s: float, interval: float = 0.05
) -> None:
    deadline = time.monotonic() + within_s
    while not await predicate():
        if time.monotonic() > deadline:
            pytest.fail(f"condition not met within {within_s}s")
        await asyncio.sleep(interval)


async def all_settled(db_url: str, expected: int) -> bool:
    [row] = await query(
        db_url,
        "SELECT count(*) AS total, count(*) FILTER (WHERE status = 'succeeded') AS ok "
        "FROM deliveries",
    )
    return bool(row["total"] == expected and row["ok"] == expected)


async def kill_backends(db_url: str) -> int:
    """Terminate every other session on the test database (a restart, as clients see it)."""
    name = make_url(db_url).database
    rows = await query(
        db_url,
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = $1 AND pid <> pg_backend_pid()",
        name,
    )
    return sum(1 for r in rows if r[0])


@dataclass(frozen=True)
class Semantics:
    deliveries: int
    attempts: int
    lease_expired: int
    received_duplicates: int
    acked_duplicates: int


async def assert_at_least_once(db_url: str, receiver: MockReceiver) -> Semantics:
    """The delivery invariants every chaos scenario must satisfy (see the module docstring)."""
    rows = await query(
        db_url,
        """
        SELECT d.id, d.event_id, d.status, d.attempt_count, e.url,
               count(a.id) AS attempt_rows,
               count(a.id) FILTER (WHERE a.error_class = 'lease_expired') AS lease_expired
        FROM deliveries d
        JOIN endpoints e ON e.id = d.endpoint_id
        LEFT JOIN delivery_attempts a ON a.delivery_id = d.id
        GROUP BY d.id, e.url
        """,
    )
    assert rows, "no deliveries were fanned out"
    recorder = receiver.recorder
    acked_dups = received_dups = lease_expired = 0
    for r in rows:
        path, webhook_id = urlsplit(r["url"]).path, str(r["event_id"])
        acked = recorder.acked[(path, webhook_id)]
        received = recorder.received[(path, webhook_id)]
        assert r["status"] == "succeeded", dict(r)
        assert r["attempt_rows"] == r["attempt_count"], dict(r)
        assert acked >= 1, f"delivery {r['id']} was never acknowledged by its receiver"
        assert acked - 1 <= r["lease_expired"], (
            f"{webhook_id} acknowledged {acked}x with only {r['lease_expired']} lost results"
        )
        assert received <= r["attempt_count"], dict(r)
        acked_dups += acked - 1
        received_dups += received - 1
        lease_expired += r["lease_expired"]
    expected_keys = {(urlsplit(r["url"]).path, str(r["event_id"])) for r in rows}
    assert set(recorder.acked) == expected_keys, "the receiver acknowledged unknown messages"
    return Semantics(
        deliveries=len(rows),
        attempts=sum(r["attempt_count"] for r in rows),
        lease_expired=lease_expired,
        received_duplicates=received_dups,
        acked_duplicates=acked_dups,
    )


# -- worker crash ----------------------------------------------------------------------------


def _victim_env(db_url: str, tmp_path: Path, port: int) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if k in {"PATH", "HOME", "LANG", "SYSTEMROOT"}}
    env.update(
        {
            "APP_ENV": "development",
            "LOG_FORMAT": "json",
            "LOG_LEVEL": "WARNING",
            "DATABASE_URL": db_url,
            "SECRETS_ENCRYPTION_KEY": TEST_ENCRYPTION_KEY,
            "ENDPOINT_ALLOW_HTTP": "true",
            "DELIVERY_ALLOW_PRIVATE_NETWORKS": "true",
            "DELIVERY_ALLOWED_PORTS": str(port),
            "WORKER_ID": "chaos-victim",
            "WORKER_CONCURRENCY": "8",
            "WORKER_LEASE_S": "5",
            "WORKER_LEASE_RENEW_INTERVAL_S": "1",
            "WORKER_POLL_MIN_MS": "10",
            "WORKER_POLL_MAX_MS": "50",
            "WORKER_HEARTBEAT_FILE": str(tmp_path / "victim.hb"),
            "ENDPOINT_DEFAULT_MAX_IN_FLIGHT": "50",
            "METRICS_PORT": "0",
            "TRACING_ENABLED": "false",
        }
    )
    return env


async def test_worker_sigkill_mid_delivery_loses_nothing(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    tmp_path: Path,
) -> None:
    receiver = mock.app
    receiver.configure({"default": {"latency_ms": 60_000}})  # hold every request
    tenant, _ = await provision(api, mock, mock.url("/victim"))
    await publish_reliably(api, tenant, 20, prefix="kill")
    settings = chaos_settings()
    await fan_out(settings)

    victim = subprocess.Popen(  # noqa: ASYNC220 - killed below, never awaited for output
        [sys.executable, "-m", "hookrelay.worker"],
        env=_victim_env(db_url, tmp_path, mock.port),
        cwd=tmp_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:

        async def victim_busy() -> bool:
            return sum(receiver.recorder.received.values()) == 8

        await wait_until(victim_busy, within_s=30)
    finally:
        victim.send_signal(signal.SIGKILL)  # no drain, no release: a crash
        await asyncio.to_thread(victim.wait, 10)
    in_flight = {wid for (_, wid) in receiver.recorder.received}
    assert len(in_flight) == 8
    leased = await query(db_url, "SELECT count(*) FROM deliveries WHERE status = 'delivering'")
    assert leased[0][0] == 8  # the dead worker still holds its leases

    receiver.configure({"default": {}})  # the receiver recovers
    running = await start_pipeline(settings, relay=False)
    try:
        await wait_until(lambda: all_settled(db_url, 20), within_s=30)
    finally:
        await running.stop()

    semantics = await assert_at_least_once(db_url, receiver)
    stats = receiver.recorder.stats()
    # The victim's 8 requests reached the receiver but were never answered (it hung up):
    # they are redelivered exactly once each, and nothing was acknowledged twice.
    assert (stats["received"], stats["unique"], stats["acked"]) == (28, 20, 20)
    assert stats["disconnected"] == 8
    assert semantics.acked_duplicates == 0
    assert semantics.lease_expired == 8
    rows = await query(
        db_url,
        "SELECT d.event_id, array_agg(a.error_class ORDER BY a.attempt_number) AS classes, "
        "array_agg(a.worker_id ORDER BY a.attempt_number) AS workers "
        "FROM deliveries d JOIN delivery_attempts a ON a.delivery_id = d.id GROUP BY d.event_id",
    )
    for r in rows:
        if str(r["event_id"]) in in_flight:
            assert r["classes"] == ["lease_expired", None]
            assert r["workers"][0] == "chaos-victim"
        else:
            assert r["classes"] == [None]
    assert running.worker.stats.reclaimed_retried == 8


# -- Postgres restart / failover ----------------------------------------------------------


async def test_postgres_connection_storm_during_traffic(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    tenant, _ = await provision(
        api,
        mock,
        mock.url("/fast"),
        mock.url("/flaky", failure_rate=0.2, failure_status=503),
        mock.url("/slow", latency_ms=30),
    )
    settings = chaos_settings()
    running = await start_pipeline(settings)
    killed: list[int] = []

    async def chaos() -> None:
        for _ in range(8):
            await asyncio.sleep(0.15)
            killed.append(await kill_backends(db_url))

    try:
        monkey = asyncio.create_task(chaos())
        client_retries = await publish_reliably(api, tenant, 60, prefix="storm")
        await monkey
        await wait_until(lambda: all_settled(db_url, 180), within_s=60)
    finally:
        await running.stop()

    assert sum(killed) > 0, "the chaos monkey never hit a live connection"
    # Idempotency keys turn client retries during the outage into exactly one event each.
    [events] = await query(db_url, "SELECT count(*) FROM events")
    assert events[0] == 60
    semantics = await assert_at_least_once(db_url, mock.app)
    assert semantics.deliveries == 180
    # Both loops survived every disconnect and kept going.
    assert all(t.done() and t.exception() is None for t in running.tasks)
    print(  # noqa: T201 - shown with -s; the numbers are useful when tuning
        f"storm: killed={sum(killed)} client_retries={client_retries} {semantics} "
        f"completion_retries={running.worker.stats.completion_retries}"
    )


# Both completion paths: one transaction per attempt (WORKER_COMPLETE_BATCH_MAX=1, the pre-M9
# behaviour) and group commit (the writer is complete_many).
COMPLETION_PATHS = pytest.mark.parametrize(
    ("batch_max", "writer"), [(1, "complete"), (32, "complete_many")], ids=["single", "batched"]
)


@COMPLETION_PATHS
async def test_completion_is_retried_when_the_backend_dies_mid_write(
    api: Any,
    *,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
    batch_max: int,
    writer: str,
) -> None:
    tenant, _ = await provision(api, mock, mock.url("/once"))
    await publish_reliably(api, tenant, 1, prefix="mid-write")
    settings = chaos_settings(worker_complete_batch_max=batch_max)
    await fan_out(settings)
    real_complete = getattr(delivery_queue, writer)
    calls = Counter[str]()

    async def killed_then_real(session: Any, *args: Any, **kwargs: Any) -> Any:
        calls["complete"] += 1
        if calls["complete"] == 1:
            # The receiver already answered 2xx; now our connection dies under the write.
            pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            await query(db_url, "SELECT pg_terminate_backend($1)", pid)
        return await real_complete(session, *args, **kwargs)

    monkeypatch.setattr(delivery_queue, writer, killed_then_real)
    db = Database.from_settings(settings)
    worker = Worker(db, settings)
    try:
        stats = await worker.run_once()
    finally:
        await worker.aclose()
        await db.dispose()
    assert calls["complete"] == 2
    assert (stats.succeeded, stats.completion_retries, stats.loop_errors) == (1, 1, 0)
    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.attempts, semantics.acked_duplicates, semantics.lease_expired) == (1, 0, 0)
    assert mock.app.recorder.stats()["received"] == 1


@COMPLETION_PATHS
async def test_without_completion_retries_a_lost_write_means_one_redelivery(
    api: Any,
    *,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
    batch_max: int,
    writer: str,
) -> None:
    """The baseline the retry improves on: the reaper still guarantees delivery, at the
    price of exactly one duplicate the receiver must deduplicate by ``webhook-id``."""
    tenant, _ = await provision(api, mock, mock.url("/once"))
    await publish_reliably(api, tenant, 1, prefix="no-retry")
    settings = chaos_settings(worker_complete_retries=0, worker_complete_batch_max=batch_max)
    await fan_out(settings)
    real_complete = getattr(delivery_queue, writer)
    calls = Counter[str]()

    async def killed_once(session: Any, *args: Any, **kwargs: Any) -> Any:
        calls["complete"] += 1
        if calls["complete"] == 1:
            pid = (await session.execute(text("SELECT pg_backend_pid()"))).scalar_one()
            await query(db_url, "SELECT pg_terminate_backend($1)", pid)
        return await real_complete(session, *args, **kwargs)

    monkeypatch.setattr(delivery_queue, writer, killed_once)
    db = Database.from_settings(settings)
    worker = Worker(db, settings)
    try:
        first = await worker.run_once()
        assert (first.succeeded, first.loop_errors) == (0, 1)  # the lease is left to expire
        await query(db_url, "UPDATE deliveries SET leased_until = now() - interval '1 second'")
        assert (await worker.reclaim_once()).retried == 1
        await query(db_url, "UPDATE deliveries SET next_attempt_at = now()")
        assert (await worker.run_once()).succeeded == 1
    finally:
        await worker.aclose()
        await db.dispose()
    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.attempts, semantics.lease_expired, semantics.acked_duplicates) == (2, 1, 1)


# -- slow, failing and hanging receivers ----------------------------------------------------


async def test_slow_receiver_does_not_starve_a_healthy_one(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    tenant, [slow_id, _] = await provision(
        api, mock, mock.url("/slow", latency_ms=5_000), mock.url("/healthy", latency_ms=5)
    )
    patched = await api.patch(
        f"/v1/endpoints/{slow_id}",
        json={"timeout_ms": 1_000, "max_in_flight": 2},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert patched.status_code == 200, patched.text
    await publish_reliably(api, tenant, 15, prefix="slow")
    settings = chaos_settings(worker_concurrency=4, delivery_max_attempts=2)
    await fan_out(settings)
    started = time.monotonic()
    running = await start_pipeline(settings, relay=False)

    async def healthy_done() -> bool:
        return sum(mock.app.recorder.acked.values()) == 15

    try:
        await wait_until(healthy_done, within_s=20)
        healthy_s = time.monotonic() - started
        rows = await query(
            db_url,
            "SELECT count(*) FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id "
            "WHERE e.url LIKE '%/slow%' AND d.status IN ('dead', 'failed', 'succeeded')",
        )
        slow_finished = rows[0][0]
    finally:
        await running.stop()
    # The slow endpoint may hold at most 2 of the 4 slots, so 15 healthy deliveries never
    # wait behind 15 slow ones (serially that would be > 15 s of timeouts).
    assert healthy_s < 10, healthy_s
    assert slow_finished < 15
    timeouts = await query(
        db_url, "SELECT count(*) FROM delivery_attempts WHERE error_class = 'timeout'"
    )
    assert timeouts[0][0] >= 2
    assert mock.app.recorder.stats()["acked_duplicates"] == 0


async def test_flaky_rate_limiting_and_hanging_receivers_converge_exactly_once(
    api: Any, mock: RunningReceiver, chaos_settings: Callable[..., Settings], db_url: str
) -> None:
    tenant, ids = await provision(
        api,
        mock,
        mock.url("/flaky", failure_rate=0.5, failure_status="500,502,503"),
        mock.url("/limited", failure_rate=0.5, failure_status=429, retry_after_s=0),
        mock.url("/hangs", hang_rate=0.2, hang_ms=3_000),
    )
    patched = await api.patch(
        f"/v1/endpoints/{ids[2]}",
        json={"timeout_ms": 1_000},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert patched.status_code == 200, patched.text
    await publish_reliably(api, tenant, 20, prefix="flaky")
    settings = chaos_settings()
    await fan_out(settings)
    running = await start_pipeline(settings, relay=False)
    try:
        await wait_until(lambda: all_settled(db_url, 60), within_s=60)
    finally:
        await running.stop()

    semantics = await assert_at_least_once(db_url, mock.app)
    stats = mock.app.recorder.stats()
    # Failures were real (many more attempts than deliveries), yet with no lost results every
    # message was acknowledged exactly once: retries only follow non-2xx or hang-ups.
    assert semantics.attempts > 60
    assert semantics.lease_expired == 0
    assert semantics.acked_duplicates == 0
    assert stats["max_acked_per_message"] == 1
    assert stats["hung"] >= 1
    assert stats["disconnected"] == stats["hung"]
    statuses = set(stats["by_status"])
    assert {"204", "429"} <= statuses
    assert statuses & {"500", "502", "503"}


async def test_drain_during_a_database_outage_leaves_leases_for_the_reaper(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock.app.configure({"default": {"latency_ms": 60_000}})
    tenant, _ = await provision(api, mock, mock.url("/drain"))
    await publish_reliably(api, tenant, 3, prefix="drain")
    settings = chaos_settings(worker_drain_timeout_s=0.1)
    await fan_out(settings)

    async def database_down(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("connection refused")

    running = await start_pipeline(settings, relay=False)

    async def all_in_flight() -> bool:
        return sum(mock.app.recorder.received.values()) == 3

    try:
        await wait_until(all_in_flight, within_s=10)
        monkeypatch.setattr(delivery_queue, "release_leases", database_down)
    finally:
        await running.stop()  # must not raise although the release cannot be written
    assert running.worker.stats.release_failures == 1
    assert running.worker.stats.released == 0
    monkeypatch.undo()

    # Nothing is lost: once the leases expire another worker retries every delivery.
    mock.app.configure({"default": {}})
    await query(db_url, "UPDATE deliveries SET leased_until = now() - interval '1 second'")
    rescue = await start_pipeline(settings, relay=False)
    try:
        await wait_until(lambda: all_settled(db_url, 3), within_s=20)
    finally:
        await rescue.stop()
    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.lease_expired, semantics.acked_duplicates) == (3, 0)


async def test_relay_does_not_park_rows_when_the_connection_is_lost(
    api: Any,
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A Postgres restart surfaces in the relay as SQLSTATE 57P01 / PendingRollbackError, not
    a flagged disconnect. That must abort the batch for a retry on a fresh connection instead
    of running poison-row isolation, which would only fail again and could park healthy rows."""
    tenant, _ = await provision(api, mock, mock.url("/r"))
    await publish_reliably(api, tenant, 3, prefix="lost")
    real_fan_out = fanout_service.fan_out

    class _AdminShutdownError(Exception):
        sqlstate = "57P01"

    async def restarting(*_args: Any, **_kwargs: Any) -> int:
        orig = Exception("terminating connection due to administrator command")
        orig.__cause__ = _AdminShutdownError()
        raise DBAPIError("INSERT INTO deliveries", {}, orig)

    async def rollback_pending(*_args: Any, **_kwargs: Any) -> int:
        raise PendingRollbackError("Can't reconnect until invalid transaction is rolled back")

    settings = chaos_settings()
    db = Database.from_settings(settings)
    try:
        relay = Relay(db, settings)
        for broken in (restarting, rollback_pending):
            monkeypatch.setattr(fanout_service, "fan_out", broken)
            with pytest.raises((DBAPIError, PendingRollbackError)):
                await relay.run_once()
        assert relay.stats.batch_fallbacks == 0
        rows = await query(db_url, "SELECT attempts, processed_at FROM event_outbox")
        assert [(r["attempts"], r["processed_at"]) for r in rows] == [(0, None)] * 3
        monkeypatch.setattr(fanout_service, "fan_out", real_fan_out)
        result = await relay.run_once()  # next iteration: same rows, healthy connection
        assert (result.claimed, result.processed, result.deliveries_created) == (3, 3, 3)
    finally:
        await db.dispose()


async def test_mock_receiver_serves_control_routes(mock: RunningReceiver) -> None:
    async with httpx.AsyncClient(base_url=mock.url("")) as client:
        assert (await client.get("/health")).json() == {"status": "ok"}
        hook = await client.post("/hook?latency_ms=1", headers={"webhook-id": "m"}, content=b"{}")
        assert hook.status_code == 204
        stats = (await client.get("/stats")).json()
        assert (stats["received"], stats["acked"], stats["unsigned"]) == (1, 1, 1)
        put = await client.put("/config", json={"paths": {"/x": {"failure_rate": 1}}})
        assert put.status_code == 200
        assert (await client.post("/x")).status_code == 500
        assert (await client.put("/config", json=[1])).status_code == 422
        assert (await client.post("/x?latency_ms=-1")).status_code == 400
        assert (await client.post("/reset")).json() == {"reset": True}
        assert (await client.get("/stats")).json()["received"] == 0
        assert (await client.delete("/hook")).status_code == 405
