"""Milestone 5 retention on real Postgres: payload purge (busy and not-yet-fanned-out events
are skipped, stored response bodies are scrubbed, the cursor moves past busy rows, concurrent
runners never double-count), what purged events look like through the API, replays and the
worker, the CLI exit codes and the relay's housekeeping pass.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

from hookrelay import retention as retention_cli
from hookrelay.db.engine import Database
from hookrelay.relay import Relay
from hookrelay.services.retention import RetentionPolicy, RetentionRunner, purge_payloads
from tests.integration.conftest import _asyncpg_dsn, publish
from tests.integration.test_replays import _run, dead_backlog  # noqa: F401 - fixture
from tests.integration.test_worker import (  # noqa: F401 - pytest fixtures
    _fetch,
    _setup,
    _val,
    api,
    database,
    db_url,
    make_worker,
    receiver,
    settings_for,
    worker_template,
)
from tests.receiver import Receiver, Reply

DAY = 86_400


async def _age_events(db_url: str, days: float = 3) -> None:
    await _run(
        db_url,
        "UPDATE events SET created_at = now() - make_interval(secs => $1)",
        float(days * DAY),
    )


def _policy(**overrides: Any) -> RetentionPolicy:
    base: dict[str, Any] = {
        "batch_size": 100,
        "max_batches": 50,
        "payload_retention_s": DAY,
        "expire_events": False,
    }
    base.update(overrides)
    return RetentionPolicy(**base)


async def _deliver_all(make_worker: Any) -> None:
    worker = make_worker()
    previous = -1
    while worker.stats.leased != previous:
        previous = worker.stats.leased
        await worker.run_once()


async def test_purges_only_old_settled_events_and_scrubs_responses(
    api: httpx.AsyncClient,
    *,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    database: Database,
    db_url: str,
) -> None:
    receiver.handler = lambda _r: Reply(200, body=b"thanks, stored")
    tenant, _endpoint_id, _, event_ids = await _setup(api, settings_for(), receiver, events=3)
    await _deliver_all(make_worker)
    # One more event that is still pending (busy) and one not fanned out yet.
    busy = (await publish(api, tenant, "invoice.paid", {"busy": True})).json()["id"]
    db = Database.from_settings(settings_for())
    try:
        await Relay(db, settings_for()).run_once()
    finally:
        await db.dispose()
    unfanned = (await publish(api, tenant, "invoice.paid", {"later": True})).json()["id"]
    young = None
    await _age_events(db_url)
    young = (await publish(api, tenant, "invoice.paid", {"young": True})).json()["id"]
    assert (
        await _val(db_url, "SELECT count(*) FROM delivery_attempts WHERE response_body IS NOT NULL")
        == 3
    )

    report = await RetentionRunner(database, _policy()).run()
    assert report.payloads == 3
    assert report.responses_scrubbed == 3
    assert report.payloads_skipped_busy == 2  # the pending one and the un-fanned-out one
    assert report.complete
    rows = {
        str(r["id"]): r
        for r in await _fetch(db_url, "SELECT id, payload, payload_purged_at FROM events")
    }
    for event_id in event_ids:
        assert rows[event_id]["payload"] is None
        assert rows[event_id]["payload_purged_at"] is not None
    for kept in (busy, unfanned, young):
        assert rows[kept]["payload"] is not None
    assert (
        await _val(db_url, "SELECT count(*) FROM delivery_attempts WHERE response_body IS NOT NULL")
        == 0
    )
    # Metadata survives: the event, its deliveries and attempts are still readable.
    assert await _val(db_url, "SELECT count(*) FROM delivery_attempts") == 3

    got = await api.get(f"/v1/events/{event_ids[0]}", headers=tenant.headers)
    assert got.status_code == 200
    assert got.json()["payload"] is None
    assert got.json()["payload_purged_at"] is not None
    kept_event = await api.get(f"/v1/events/{busy}", headers=tenant.headers)
    assert kept_event.json()["payload"] == {"busy": True}
    assert kept_event.json()["payload_purged_at"] is None

    # Idempotent: a second run finds nothing new.
    again = await RetentionRunner(database, _policy()).run()
    assert (again.payloads, again.responses_scrubbed) == (0, 0)


async def test_cursor_moves_past_busy_events(
    api: httpx.AsyncClient,
    *,
    settings_for: Any,
    receiver: Receiver,
    make_worker: Any,
    database: Database,
    db_url: str,
) -> None:
    """Five busy events older than five settled ones, batch size 2: the runner walks past the
    busy rows instead of rescanning them, and purges every settled one in one run."""
    tenant, _, _, busy_ids = await _setup(api, settings_for(), receiver, events=5)
    await _age_events(db_url, days=5)  # the pending (busy) ones are the oldest
    for i in range(5):
        assert (await publish(api, tenant, "invoice.paid", {"i": i})).status_code == 202
    db = Database.from_settings(settings_for())
    try:
        relay = Relay(db, settings_for())
        while (await relay.run_once()).claimed:
            pass
    finally:
        await db.dispose()
    # Settle only the newer five.
    await _run(
        db_url,
        "UPDATE deliveries SET status = 'succeeded', completed_at = now() "
        "WHERE event_id <> ALL($1::uuid[])",
        busy_ids,
    )
    await _run(
        db_url,
        "UPDATE events SET created_at = now() - interval '2 days' WHERE id <> ALL($1::uuid[])",
        busy_ids,
    )
    report = await RetentionRunner(database, _policy(batch_size=2)).run()
    assert report.payloads == 5
    assert report.payloads_skipped_busy == 5
    assert report.complete
    assert report.batches >= 5 + 1  # 10 rows / 2 per batch, plus the secret pass

    # A budget that cannot cover the backlog reports incomplete.
    await _run(db_url, "UPDATE deliveries SET status = 'succeeded', completed_at = now()")
    short = await RetentionRunner(database, _policy(batch_size=1, max_batches=2)).run()
    assert short.payloads == 2
    assert short.complete is False


async def test_concurrent_runners_never_double_count(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Receiver,
    database: Database,
    db_url: str,
) -> None:
    await _setup(api, settings_for(), receiver, events=30)
    await _run(db_url, "UPDATE deliveries SET status = 'failed', completed_at = now()")
    await _age_events(db_url)
    reports = await asyncio.gather(
        *(RetentionRunner(database, _policy(batch_size=3)).run() for _ in range(4))
    )
    assert sum(r.payloads for r in reports) == 30
    assert await _val(db_url, "SELECT count(*) FROM events WHERE payload IS NOT NULL") == 0


async def test_purge_waits_for_a_delivery_being_replayed(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, database: Database, db_url: str
) -> None:
    """A delivery locked by a replay in flight: the purge blocks on its share lock, then sees
    the delivery pending again and skips the event (never purges a payload about to be sent)."""
    await _setup(api, settings_for(), receiver)
    await _run(db_url, "UPDATE deliveries SET status = 'dead', completed_at = now()")
    await _age_events(db_url)
    conn = await asyncpg.connect(_asyncpg_dsn(db_url))
    try:
        tx = conn.transaction()
        await tx.start()
        await conn.execute("SELECT id FROM deliveries FOR UPDATE")
        purge = asyncio.create_task(RetentionRunner(database, _policy()).run())
        await asyncio.sleep(0.3)
        assert not purge.done(), purge.result()
        await conn.execute(
            "UPDATE deliveries SET status = 'pending', completed_at = NULL, next_attempt_at = now()"
        )
        await tx.commit()
        report = await asyncio.wait_for(purge, 10)
    finally:
        await conn.close()
    assert report.payloads == 0
    assert report.payloads_skipped_busy == 1
    assert await _val(db_url, "SELECT payload IS NOT NULL FROM events") is True


async def test_purge_batch_with_nothing_due(database: Database) -> None:
    async with database.session() as session:
        batch = await purge_payloads(session, older_than_s=DAY, batch_size=10)
    assert (batch.scanned, batch.purged, batch.last) == (0, 0, None)


async def test_replays_refuse_purged_events(
    api: httpx.AsyncClient,
    database: Database,
    db_url: str,
    dead_backlog: Any,
) -> None:
    tenant, _, event_ids, _ = await dead_backlog(events=2)
    await _age_events(db_url)
    report = await RetentionRunner(database, _policy()).run()
    assert report.payloads == 2
    [delivery_id, _] = [
        str(r["id"]) for r in await _fetch(db_url, "SELECT id FROM deliveries ORDER BY id")
    ]
    single = await api.post(f"/v1/deliveries/{delivery_id}/replay", headers=tenant.headers)
    assert single.status_code == 409
    error = single.json()["error"]
    assert error["code"] == "payload_purged"
    assert error["details"]["payload_purged_at"] is not None
    by_event = await api.post(f"/v1/events/{event_ids[0]}/replay", headers=tenant.headers)
    assert by_event.status_code == 409
    assert by_event.json()["error"]["code"] == "payload_purged"

    now = datetime.now(UTC)
    bulk = await api.post(
        "/v1/replays",
        json={
            "created_after": (now - timedelta(hours=1)).isoformat(),
            "created_before": (now + timedelta(minutes=1)).isoformat(),
        },
        headers=tenant.headers,
    )
    assert bulk.status_code == 202, bulk.text
    assert bulk.json()["replayed"] == 0  # purged events are skipped, not failed
    assert {r["status"] for r in await _fetch(db_url, "SELECT status FROM deliveries")} == {"dead"}


async def test_worker_fails_a_delivery_whose_payload_is_gone(
    api: httpx.AsyncClient, settings_for: Any, receiver: Receiver, make_worker: Any, db_url: str
) -> None:
    """Defence in depth: a payload missing at send time (e.g. purged by hand) ends the delivery
    as failed/payload_purged without a request and without counting against the breaker."""
    await _setup(api, settings_for(), receiver)
    await _run(db_url, "UPDATE events SET payload = NULL, payload_purged_at = now()")
    stats = await make_worker().run_once()
    assert stats.failed == 1
    assert receiver.requests == []
    [delivery] = await _fetch(db_url, "SELECT status, last_error_class FROM deliveries")
    assert delivery["status"] == "failed"
    [attempt] = await _fetch(
        db_url, "SELECT outcome, error_class, status_code FROM delivery_attempts"
    )
    assert (attempt["outcome"], attempt["error_class"]) == ("failed", "payload_purged")
    assert attempt["status_code"] is None
    state = await _fetch(db_url, "SELECT consecutive_failures FROM endpoint_states")
    assert all(s["consecutive_failures"] == 0 for s in state)


async def test_cli_run_and_exit_codes(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Receiver,
    db_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _setup(api, settings_for(), receiver, events=3)
    await _run(db_url, "UPDATE deliveries SET status = 'succeeded', completed_at = now()")
    await _age_events(db_url)
    settings = settings_for(
        event_payload_retention_days=1, retention_batch_size=1, retention_batch_pause_ms=0
    )
    short = await retention_cli.run(settings, max_batches=1)
    assert short.payloads == 1
    assert short.complete is False

    monkeypatch.setattr(retention_cli, "get_settings", lambda: settings)
    # main() owns its event loop (asyncio.run), so run it off this loop's thread.
    incomplete = await asyncio.to_thread(retention_cli.main, ["--max-batches", "1"])
    assert incomplete == retention_cli.EXIT_INCOMPLETE
    assert await asyncio.to_thread(retention_cli.main, []) == 0
    assert await _val(db_url, "SELECT count(*) FROM events WHERE payload IS NOT NULL") == 0


async def test_relay_housekeeping_runs_retention(
    api: httpx.AsyncClient,
    settings_for: Any,
    receiver: Receiver,
    database: Database,
    db_url: str,
) -> None:
    tenant, endpoint_id, _, _ = await _setup(api, settings_for(), receiver, events=2)
    await _run(db_url, "UPDATE deliveries SET status = 'succeeded', completed_at = now()")
    await _age_events(db_url)
    rotated = await api.post(
        f"/v1/endpoints/{endpoint_id}/secret/rotate",
        json={"overlap_s": 60},
        headers={**tenant.headers, "If-Match": "*"},
    )
    assert rotated.status_code == 200
    await _run(db_url, "UPDATE endpoints SET previous_secret_expires_at = now()")

    # Payload retention off (the default): only the ended overlap is cleaned up.
    relay = Relay(database, settings_for())
    await relay.housekeep()
    assert relay.stats.purged_payloads == 0
    assert relay.stats.cleared_previous_secrets == 1

    relay = Relay(database, settings_for(event_payload_retention_days=1))
    await relay.housekeep()
    assert relay.stats.purged_payloads == 2
    assert await _val(db_url, "SELECT count(*) FROM events WHERE payload IS NULL") == 2
