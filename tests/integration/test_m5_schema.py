"""Migration 0009 on real Postgres: the backfills of ``deliveries.event_type`` and
``delivery_attempts.endpoint_id`` from existing rows, the triggers that keep them derived, the
new CHECK constraints and FKs, and the downgrade of purged payloads.
"""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest
from alembic import command

from tests.integration.conftest import _asyncpg_dsn, alembic_config
from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant
from tests.integration.test_m2_schema import _event
from tests.integration.test_migrations import _fetch, _schema_diff


async def _delivery(
    conn: asyncpg.Connection, tenant: uuid.UUID, event: uuid.UUID, endpoint: uuid.UUID
) -> uuid.UUID:
    delivery_id: uuid.UUID = await conn.fetchval(
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
        "VALUES ($1, $2, $3, now() + interval '1 day') RETURNING id",
        tenant,
        event,
        endpoint,
    )
    return delivery_id


async def _attempt(
    conn: asyncpg.Connection, tenant: uuid.UUID, delivery: uuid.UUID, number: int = 1
) -> uuid.UUID:
    attempt_id: uuid.UUID = await conn.fetchval(
        "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
        "status_code, error_class, request_url, worker_id, started_at, finished_at) "
        "VALUES ($1, $2, $3, 'retrying', 503, 'http_status', 'https://a.example.com/', 'w', "
        "now(), now()) RETURNING id",
        tenant,
        delivery,
        number,
    )
    return attempt_id


async def _seed_m4(url: str) -> dict[str, uuid.UUID]:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        tenant = await _tenant(conn, "m5-backfill")
        await _event_type(conn, tenant, "order.created")
        endpoint = await _endpoint(conn, tenant, "https://a.example.com/")
        event = await _event(conn, tenant, "order.created")
        delivery = await _delivery(conn, tenant, event, endpoint)
        await _attempt(conn, tenant, delivery)
        return {"tenant": tenant, "endpoint": endpoint, "event": event, "delivery": delivery}
    finally:
        await conn.close()


def test_0009_backfills_and_downgrades_with_data(scratch_database_url: str) -> None:
    url = scratch_database_url
    cfg = alembic_config(url)
    command.upgrade(cfg, "0008")
    ids = asyncio.run(_seed_m4(url))
    command.upgrade(cfg, "head")
    [delivery] = asyncio.run(_fetch(url, "SELECT event_type FROM deliveries"))
    assert delivery["event_type"] == "order.created"
    [attempt] = asyncio.run(_fetch(url, "SELECT endpoint_id FROM delivery_attempts"))
    assert attempt["endpoint_id"] == ids["endpoint"]

    # Purge the payload and start a rotation, then go back to 0008.
    asyncio.run(
        _fetch(url, "UPDATE events SET payload = NULL, payload_purged_at = now() RETURNING id")
    )
    asyncio.run(
        _fetch(
            url,
            "UPDATE endpoints SET previous_secret_ciphertext = secret_ciphertext, "
            "previous_secret_expires_at = now() + interval '1 hour', secret_rotated_at = now() "
            "RETURNING id",
        )
    )
    command.downgrade(cfg, "0008")
    [event] = asyncio.run(_fetch(url, "SELECT payload FROM events"))
    assert event["payload"] == "{}"  # the data is gone; NOT NULL holds again
    columns = asyncio.run(
        _fetch(
            url,
            "SELECT table_name, column_name FROM information_schema.columns "
            "WHERE column_name IN ('event_type', 'endpoint_id', 'payload_purged_at', "
            "'previous_secret_ciphertext', 'secret_rotated_at') "
            "AND table_name IN ('events', 'deliveries', 'delivery_attempts', 'endpoints')",
        )
    )
    assert {(c["table_name"], c["column_name"]) for c in columns} == {
        ("events", "event_type"),  # the original column; the copies are gone
        ("deliveries", "endpoint_id"),
    }
    command.upgrade(cfg, "head")
    assert _schema_diff(url) == []


async def _with_head(url: str) -> asyncpg.Connection:
    return await asyncpg.connect(_asyncpg_dsn(url))


@pytest.fixture
def head_url(scratch_database_url: str) -> str:
    command.upgrade(alembic_config(scratch_database_url), "head")
    return scratch_database_url


async def test_triggers_derive_columns_and_constraints_hold(head_url: str) -> None:
    conn = await _with_head(head_url)
    try:
        tenant = await _tenant(conn, "m5-triggers")
        other = await _tenant(conn, "m5-other")
        await _event_type(conn, tenant, "a.b")
        await _event_type(conn, other, "c.d")
        endpoint = await _endpoint(conn, tenant, "https://a.example.com/")
        other_endpoint = await _endpoint(conn, other, "https://b.example.com/")
        event = await _event(conn, tenant, "a.b")
        other_event = await _event(conn, other, "c.d")

        # A writer cannot set the derived columns to something else: the trigger wins.
        delivery = await conn.fetchval(
            "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, event_type) "
            "VALUES ($1, $2, $3, now() + interval '1 day', 'forged.type') RETURNING id",
            tenant,
            event,
            endpoint,
        )
        assert (
            await conn.fetchval("SELECT event_type FROM deliveries WHERE id = $1", delivery)
            == "a.b"
        )
        attempt = await conn.fetchval(
            "INSERT INTO delivery_attempts (tenant_id, delivery_id, endpoint_id, attempt_number, "
            "outcome, status_code, request_url, worker_id, started_at, finished_at) "
            "VALUES ($1, $2, $3, 1, 'succeeded', 200, 'https://a.example.com/', 'w', now(), "
            "now()) RETURNING id",
            tenant,
            delivery,
            other_endpoint,
        )
        assert (
            await conn.fetchval("SELECT endpoint_id FROM delivery_attempts WHERE id = $1", attempt)
            == endpoint
        )

        # Cross-tenant or unknown references fail on their FKs, not on NOT NULL.
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await _delivery(conn, tenant, other_event, endpoint)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await _delivery(conn, tenant, uuid.uuid4(), endpoint)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await _attempt(conn, tenant, uuid.uuid4())
        other_delivery = await _delivery(conn, other, other_event, other_endpoint)
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await _attempt(conn, tenant, other_delivery)

        # Payload and purge marker go together; payload must stay a JSON object.
        with pytest.raises(asyncpg.CheckViolationError, match="payload_iff_not_purged"):
            await conn.execute("UPDATE events SET payload = NULL WHERE id = $1", event)
        with pytest.raises(asyncpg.CheckViolationError, match="payload_iff_not_purged"):
            await conn.execute("UPDATE events SET payload_purged_at = now() WHERE id = $1", event)
        with pytest.raises(asyncpg.CheckViolationError, match="payload_object"):
            await conn.execute("UPDATE events SET payload = '[]' WHERE id = $1", event)
        await conn.execute(
            "UPDATE events SET payload = NULL, payload_purged_at = now() WHERE id = $1", event
        )

        # The previous secret is ciphertext and comes with its expiry.
        with pytest.raises(asyncpg.CheckViolationError, match="previous_secret_iff_expiry"):
            await conn.execute(
                "UPDATE endpoints SET previous_secret_ciphertext = secret_ciphertext WHERE id = $1",
                endpoint,
            )
        with pytest.raises(asyncpg.CheckViolationError, match="previous_secret_encrypted"):
            await conn.execute(
                "UPDATE endpoints SET previous_secret_ciphertext = 'whsec_plain', "
                "previous_secret_expires_at = now() WHERE id = $1",
                endpoint,
            )

        # Deleting an endpoint still cascades to its attempts via the new FK.
        await conn.execute("DELETE FROM endpoints WHERE id = $1", endpoint)
        assert await conn.fetchval("SELECT count(*) FROM delivery_attempts") == 0
    finally:
        await conn.close()
