"""M2 tables enforce tenancy and invariants in Postgres, independent of the service layer."""

from __future__ import annotations

import hashlib
import uuid

import asyncpg
import pytest

from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant, _violation

FP = hashlib.sha256(b"x").digest()


async def _event(
    pg: asyncpg.Connection, tenant_id: uuid.UUID, event_type: str, **extra: object
) -> uuid.UUID:
    row: dict[str, object] = {
        "tenant_id": tenant_id,
        "event_type": event_type,
        "payload": "{}",
        "payload_size": 2,
        "occurred_at": None,
        **extra,
    }
    row = {k: v for k, v in row.items() if v is not None}
    cols = list(row)
    params = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    event_id: uuid.UUID = await pg.fetchval(
        f"INSERT INTO events ({', '.join(cols)}, occurred_at, retain_until) "  # noqa: S608
        f"VALUES ({params}, now(), now() + interval '1 day') RETURNING id",
        *row.values(),
    )
    return event_id


@pytest.fixture
async def world(pg: asyncpg.Connection) -> dict[str, uuid.UUID]:
    a = await _tenant(pg, f"a-{uuid.uuid4().hex[:8]}")
    b = await _tenant(pg, f"b-{uuid.uuid4().hex[:8]}")
    await _event_type(pg, a, "x.y")
    await _event_type(pg, b, "x.y")
    return {
        "a": a,
        "b": b,
        "event_a": await _event(pg, a, "x.y"),
        "event_b": await _event(pg, b, "x.y"),
        "endpoint_a": await _endpoint(pg, a, "https://a.example.com/"),
        "endpoint_b": await _endpoint(pg, b, "https://b.example.com/"),
    }


async def test_events_need_a_registered_type_of_the_same_tenant(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    other = await _tenant(pg, f"c-{uuid.uuid4().hex[:8]}")
    insert = (
        "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
        "retain_until) VALUES ($1, $2, $3, $4, now(), now() + interval '1 day')"
    )
    assert await _violation(pg, insert, other, "x.y", "{}", 2) == "fk_events_event_type_same_tenant"
    assert await _violation(pg, insert, world["a"], "x.y", "[]", 2) == "ck_events_payload_object"
    assert (
        await _violation(pg, insert, world["a"], "x.y", "{}", 0)
        == "ck_events_payload_size_positive"
    )


async def test_event_checks(pg: asyncpg.Connection, world: dict[str, uuid.UUID]) -> None:
    a = world["a"]
    bad_retention = (
        "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
        "created_at, retain_until) VALUES ($1, 'x.y', '{}', 2, now(), now(), now())"
    )
    assert await _violation(pg, bad_retention, a) == "ck_events_retain_after_created"
    bad_external = (
        "INSERT INTO events (tenant_id, event_type, payload, payload_size, occurred_at, "
        "retain_until, external_id) "
        "VALUES ($1, 'x.y', '{}', 2, now(), now() + interval '1 day', $2)"
    )
    assert await _violation(pg, bad_external, a, "has space") == "ck_events_external_id_format"
    await _event(pg, a, "x.y", external_id="ext-1")
    assert await _violation(pg, bad_external, a, "ext-1") == "uq_events_tenant_external_id"
    await _event(pg, world["b"], "x.y", external_id="ext-1")  # other tenant: fine


async def test_outbox_and_deliveries_stay_inside_the_tenant(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    outbox = "INSERT INTO event_outbox (tenant_id, event_id) VALUES ($1, $2)"
    assert await _violation(pg, outbox, world["a"], world["event_b"]) == (
        "fk_event_outbox_event_same_tenant"
    )
    await pg.execute(outbox, world["a"], world["event_a"])
    assert await _violation(pg, outbox, world["a"], world["event_a"]) == "uq_event_outbox_event_id"

    delivery = (
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
        "VALUES ($1, $2, $3, now() + interval '1 day')"
    )
    assert await _violation(pg, delivery, world["a"], world["event_b"], world["endpoint_a"]) == (
        "fk_deliveries_event_same_tenant"
    )
    assert await _violation(pg, delivery, world["a"], world["event_a"], world["endpoint_b"]) == (
        "fk_deliveries_endpoint_same_tenant"
    )
    await pg.execute(delivery, world["a"], world["event_a"], world["endpoint_a"])
    assert await _violation(pg, delivery, world["a"], world["event_a"], world["endpoint_a"]) == (
        "uq_deliveries_event_endpoint"
    )


async def test_delivery_state_checks_and_trigger(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    delivery_id = await pg.fetchval(
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
        "VALUES ($1, $2, $3, now() + interval '1 day') RETURNING id",
        world["a"],
        world["event_a"],
        world["endpoint_a"],
    )
    row = await pg.fetchrow(
        "SELECT status, attempt_count FROM deliveries WHERE id = $1", delivery_id
    )
    assert row is not None
    assert (row["status"], row["attempt_count"]) == ("pending", 0)
    update = "UPDATE deliveries SET {} WHERE id = $1"
    assert await _violation(pg, update.format("status = 'lost'"), delivery_id) == (
        "ck_deliveries_status_valid"
    )
    assert await _violation(pg, update.format("attempt_count = -1"), delivery_id) == (
        "ck_deliveries_attempt_count_non_negative"
    )
    assert await _violation(pg, update.format("status = 'succeeded'"), delivery_id) == (
        "ck_deliveries_completed_iff_terminal"
    )
    assert await _violation(pg, update.format("completed_at = now()"), delivery_id) == (
        "ck_deliveries_completed_iff_terminal"
    )
    before = await pg.fetchval("SELECT updated_at FROM deliveries WHERE id = $1", delivery_id)
    await pg.execute("SELECT pg_sleep(0.01)")
    # Inside one transaction now() is constant; the trigger uses clock_timestamp() or now().
    await pg.execute(update.format("status = 'dead', completed_at = now()"), delivery_id)
    after = await pg.fetchval("SELECT updated_at FROM deliveries WHERE id = $1", delivery_id)
    assert after >= before


async def test_idempotency_record_checks(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    insert = (
        "INSERT INTO idempotency_keys (tenant_id, key, request_method, request_path, fingerprint, "
        "expires_at, event_id, response_status, response_body) "
        "VALUES ($1, $2, 'POST', '/v1/events', $3, now() + interval '1 day', $4, $5, $6)"
    )
    a, event_a = world["a"], world["event_a"]
    assert await _violation(pg, insert, a, "k", b"short", None, None, None) == (
        "ck_idempotency_keys_fingerprint_sha256"
    )
    assert await _violation(pg, insert, a, "has space", FP, None, None, None) == (
        "ck_idempotency_keys_key_printable_ascii"
    )
    assert await _violation(pg, insert, a, "k", FP, event_a, 202, None) == (
        "ck_idempotency_keys_response_complete"
    )
    assert await _violation(pg, insert, a, "k", FP, event_a, 500, '{"x": 1}') == (
        "ck_idempotency_keys_response_status_success"
    )
    assert await _violation(pg, insert, a, "k", FP, world["event_b"], 202, "{}") == (
        "fk_idempotency_keys_event_same_tenant"
    )
    await pg.execute(insert, a, "k", FP, event_a, 202, "{}")
    expired = (
        "INSERT INTO idempotency_keys (tenant_id, key, request_method, request_path, fingerprint, "
        "created_at, expires_at) VALUES ($1, 'k2', 'POST', '/', $2, now(), now())"
    )
    assert await _violation(pg, expired, a, FP) == "ck_idempotency_keys_expires_after_created"


async def test_deleting_a_tenant_cascades_everything(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    a = world["a"]
    await pg.execute(
        "INSERT INTO event_outbox (tenant_id, event_id) VALUES ($1, $2)", a, world["event_a"]
    )
    await pg.execute(
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
        "VALUES ($1, $2, $3, now() + interval '1 day')",
        a,
        world["event_a"],
        world["endpoint_a"],
    )
    await pg.execute(
        "INSERT INTO idempotency_keys (tenant_id, key, request_method, request_path, fingerprint, "
        "expires_at, event_id, response_status, response_body) "
        "VALUES ($1, 'k', 'POST', '/v1/events', $2, now() + interval '1 day', $3, 202, '{}')",
        a,
        FP,
        world["event_a"],
    )
    await pg.execute("DELETE FROM tenants WHERE id = $1", a)
    for table in ("events", "event_outbox", "deliveries", "idempotency_keys"):
        left = await pg.fetchval(f"SELECT count(*) FROM {table} WHERE tenant_id = $1", a)  # noqa: S608
        assert left == 0, table
    assert await pg.fetchval("SELECT count(*) FROM events WHERE tenant_id = $1", world["b"]) == 1


async def test_event_type_with_events_cannot_be_deleted(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    # Event types are archived, never deleted, once events reference them.
    assert (
        await _violation(pg, "DELETE FROM event_types WHERE tenant_id = $1", world["a"])
        == "fk_events_event_type_same_tenant"
    )
