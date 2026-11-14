"""M3 lease columns and delivery_attempts enforce their invariants in Postgres."""

from __future__ import annotations

import uuid

import asyncpg
import pytest

from tests.integration.test_m1_schema import _endpoint, _event_type, _tenant, _violation
from tests.integration.test_m2_schema import _event

_ATTEMPT = (
    "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
    "status_code, error_class, request_url, worker_id, started_at, finished_at) "
    "VALUES ($1, $2, $3, $4, $5, $6, 'https://a.example.com/', 'w', now(), now())"
)


async def _delivery(pg: asyncpg.Connection, tenant: uuid.UUID) -> uuid.UUID:
    await _event_type(pg, tenant, "x.y")
    delivery_id: uuid.UUID = await pg.fetchval(
        "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until) "
        "VALUES ($1, $2, $3, now() + interval '1 day') RETURNING id",
        tenant,
        await _event(pg, tenant, "x.y"),
        await _endpoint(pg, tenant, f"https://{uuid.uuid4().hex[:8]}.example.com/"),
    )
    return delivery_id


@pytest.fixture
async def world(pg: asyncpg.Connection) -> dict[str, uuid.UUID]:
    a = await _tenant(pg, f"a-{uuid.uuid4().hex[:8]}")
    b = await _tenant(pg, f"b-{uuid.uuid4().hex[:8]}")
    return {"a": a, "b": b, "delivery_a": await _delivery(pg, a)}


async def test_lease_exists_exactly_while_delivering(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    update = "UPDATE deliveries SET {} WHERE id = $1"
    d = world["delivery_a"]
    lease = "lease_token = gen_random_uuid(), leased_until = now(), leased_by = 'w'"
    assert await _violation(pg, update.format("status = 'delivering'"), d) == (
        "ck_deliveries_lease_iff_delivering"
    )
    assert await _violation(pg, update.format(lease), d) == "ck_deliveries_lease_iff_delivering"
    partial = "status = 'delivering', lease_token = gen_random_uuid(), leased_by = 'w'"
    assert await _violation(pg, update.format(partial), d) == "ck_deliveries_lease_iff_delivering"
    await pg.execute(update.format(f"status = 'delivering', {lease}"), d)
    assert await _violation(pg, update.format("last_status_code = 600"), d) == (
        "ck_deliveries_last_status_code_range"
    )
    released = "status = 'failed', completed_at = now(), lease_token = NULL, "
    released += "leased_until = NULL, leased_by = NULL"
    await pg.execute(update.format(released), d)
    assert await pg.fetchval("SELECT status FROM deliveries WHERE id = $1", d) == "failed"
    assert await _violation(pg, update.format("completed_at = NULL"), d) == (
        "ck_deliveries_completed_iff_terminal"
    )


async def test_attempts_stay_inside_the_tenant_and_are_numbered_once(
    pg: asyncpg.Connection, world: dict[str, uuid.UUID]
) -> None:
    d = world["delivery_a"]
    assert await _violation(pg, _ATTEMPT, world["b"], d, 1, "failed", 500, "http_status") == (
        "fk_delivery_attempts_delivery_same_tenant"
    )
    await pg.execute(_ATTEMPT, world["a"], d, 1, "retrying", 503, "http_status")
    # M9c: delivery_attempts is partitioned, so Postgres names the partition's copy of
    # uq_delivery_attempts_number (delivery_id, attempt_number, created_at) in the error.
    violated = await _violation(pg, _ATTEMPT, world["a"], d, 1, "failed", 500, "http_status")
    assert violated.startswith("delivery_attempts_p")
    assert violated.endswith("_delivery_id_attempt_number_crea_key")
    await pg.execute("DELETE FROM deliveries WHERE id = $1", d)
    assert (
        await pg.fetchval("SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1", d) == 0
    )


@pytest.mark.parametrize(
    ("args", "constraint"),
    [
        ((0, "failed", 500, "http_status"), "ck_delivery_attempts_attempt_number_positive"),
        ((1, "lost", 500, "http_status"), "ck_delivery_attempts_outcome_valid"),
        ((1, "failed", 99, "http_status"), "ck_delivery_attempts_status_code_range"),
        ((1, "succeeded", 200, "http_status"), "ck_delivery_attempts_error_class_iff_unsuccessful"),
        ((1, "failed", 500, None), "ck_delivery_attempts_error_class_iff_unsuccessful"),
        ((1, "succeeded", 302, None), "ck_delivery_attempts_success_is_2xx"),
        ((1, "succeeded", None, None), "ck_delivery_attempts_success_is_2xx"),
    ],
)
async def test_attempt_checks(
    pg: asyncpg.Connection,
    world: dict[str, uuid.UUID],
    args: tuple[object, ...],
    constraint: str,
) -> None:
    assert await _violation(pg, _ATTEMPT, world["a"], world["delivery_a"], *args) == constraint


async def test_attempt_bounds(pg: asyncpg.Connection, world: dict[str, uuid.UUID]) -> None:
    base = (
        "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
        "error_class, request_url, worker_id, started_at, finished_at, {}) "
        "VALUES ($1, $2, 1, 'failed', 'timeout', 'u', 'w', now(), now(), $3)"
    )
    t, d = world["a"], world["delivery_a"]
    assert await _violation(pg, base.format("latency_ms"), t, d, -1) == (
        "ck_delivery_attempts_latency_non_negative"
    )
    assert await _violation(pg, base.format("response_body"), t, d, "x" * 65537) == (
        "ck_delivery_attempts_response_body_bounded"
    )
    assert await _violation(pg, base.format("error_message"), t, d, "x" * 1025) == (
        "ck_delivery_attempts_error_message_bounded"
    )
    assert await _violation(pg, base.format("request_headers"), t, d, "[]") == (
        "ck_delivery_attempts_request_headers_object"
    )
    backwards = (
        "INSERT INTO delivery_attempts (tenant_id, delivery_id, attempt_number, outcome, "
        "error_class, request_url, worker_id, started_at, finished_at) "
        "VALUES ($1, $2, 1, 'failed', 'timeout', 'u', 'w', now(), now() - interval '1 second')"
    )
    assert await _violation(pg, backwards, t, d) == "ck_delivery_attempts_finished_after_started"
