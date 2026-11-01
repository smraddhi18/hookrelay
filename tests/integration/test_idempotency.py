"""Idempotency-Key on POST /v1/events: replay, mismatch, concurrency, expiry and scoping."""

from __future__ import annotations

import asyncio
import hashlib
import uuid

import asyncpg
import httpx
import pytest

from tests.integration.conftest import (
    TenantCtx,
    _asyncpg_dsn,
    api_client,
    create_tenant,
    publish,
    register_event_type,
)


def _code(response: httpx.Response) -> str:
    return str(response.json()["error"]["code"])


@pytest.fixture
async def typed_tenant(client: httpx.AsyncClient, tenant: TenantCtx) -> TenantCtx:
    await register_event_type(client, tenant, "order.placed")
    return tenant


async def _scalar(url: str, sql: str, *args: object) -> object:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        return await conn.fetchval(sql, *args)
    finally:
        await conn.close()


async def test_same_key_same_body_replays_the_stored_response(
    client: httpx.AsyncClient, typed_tenant: TenantCtx, migrated_database_url: str
) -> None:
    first = await publish(client, typed_tenant, "order.placed", {"a": 1, "b": [1, 2]}, key="k-1")
    assert first.status_code == 202
    # Key order and whitespace do not change the canonical body.
    raw = b'{ "payload": {"b": [1,2], "a": 1},\n "event_type": "order.placed" }'
    again = await client.post(
        "/v1/events",
        content=raw,
        headers={
            **typed_tenant.headers,
            "Content-Type": "application/json",
            "Idempotency-Key": "k-1",
        },
    )
    assert again.status_code == 202
    assert again.headers["Idempotent-Replayed"] == "true"
    assert again.headers["Location"] == first.headers["Location"]
    assert again.json() == first.json()
    count = await _scalar(
        migrated_database_url,
        "SELECT count(*) FROM events WHERE tenant_id = $1",
        uuid.UUID(typed_tenant.id),
    )
    assert count == 1
    record = await _scalar(
        migrated_database_url,
        "SELECT event_id FROM idempotency_keys WHERE tenant_id = $1 AND key = 'k-1'",
        uuid.UUID(typed_tenant.id),
    )
    assert str(record) == first.json()["id"]


async def test_same_key_different_body_is_rejected(
    client: httpx.AsyncClient, typed_tenant: TenantCtx
) -> None:
    assert (
        await publish(client, typed_tenant, "order.placed", {"a": 1}, key="k-2")
    ).status_code == 202
    mismatch = await publish(client, typed_tenant, "order.placed", {"a": 2}, key="k-2")
    assert mismatch.status_code == 422
    assert _code(mismatch) == "idempotency_key_mismatch"
    assert "Idempotent-Replayed" not in mismatch.headers


async def test_keys_are_tenant_scoped(client: httpx.AsyncClient, typed_tenant: TenantCtx) -> None:
    mine = await publish(client, typed_tenant, "order.placed", key="shared-key")
    other = await create_tenant(client)
    await register_event_type(client, other, "order.placed")
    theirs = await publish(client, other, "order.placed", key="shared-key")
    assert theirs.status_code == 202
    assert "Idempotent-Replayed" not in theirs.headers
    assert theirs.json()["id"] != mine.json()["id"]


@pytest.mark.parametrize("key", ["has space", "x" * 256, "tab\tkey"])
async def test_invalid_keys_are_400(
    client: httpx.AsyncClient, typed_tenant: TenantCtx, key: str
) -> None:
    response = await client.post(
        "/v1/events",
        json={"event_type": "order.placed", "payload": {}},
        headers={**typed_tenant.headers, "Idempotency-Key": key},
    )
    assert response.status_code == 400
    assert _code(response) == "invalid_idempotency_key"


async def test_errors_are_not_cached(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    failed = await publish(client, tenant, "order.shipped", key="k-err")
    assert failed.status_code == 422
    assert _code(failed) == "unknown_event_type"
    await register_event_type(client, tenant, "order.shipped")
    retried = await publish(client, tenant, "order.shipped", key="k-err")
    assert retried.status_code == 202
    assert "Idempotent-Replayed" not in retried.headers


async def test_expired_keys_can_be_reused(
    client: httpx.AsyncClient, typed_tenant: TenantCtx, migrated_database_url: str
) -> None:
    first = await publish(client, typed_tenant, "order.placed", {"v": 1}, key="k-exp")
    await _scalar(
        migrated_database_url,
        "UPDATE idempotency_keys SET created_at = now() - interval '2 days', "
        "expires_at = now() - interval '1 second' WHERE tenant_id = $1 AND key = 'k-exp'",
        uuid.UUID(typed_tenant.id),
    )
    # Even a different body is fine: the old record is gone as far as the API is concerned.
    second = await publish(client, typed_tenant, "order.placed", {"v": 2}, key="k-exp")
    assert second.status_code == 202
    assert "Idempotent-Replayed" not in second.headers
    assert second.json()["id"] != first.json()["id"]
    replay = await publish(client, typed_tenant, "order.placed", {"v": 2}, key="k-exp")
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert replay.json()["id"] == second.json()["id"]


async def test_concurrent_requests_with_one_key_create_one_event(
    migrated_database_url: str,
) -> None:
    async with api_client(migrated_database_url, idempotency_lock_timeout_ms=10_000) as client:
        tenant = await create_tenant(client)
        await register_event_type(client, tenant, "order.placed")
        responses = await asyncio.gather(
            *(publish(client, tenant, "order.placed", {"same": True}, key="race") for _ in range(8))
        )
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["id"] for r in responses}) == 1
    replayed = [r for r in responses if r.headers.get("Idempotent-Replayed") == "true"]
    assert len(replayed) == 7
    count = await _scalar(
        migrated_database_url,
        "SELECT count(*) FROM events WHERE tenant_id = $1",
        uuid.UUID(tenant.id),
    )
    assert count == 1


async def test_in_flight_key_times_out_with_409_and_retry_after(migrated_database_url: str) -> None:
    async with api_client(migrated_database_url, idempotency_lock_timeout_ms=100) as client:
        tenant = await create_tenant(client)
        await register_event_type(client, tenant, "order.placed")
        holder = await asyncpg.connect(_asyncpg_dsn(migrated_database_url))
        try:
            tx = holder.transaction()
            await tx.start()
            # Simulates a request that has claimed the key and is still running.
            await holder.execute(
                "INSERT INTO idempotency_keys (tenant_id, key, request_method, request_path, "
                "fingerprint, expires_at) VALUES ($1, 'busy', 'POST', '/v1/events', $2, "
                "now() + interval '1 hour')",
                uuid.UUID(tenant.id),
                hashlib.sha256(b"x").digest(),
            )
            busy = await publish(client, tenant, "order.placed", key="busy")
            assert busy.status_code == 409
            assert _code(busy) == "idempotency_key_in_progress"
            assert busy.headers["Retry-After"] == "1"
            # Other keys are unaffected while that one is held.
            assert (await publish(client, tenant, "order.placed", key="free")).status_code == 202
            await tx.rollback()  # the in-flight request failed; its claim disappears
        finally:
            await holder.close()
        retried = await publish(client, tenant, "order.placed", key="busy")
        assert retried.status_code == 202
        assert "Idempotent-Replayed" not in retried.headers
