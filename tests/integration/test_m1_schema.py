"""M1 tables enforce tenancy and invariants in Postgres, independent of the service layer."""

from __future__ import annotations

import hashlib
import uuid

import asyncpg
import pytest

SECRET = "v1.k1.ciphertext"


async def _tenant(pg: asyncpg.Connection, slug: str) -> uuid.UUID:
    tenant_id: uuid.UUID = await pg.fetchval(
        "INSERT INTO tenants (slug, name) VALUES ($1, $1) RETURNING id", slug
    )
    return tenant_id


async def _endpoint(pg: asyncpg.Connection, tenant_id: uuid.UUID, url: str) -> uuid.UUID:
    endpoint_id: uuid.UUID = await pg.fetchval(
        "INSERT INTO endpoints (tenant_id, url, secret_ciphertext) "
        "VALUES ($1, $2, $3) RETURNING id",
        tenant_id,
        url,
        SECRET,
    )
    return endpoint_id


async def _event_type(pg: asyncpg.Connection, tenant_id: uuid.UUID, name: str) -> uuid.UUID:
    event_type_id: uuid.UUID = await pg.fetchval(
        "INSERT INTO event_types (tenant_id, name) VALUES ($1, $2) RETURNING id", tenant_id, name
    )
    return event_type_id


async def _key(
    pg: asyncpg.Connection, tenant_id: uuid.UUID, prefix: str, **extra: object
) -> uuid.UUID:
    row: dict[str, object] = {
        "tenant_id": tenant_id,
        "name": "k",
        "role": "admin",
        "prefix": prefix,
        "key_hash": hashlib.sha256(prefix.encode()).digest(),
        **extra,
    }
    cols, values = list(row), list(row.values())
    params = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    key_id: uuid.UUID = await pg.fetchval(
        f"INSERT INTO api_keys ({', '.join(cols)}) VALUES ({params}) RETURNING id",  # noqa: S608
        *values,
    )
    return key_id


async def _violation(pg: asyncpg.Connection, sql: str, *args: object) -> str:
    # The savepoint rolls back on the error, so the outer test transaction survives.
    with pytest.raises(asyncpg.IntegrityConstraintViolationError) as info:
        async with pg.transaction():
            await pg.execute(sql, *args)
    return str(info.value.constraint_name)


async def test_subscription_cannot_point_at_another_tenants_rows(pg: asyncpg.Connection) -> None:
    a = await _tenant(pg, "tenant-a")
    b = await _tenant(pg, "tenant-b")
    endpoint_b = await _endpoint(pg, b, "https://b.example.com/")
    endpoint_a = await _endpoint(pg, a, "https://a.example.com/")
    type_b = await _event_type(pg, b, "invoice.paid")

    insert = (
        "INSERT INTO subscriptions (tenant_id, endpoint_id, event_type_pattern, event_type_id) "
        "VALUES ($1, $2, $3, $4)"
    )
    assert (
        await _violation(pg, insert, a, endpoint_b, "*", None)
        == "fk_subscriptions_endpoint_same_tenant"
    )
    assert (
        await _violation(pg, insert, a, endpoint_a, "invoice.paid", type_b)
        == "fk_subscriptions_event_type_same_tenant"
    )


async def test_subscription_pattern_checks(pg: asyncpg.Connection) -> None:
    t = await _tenant(pg, "subs-checks")
    endpoint = await _endpoint(pg, t, "https://s.example.com/")
    event_type = await _event_type(pg, t, "order.created")
    insert = (
        "INSERT INTO subscriptions (tenant_id, endpoint_id, event_type_pattern, event_type_id) "
        "VALUES ($1, $2, $3, $4)"
    )
    # Exact patterns need their event type; wildcards must not carry one.
    assert (
        await _violation(pg, insert, t, endpoint, "order.created", None)
        == "ck_subscriptions_exact_pattern_has_event_type"
    )
    assert (
        await _violation(pg, insert, t, endpoint, "order.*", event_type)
        == "ck_subscriptions_exact_pattern_has_event_type"
    )
    assert (
        await _violation(pg, insert, t, endpoint, "order.*.x", event_type)
        == "ck_subscriptions_pattern_format"
    )
    await pg.execute(insert, t, endpoint, "order.created", event_type)
    assert (
        await _violation(pg, insert, t, endpoint, "order.created", event_type)
        == "uq_subscriptions_endpoint_pattern"
    )
    # Referenced event types cannot be hard-deleted out from under a subscription.
    assert (
        await _violation(pg, "DELETE FROM event_types WHERE id = $1", event_type)
        == "fk_subscriptions_event_type_same_tenant"
    )
    # Hard-deleting the endpoint cascades to its subscriptions.
    await pg.execute("DELETE FROM endpoints WHERE id = $1", endpoint)
    assert await pg.fetchval("SELECT count(*) FROM subscriptions WHERE tenant_id = $1", t) == 0


async def test_endpoint_checks(pg: asyncpg.Connection) -> None:
    t = await _tenant(pg, "endpoint-checks")
    base = (
        "INSERT INTO endpoints (tenant_id, url, secret_ciphertext{cols}) VALUES ($1, $2, $3{vals})"
    )
    plain = base.format(cols="", vals="")
    assert (
        await _violation(pg, plain, t, "ftp://x.example.com/", SECRET) == "ck_endpoints_url_format"
    )
    assert (
        await _violation(pg, plain, t, "https://x.example.com/", "whsec_plaintext")
        == "ck_endpoints_secret_encrypted"
    )
    one = base.format(cols=", {}", vals=", $4")
    for col, value, name in [
        ("timeout_ms", 50, "ck_endpoints_timeout_range"),
        ("timeout_ms", 60_001, "ck_endpoints_timeout_range"),
        ("headers", "[]", "ck_endpoints_headers_object"),
        ("version", 0, "ck_endpoints_version_positive"),
    ]:
        sql = one.format(col)
        if col == "headers":
            sql = sql.replace("$4", "$4::jsonb")
        assert await _violation(pg, sql, t, "https://x.example.com/", SECRET, value) == name

    live = await _endpoint(pg, t, "https://dup.example.com/")
    assert await _violation(pg, plain, t, "https://dup.example.com/", SECRET) == (
        "uq_endpoints_tenant_url_live"
    )
    assert (
        await _violation(pg, "UPDATE endpoints SET deleted_at = now() WHERE id = $1", live)
        == "ck_endpoints_deleted_is_disabled"
    )
    await pg.execute("UPDATE endpoints SET deleted_at = now(), enabled = false WHERE id = $1", live)
    await _endpoint(pg, t, "https://dup.example.com/")  # partial unique index frees the URL


async def test_api_key_checks(pg: asyncpg.Connection) -> None:
    t = await _tenant(pg, "key-checks")
    first = await _key(pg, t, "aaaaaaaaaaaa")
    await _key(pg, t, "bbbbbbbbbbbb", rotated_from_id=first)

    async def bad(prefix: str, **extra: object) -> str:
        with pytest.raises(asyncpg.IntegrityConstraintViolationError) as info:
            async with pg.transaction():
                await _key(pg, t, prefix, **extra)
        return str(info.value.constraint_name)

    assert await bad("cccccccccccc", rotated_from_id=first) == "uq_api_keys_rotated_from_id"
    assert await bad("aaaaaaaaaaaa") == "uq_api_keys_prefix"
    assert await bad("SHORT") == "ck_api_keys_prefix_format"
    assert await bad("dddddddddddd", role="owner") == "ck_api_keys_role_valid"
    assert await bad("eeeeeeeeeeee", name="   ") == "ck_api_keys_name_length"
    assert await bad("ffffffffffff", key_hash=b"short") == "ck_api_keys_key_hash_sha256"
    assert (
        await bad("gggggggggggg", expires_at=await pg.fetchval("SELECT now() - interval '1 day'"))
        == "ck_api_keys_expiry_after_creation"
    )
    assert (
        await _violation(pg, "UPDATE api_keys SET rotated_from_id = id WHERE id = $1", first)
        == "ck_api_keys_not_self_rotated"
    )


async def test_event_type_checks_and_tenant_cascade(pg: asyncpg.Connection) -> None:
    t = await _tenant(pg, "cascade")
    await _event_type(pg, t, "a.b")
    insert = "INSERT INTO event_types (tenant_id, name) VALUES ($1, $2)"
    assert await _violation(pg, insert, t, "a.b") == "uq_event_types_tenant_id_name"
    assert await _violation(pg, insert, t, "A B") == "ck_event_types_name_format"
    await _key(pg, t, "hhhhhhhhhhhh")
    endpoint = await _endpoint(pg, t, "https://c.example.com/")
    await pg.execute(
        "INSERT INTO subscriptions (tenant_id, endpoint_id, event_type_pattern) "
        "VALUES ($1, $2, '*')",
        t,
        endpoint,
    )
    await pg.execute("DELETE FROM tenants WHERE id = $1", t)
    for table in ("api_keys", "endpoints", "event_types", "subscriptions"):
        count = await pg.fetchval(f"SELECT count(*) FROM {table} WHERE tenant_id = $1", t)  # noqa: S608
        assert count == 0, table
