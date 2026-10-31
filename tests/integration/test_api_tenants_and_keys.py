"""Operator tenant API, API-key lifecycle and authentication against a real Postgres."""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import asyncpg
import httpx

from tests.integration.conftest import (
    OPERATOR,
    TenantCtx,
    _asyncpg_dsn,
    api_client,
    bearer,
    create_tenant,
    issue_key,
)


def _code(response: httpx.Response) -> str:
    return str(response.json()["error"]["code"])


async def _sql(url: str, sql: str, *args: object) -> list[asyncpg.Record]:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        return await conn.fetch(sql, *args)
    finally:
        await conn.close()


# --- operator API ---------------------------------------------------------------------


async def test_tenant_lifecycle(client: httpx.AsyncClient) -> None:
    slug = f"acme-{uuid.uuid4().hex[:8]}"
    created = await client.post(
        "/admin/tenants", json={"slug": slug, "name": "Acme"}, headers=OPERATOR
    )
    assert created.status_code == 201
    body = created.json()
    assert body["status"] == "active"
    assert created.headers["Location"] == f"/admin/tenants/{body['id']}"

    duplicate = await client.post(
        "/admin/tenants", json={"slug": slug, "name": "Other"}, headers=OPERATOR
    )
    assert duplicate.status_code == 409
    assert _code(duplicate) == "tenant_slug_taken"

    fetched = await client.get(f"/admin/tenants/{body['id']}", headers=OPERATOR)
    assert fetched.json()["slug"] == slug

    for _ in range(2):  # idempotent
        suspended = await client.post(f"/admin/tenants/{body['id']}/suspend", headers=OPERATOR)
        assert suspended.json()["status"] == "suspended"
    resumed = await client.post(f"/admin/tenants/{body['id']}/resume", headers=OPERATOR)
    assert resumed.json()["status"] == "active"


async def test_tenant_validation_and_not_found(client: httpx.AsyncClient) -> None:
    bad = await client.post(
        "/admin/tenants", json={"slug": "Not A Slug", "name": "x"}, headers=OPERATOR
    )
    assert bad.status_code == 422
    missing = uuid.uuid4()
    for response in (
        await client.get(f"/admin/tenants/{missing}", headers=OPERATOR),
        await client.post(f"/admin/tenants/{missing}/suspend", headers=OPERATOR),
        await client.post(
            f"/admin/tenants/{missing}/api-keys",
            json={"name": "k", "role": "admin"},
            headers=OPERATOR,
        ),
    ):
        assert response.status_code == 404
        assert _code(response) == "tenant_not_found"


async def test_list_tenants_paginates_and_filters(client: httpx.AsyncClient) -> None:
    tenants = [await create_tenant(client) for _ in range(3)]
    await client.post(f"/admin/tenants/{tenants[0].id}/suspend", headers=OPERATOR)

    seen: list[str] = []
    cursor: str | None = None
    while True:
        params: dict[str, str | int] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/admin/tenants", params=params, headers=OPERATOR)).json()
        assert len(page["data"]) <= 2
        seen.extend(t["id"] for t in page["data"])
        if not page["has_more"]:
            assert page["next_cursor"] is None
            break
        cursor = page["next_cursor"]
    assert len(seen) == len(set(seen))  # no duplicates across pages
    assert {t.id for t in tenants} <= set(seen)

    suspended = await client.get(
        "/admin/tenants", params={"status": "suspended", "limit": 100}, headers=OPERATOR
    )
    statuses = {t["status"] for t in suspended.json()["data"]}
    assert statuses == {"suspended"}

    bad_cursor = await client.get("/admin/tenants", params={"cursor": "%%"}, headers=OPERATOR)
    assert bad_cursor.status_code == 400
    assert _code(bad_cursor) == "invalid_cursor"


async def test_bootstrap_key_secret_is_shown_once_and_only_hashed_at_rest(
    client: httpx.AsyncClient, migrated_database_url: str
) -> None:
    tenant = await create_tenant(client)
    me = await client.get("/v1/me", headers=tenant.headers)
    assert me.status_code == 200
    assert me.json()["tenant"] == {"id": tenant.id, "slug": tenant.slug}
    assert me.json()["api_key"]["role"] == "admin"

    listed = (await client.get("/v1/api-keys", headers=tenant.headers)).json()["data"]
    assert all("key" not in k for k in listed)
    rows = await _sql(
        migrated_database_url,
        "SELECT prefix, key_hash FROM api_keys WHERE tenant_id = $1",
        uuid.UUID(tenant.id),
    )
    (row,) = rows
    assert tenant.admin_key.split("_")[1] == row["prefix"]
    assert len(row["key_hash"]) == 32
    assert tenant.admin_key.encode() not in bytes(row["key_hash"])


# --- authentication -------------------------------------------------------------------


async def test_wrong_secret_and_unknown_prefix_are_indistinguishable(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    prefix = tenant.admin_key.split("_")[1]
    wrong_secret = f"hrk_{prefix}_" + "A" * 43
    unknown_prefix = "hrk_zzzzzzzzzzzz_" + "A" * 43
    first = await client.get("/v1/me", headers=bearer(wrong_secret))
    second = await client.get("/v1/me", headers=bearer(unknown_prefix))
    assert first.status_code == second.status_code == 401
    assert first.json()["error"]["message"] == second.json()["error"]["message"]
    assert _code(first) == _code(second) == "unauthorized"
    assert first.headers["WWW-Authenticate"] == 'Bearer realm="hookrelay"'


async def test_operator_token_is_not_an_api_key(client: httpx.AsyncClient) -> None:
    response = await client.get("/v1/me", headers=OPERATOR)
    assert response.status_code == 401


async def test_api_key_cannot_call_the_operator_api(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    response = await client.get("/admin/tenants", headers=tenant.headers)
    assert response.status_code == 401


async def test_suspended_tenant_is_forbidden(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    await client.post(f"/admin/tenants/{tenant.id}/suspend", headers=OPERATOR)
    response = await client.get("/v1/me", headers=tenant.headers)
    assert response.status_code == 403
    assert _code(response) == "tenant_suspended"
    await client.post(f"/admin/tenants/{tenant.id}/resume", headers=OPERATOR)
    assert (await client.get("/v1/me", headers=tenant.headers)).status_code == 200


async def test_roles_are_enforced(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    reader = await issue_key(client, tenant, "reader")
    publisher = await issue_key(client, tenant, "publisher")

    assert (await client.get("/v1/api-keys", headers=bearer(reader))).status_code == 403
    denied = await client.post("/v1/event-types", json={"name": "a.b"}, headers=bearer(reader))
    assert denied.status_code == 403
    assert _code(denied) == "insufficient_role"
    assert denied.json()["error"]["details"] == {"required_role": "publisher", "role": "reader"}
    allowed = await client.post("/v1/event-types", json={"name": "a.b"}, headers=bearer(publisher))
    assert allowed.status_code == 201
    escalate = await client.post(
        "/v1/api-keys", json={"name": "x", "role": "admin"}, headers=bearer(publisher)
    )
    assert escalate.status_code == 403
    assert (await client.get("/v1/event-types", headers=bearer(reader))).status_code == 200


async def test_revoked_key_is_rejected_and_revoke_is_idempotent(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    key = await issue_key(client, tenant, "reader")
    key_id = (await client.get("/v1/me", headers=bearer(key))).json()["api_key"]["id"]
    first = await client.post(f"/v1/api-keys/{key_id}/revoke", headers=tenant.headers)
    second = await client.post(f"/v1/api-keys/{key_id}/revoke", headers=tenant.headers)
    assert first.status_code == second.status_code == 200
    assert first.json()["revoked_at"] == second.json()["revoked_at"]
    assert first.json()["status"] == "revoked"
    response = await client.get("/v1/me", headers=bearer(key))
    assert response.status_code == 401
    assert _code(response) == "api_key_revoked"


async def test_expired_key_is_rejected(
    client: httpx.AsyncClient, tenant: TenantCtx, migrated_database_url: str
) -> None:
    soon = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    key = await issue_key(client, tenant, "reader", expires_at=soon)
    assert (await client.get("/v1/me", headers=bearer(key))).status_code == 200
    await _sql(
        migrated_database_url,
        "UPDATE api_keys SET created_at = now() - interval '2 hours', "
        "expires_at = now() - interval '1 second' WHERE prefix = $1",
        key.split("_")[1],
    )
    response = await client.get("/v1/me", headers=bearer(key))
    assert response.status_code == 401
    assert _code(response) == "api_key_expired"


async def test_expiry_must_be_in_the_future(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    response = await client.post(
        "/v1/api-keys",
        json={"name": "x", "role": "reader", "expires_at": past},
        headers=tenant.headers,
    )
    assert response.status_code == 422


async def test_last_used_at_is_throttled(
    migrated_database_url: str,
) -> None:
    async with api_client(migrated_database_url, api_key_last_used_resolution_s=3600) as client:
        tenant = await create_tenant(client)
        prefix = tenant.admin_key.split("_")[1]

        async def last_used() -> datetime | None:
            rows = await _sql(
                migrated_database_url,
                "SELECT last_used_at FROM api_keys WHERE prefix = $1",
                prefix,
            )
            value: datetime | None = rows[0]["last_used_at"]
            return value

        assert await last_used() is None
        await client.get("/v1/me", headers=tenant.headers)
        first = await last_used()
        assert first is not None
        await client.get("/v1/me", headers=tenant.headers)
        assert await last_used() == first  # within the resolution window: no write

    async with api_client(migrated_database_url, api_key_last_used_resolution_s=0) as client:
        await asyncio.sleep(0.01)
        await client.get("/v1/me", headers=tenant.headers)
        later = await last_used()
        assert later is not None
        assert later > first


async def test_last_used_survives_a_failing_request(
    client: httpx.AsyncClient, tenant: TenantCtx, migrated_database_url: str
) -> None:
    response = await client.get(f"/v1/endpoints/{uuid.uuid4()}", headers=tenant.headers)
    assert response.status_code == 404
    rows = await _sql(
        migrated_database_url,
        "SELECT last_used_at FROM api_keys WHERE prefix = $1",
        tenant.admin_key.split("_")[1],
    )
    assert rows[0]["last_used_at"] is not None


# --- rotation -------------------------------------------------------------------------


async def test_rotation_overlap_window(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    old_key = await issue_key(client, tenant, "publisher")
    old_id = (await client.get("/v1/me", headers=bearer(old_key))).json()["api_key"]["id"]

    rotated = await client.post(f"/v1/api-keys/{old_id}/rotate", headers=tenant.headers)
    assert rotated.status_code == 201
    assert rotated.headers["Cache-Control"] == "no-store"
    body = rotated.json()
    new_key = body["new_key"]["key"]
    assert body["new_key"]["rotated_from_id"] == old_id
    assert body["new_key"]["role"] == "publisher"
    previous_expiry = datetime.fromisoformat(body["previous_key"]["expires_at"])
    remaining = previous_expiry - datetime.now(UTC)
    assert timedelta(hours=23) < remaining <= timedelta(hours=24, minutes=1)

    # Both keys work during the overlap window.
    assert (await client.get("/v1/me", headers=bearer(old_key))).status_code == 200
    assert (await client.get("/v1/me", headers=bearer(new_key))).status_code == 200

    again = await client.post(f"/v1/api-keys/{old_id}/rotate", headers=tenant.headers)
    assert again.status_code == 409
    assert _code(again) == "api_key_already_rotated"


async def test_rotation_with_zero_overlap_expires_the_old_key(migrated_database_url: str) -> None:
    async with api_client(migrated_database_url, api_key_rotation_overlap_s=0) as client:
        tenant = await create_tenant(client)
        old_key = await issue_key(client, tenant, "reader")
        old_id = (await client.get("/v1/me", headers=bearer(old_key))).json()["api_key"]["id"]
        rotated = await client.post(f"/v1/api-keys/{old_id}/rotate", headers=tenant.headers)
        assert rotated.status_code == 201
        response = await client.get("/v1/me", headers=bearer(old_key))
        assert response.status_code == 401
        assert _code(response) == "api_key_expired"
        new_key = rotated.json()["new_key"]["key"]
        assert (await client.get("/v1/me", headers=bearer(new_key))).status_code == 200


async def test_concurrent_rotations_mint_one_successor(
    migrated_database_url: str,
) -> None:
    async with api_client(migrated_database_url, db_pool_size=10) as client:
        tenant = await create_tenant(client)
        key = await issue_key(client, tenant, "reader")
        key_id = (await client.get("/v1/me", headers=bearer(key))).json()["api_key"]["id"]
        responses = await asyncio.gather(
            *(
                client.post(f"/v1/api-keys/{key_id}/rotate", headers=tenant.headers)
                for _ in range(5)
            )
        )
        codes = sorted(r.status_code for r in responses)
        assert codes == [201, 409, 409, 409, 409]


async def test_revoked_key_cannot_be_rotated(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    key = await issue_key(client, tenant, "reader")
    key_id = (await client.get("/v1/me", headers=bearer(key))).json()["api_key"]["id"]
    await client.post(f"/v1/api-keys/{key_id}/revoke", headers=tenant.headers)
    response = await client.post(f"/v1/api-keys/{key_id}/rotate", headers=tenant.headers)
    assert response.status_code == 409
    assert _code(response) == "api_key_inactive"


async def test_keys_are_tenant_scoped(client: httpx.AsyncClient) -> None:
    a = await create_tenant(client)
    b = await create_tenant(client)
    a_key_id = (await client.get("/v1/me", headers=a.headers)).json()["api_key"]["id"]
    for response in (
        await client.get(f"/v1/api-keys/{a_key_id}", headers=b.headers),
        await client.post(f"/v1/api-keys/{a_key_id}/revoke", headers=b.headers),
        await client.post(f"/v1/api-keys/{a_key_id}/rotate", headers=b.headers),
    ):
        assert response.status_code == 404
        assert _code(response) == "api_key_not_found"
    b_ids = {k["id"] for k in (await client.get("/v1/api-keys", headers=b.headers)).json()["data"]}
    assert a_key_id not in b_ids
    assert (await client.get("/v1/me", headers=a.headers)).status_code == 200
