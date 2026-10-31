"""Endpoint CRUD, signing secrets, validation and optimistic concurrency."""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import httpx
import pytest

from tests.integration.conftest import (
    TenantCtx,
    _asyncpg_dsn,
    api_client,
    bearer,
    create_tenant,
    issue_key,
)

URL = "https://hooks.example.com/hookrelay"


def _code(response: httpx.Response) -> str:
    return str(response.json()["error"]["code"])


async def _create(
    client: httpx.AsyncClient, tenant: TenantCtx, url: str = URL, **extra: object
) -> dict[str, object]:
    response = await client.post(
        "/v1/endpoints", json={"url": url, **extra}, headers=tenant.headers
    )
    assert response.status_code == 201, response.text
    body: dict[str, object] = response.json()
    return body


async def test_create_returns_secret_once_and_stores_it_encrypted(
    client: httpx.AsyncClient, tenant: TenantCtx, migrated_database_url: str
) -> None:
    response = await client.post(
        "/v1/endpoints",
        json={
            "url": "HTTPS://Hooks.Example.com/in",
            "description": "billing",
            "headers": {"X-Receiver-Token": "s3cr3t"},
        },
        headers=tenant.headers,
    )
    assert response.status_code == 201
    assert response.headers["ETag"] == '"1"'
    assert response.headers["Cache-Control"] == "no-store"
    body = response.json()
    assert body["url"] == "https://hooks.example.com/in"
    assert body["timeout_ms"] == 10_000
    assert body["enabled"] is True
    assert body["headers"] == {"X-Receiver-Token": "s3cr3t"}
    secret = body["signing_secret"]
    assert secret.startswith("whsec_")
    assert response.headers["Location"] == f"/v1/endpoints/{body['id']}"

    fetched = await client.get(f"/v1/endpoints/{body['id']}", headers=tenant.headers)
    assert "signing_secret" not in fetched.json()
    assert fetched.headers["ETag"] == '"1"'

    revealed = await client.get(f"/v1/endpoints/{body['id']}/secret", headers=tenant.headers)
    assert revealed.status_code == 200
    assert revealed.json() == {"endpoint_id": body["id"], "signing_secret": secret}
    assert revealed.headers["Cache-Control"] == "no-store"

    conn = await asyncpg.connect(_asyncpg_dsn(migrated_database_url))
    try:
        stored = await conn.fetchval(
            "SELECT secret_ciphertext FROM endpoints WHERE id = $1", uuid.UUID(body["id"])
        )
    finally:
        await conn.close()
    assert stored.startswith("v1.")
    assert secret not in stored
    assert secret.removeprefix("whsec_") not in stored


async def test_secret_is_admin_only_and_headers_are_masked_for_non_admins(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    created = await _create(client, tenant, headers={"X-Token": "abc"})
    reader = bearer(await issue_key(client, tenant, "reader"))
    publisher = bearer(await issue_key(client, tenant, "publisher"))
    for headers in (reader, publisher):
        secret = await client.get(f"/v1/endpoints/{created['id']}/secret", headers=headers)
        assert secret.status_code == 403
        got = await client.get(f"/v1/endpoints/{created['id']}", headers=headers)
        assert got.json()["headers"] == {"X-Token": "********"}
        listed = await client.get("/v1/endpoints", headers=headers)
        assert listed.json()["data"][0]["headers"] == {"X-Token": "********"}
        denied = await client.post("/v1/endpoints", json={"url": URL + "/x"}, headers=headers)
        assert denied.status_code == 403


async def test_secret_unreadable_with_a_different_key(
    client: httpx.AsyncClient, tenant: TenantCtx, migrated_database_url: str
) -> None:
    created = await _create(client, tenant)
    other_key = "b3RoZXIta2V5LW90aGVyLWtleS1vdGhlci1rZXktMzI"
    async with api_client(migrated_database_url, secrets_encryption_key=other_key) as other:
        response = await other.get(f"/v1/endpoints/{created['id']}/secret", headers=tenant.headers)
    assert response.status_code == 500
    assert _code(response) == "secret_unavailable"
    assert "whsec_" not in response.text


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        ({"url": "http://hooks.example.com/"}, "invalid_endpoint_url"),
        ({"url": "https://127.0.0.1/"}, "invalid_endpoint_url"),
        ({"url": "https://169.254.169.254/latest"}, "invalid_endpoint_url"),
        ({"url": "https://user:pw@hooks.example.com/"}, "invalid_endpoint_url"),
        ({"url": URL, "timeout_ms": 45_000}, "timeout_too_large"),
        ({"url": URL, "timeout_ms": 10}, "validation_error"),
        ({"url": URL, "headers": {"Host": "evil"}}, "validation_error"),
        ({"url": URL, "headers": {"X-A": "a\r\nb: c"}}, "validation_error"),
        ({"url": URL, "unknown": 1}, "validation_error"),
    ],
)
async def test_create_validation(
    client: httpx.AsyncClient, tenant: TenantCtx, payload: dict[str, object], code: str
) -> None:
    response = await client.post("/v1/endpoints", json=payload, headers=tenant.headers)
    assert response.status_code == 422, response.text
    assert _code(response) == code


async def test_dev_policy_allows_local_http_targets(migrated_database_url: str) -> None:
    async with api_client(
        migrated_database_url, endpoint_allow_http=True, endpoint_allow_private_networks=True
    ) as client:
        tenant = await create_tenant(client)
        created = await _create(client, tenant, url="http://localhost:9000/in")
        assert created["url"] == "http://localhost:9000/in"


async def test_duplicate_live_url_conflicts_but_deleted_url_can_be_reused(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    first = await _create(client, tenant)
    dup = await client.post("/v1/endpoints", json={"url": URL}, headers=tenant.headers)
    assert dup.status_code == 409
    assert _code(dup) == "endpoint_url_taken"

    other_tenant = await create_tenant(client)
    assert (await _create(client, other_tenant))["url"] == URL  # unique per tenant only

    deleted = await client.delete(f"/v1/endpoints/{first['id']}", headers=tenant.headers)
    assert deleted.status_code == 204
    gone = await client.get(f"/v1/endpoints/{first['id']}", headers=tenant.headers)
    assert gone.status_code == 404
    again = await client.delete(f"/v1/endpoints/{first['id']}", headers=tenant.headers)
    assert again.status_code == 404
    assert (await _create(client, tenant))["id"] != first["id"]


async def test_update_requires_if_match_and_detects_lost_updates(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    created = await _create(client, tenant)
    path = f"/v1/endpoints/{created['id']}"

    missing = await client.patch(path, json={"enabled": False}, headers=tenant.headers)
    assert missing.status_code == 428
    assert _code(missing) == "precondition_required"

    weak = await client.patch(
        path, json={"enabled": False}, headers={**tenant.headers, "If-Match": 'W/"1"'}
    )
    assert weak.status_code == 412

    ok = await client.patch(
        path,
        json={"enabled": False, "timeout_ms": 5000},
        headers={**tenant.headers, "If-Match": '"1"'},
    )
    assert ok.status_code == 200
    assert ok.headers["ETag"] == '"2"'
    assert ok.json()["enabled"] is False
    assert ok.json()["timeout_ms"] == 5000
    assert ok.json()["version"] == 2

    stale = await client.patch(
        path, json={"description": "late"}, headers={**tenant.headers, "If-Match": '"1"'}
    )
    assert stale.status_code == 412
    assert _code(stale) == "precondition_failed"
    assert stale.json()["error"]["details"]["current_version"] == 2
    assert stale.headers["ETag"] == '"2"'

    forced = await client.patch(
        path, json={"description": "forced"}, headers={**tenant.headers, "If-Match": "*"}
    )
    assert forced.status_code == 200
    assert forced.json()["version"] == 3

    cleared = await client.patch(
        path,
        json={"description": None, "headers": {"X-New": "1"}},
        headers={**tenant.headers, "If-Match": '"3"'},
    )
    assert cleared.json()["description"] is None
    assert cleared.json()["headers"] == {"X-New": "1"}


async def test_update_validation(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    created = await _create(client, tenant)
    other = await _create(client, tenant, url=URL + "/other")
    path = f"/v1/endpoints/{created['id']}"
    match = {**tenant.headers, "If-Match": "*"}
    empty = await client.patch(path, json={}, headers=match)
    assert empty.status_code == 422
    bad_url = await client.patch(path, json={"url": "https://10.0.0.1/"}, headers=match)
    assert _code(bad_url) == "invalid_endpoint_url"
    taken = await client.patch(path, json={"url": other["url"]}, headers=match)
    assert taken.status_code == 409
    assert _code(taken) == "endpoint_url_taken"
    null_url = await client.patch(path, json={"url": None}, headers=match)
    assert null_url.status_code == 422
    too_slow = await client.patch(path, json={"timeout_ms": 59_000}, headers=match)
    assert _code(too_slow) == "timeout_too_large"
    unchanged = await client.get(path, headers=tenant.headers)
    assert unchanged.json()["version"] == 1


async def test_concurrent_updates_with_the_same_etag_let_exactly_one_win(
    migrated_database_url: str,
) -> None:
    async with api_client(migrated_database_url, db_pool_size=10) as client:
        tenant = await create_tenant(client)
        created = await _create(client, tenant)
        responses = await asyncio.gather(
            *(
                client.patch(
                    f"/v1/endpoints/{created['id']}",
                    json={"description": f"writer-{i}"},
                    headers={**tenant.headers, "If-Match": '"1"'},
                )
                for i in range(5)
            )
        )
    assert sorted(r.status_code for r in responses) == [200, 412, 412, 412, 412]


async def test_delete_with_if_match(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    created = await _create(client, tenant)
    path = f"/v1/endpoints/{created['id']}"
    stale = await client.delete(path, headers={**tenant.headers, "If-Match": '"7"'})
    assert stale.status_code == 412
    ok = await client.delete(path, headers={**tenant.headers, "If-Match": '"1"'})
    assert ok.status_code == 204


async def test_endpoints_are_tenant_scoped(client: httpx.AsyncClient) -> None:
    a = await create_tenant(client)
    b = await create_tenant(client)
    endpoint = await _create(client, a)
    path = f"/v1/endpoints/{endpoint['id']}"
    for response in (
        await client.get(path, headers=b.headers),
        await client.get(f"{path}/secret", headers=b.headers),
        await client.patch(path, json={"enabled": False}, headers={**b.headers, "If-Match": "*"}),
        await client.delete(path, headers=b.headers),
    ):
        assert response.status_code == 404
        assert _code(response) == "endpoint_not_found"
    assert (await client.get("/v1/endpoints", headers=b.headers)).json()["data"] == []
    assert (await client.get(path, headers=a.headers)).status_code == 200


async def test_list_paginates_newest_first_and_filters(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    ids = [str((await _create(client, tenant, url=f"{URL}/{i}"))["id"]) for i in range(5)]
    await client.patch(
        f"/v1/endpoints/{ids[0]}",
        json={"enabled": False},
        headers={**tenant.headers, "If-Match": "*"},
    )
    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, str | int] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/v1/endpoints", params=params, headers=tenant.headers)).json()
        pages += 1
        seen.extend(e["id"] for e in page["data"])
        if not page["has_more"]:
            break
        cursor = page["next_cursor"]
    assert pages == 3
    assert seen == list(reversed(ids))

    disabled = await client.get(
        "/v1/endpoints", params={"enabled": "false"}, headers=tenant.headers
    )
    assert [e["id"] for e in disabled.json()["data"]] == [ids[0]]

    too_big = await client.get("/v1/endpoints", params={"limit": 101}, headers=tenant.headers)
    assert too_big.status_code == 422
