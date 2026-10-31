"""Event-type registry, subscriptions (exact + wildcard) and fan-out matching."""

from __future__ import annotations

import itertools

import httpx

from hookrelay.services.subscriptions import pattern_matches
from tests.integration.conftest import TenantCtx, bearer, create_tenant, issue_key


def _code(response: httpx.Response) -> str:
    return str(response.json()["error"]["code"])


async def _event_type(client: httpx.AsyncClient, tenant: TenantCtx, name: str) -> dict[str, object]:
    response = await client.post(
        "/v1/event-types",
        json={"name": name, "description": f"{name} happened"},
        headers=tenant.headers,
    )
    assert response.status_code == 201, response.text
    body: dict[str, object] = response.json()
    return body


async def _endpoint(client: httpx.AsyncClient, tenant: TenantCtx, suffix: str) -> str:
    response = await client.post(
        "/v1/endpoints",
        json={"url": f"https://hooks.example.com/{suffix}"},
        headers=tenant.headers,
    )
    assert response.status_code == 201, response.text
    return str(response.json()["id"])


async def _subscribe(
    client: httpx.AsyncClient, tenant: TenantCtx, endpoint_id: str, pattern: str
) -> httpx.Response:
    return await client.post(
        f"/v1/endpoints/{endpoint_id}/subscriptions",
        json={"event_type": pattern},
        headers=tenant.headers,
    )


async def test_event_type_registry(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    created = await _event_type(client, tenant, "invoice.paid")
    assert created["archived"] is False
    assert created["version"] == 1
    dup = await client.post(
        "/v1/event-types", json={"name": "invoice.paid"}, headers=tenant.headers
    )
    assert dup.status_code == 409
    assert _code(dup) == "event_type_exists"
    bad = await client.post(
        "/v1/event-types", json={"name": "Invoice Paid"}, headers=tenant.headers
    )
    assert bad.status_code == 422

    fetched = await client.get("/v1/event-types/invoice.paid", headers=tenant.headers)
    assert fetched.headers["ETag"] == '"1"'
    missing = await client.get("/v1/event-types/nope", headers=tenant.headers)
    assert missing.status_code == 404
    assert _code(missing) == "event_type_not_found"

    # Same name in another tenant is independent.
    other = await create_tenant(client)
    await _event_type(client, other, "invoice.paid")


async def test_archive_and_unarchive(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    await _event_type(client, tenant, "user.created")
    path = "/v1/event-types/user.created"
    no_etag = await client.patch(path, json={"archived": True}, headers=tenant.headers)
    assert no_etag.status_code == 428

    archived = await client.patch(
        path, json={"archived": True}, headers={**tenant.headers, "If-Match": '"1"'}
    )
    assert archived.status_code == 200
    first_archived_at = archived.json()["archived_at"]
    assert archived.json()["archived"] is True
    assert archived.headers["ETag"] == '"2"'

    again = await client.patch(
        path, json={"archived": True}, headers={**tenant.headers, "If-Match": "*"}
    )
    assert again.json()["archived_at"] == first_archived_at  # original timestamp kept

    stale = await client.patch(
        path, json={"description": "x"}, headers={**tenant.headers, "If-Match": '"1"'}
    )
    assert stale.status_code == 412
    assert stale.json()["error"]["details"]["current_version"] == 3

    endpoint_id = await _endpoint(client, tenant, "archived")
    refused = await _subscribe(client, tenant, endpoint_id, "user.created")
    assert refused.status_code == 422
    assert _code(refused) == "unknown_event_type"

    listed = await client.get(
        "/v1/event-types", params={"include_archived": "false"}, headers=tenant.headers
    )
    assert "user.created" not in {e["name"] for e in listed.json()["data"]}
    all_types = await client.get(
        "/v1/event-types", params={"include_archived": "true"}, headers=tenant.headers
    )
    assert "user.created" in {e["name"] for e in all_types.json()["data"]}

    restored = await client.patch(
        path, json={"archived": False}, headers={**tenant.headers, "If-Match": "*"}
    )
    assert restored.json()["archived_at"] is None
    assert (await _subscribe(client, tenant, endpoint_id, "user.created")).status_code == 201


async def test_subscription_lifecycle(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    await _event_type(client, tenant, "invoice.paid")
    endpoint_id = await _endpoint(client, tenant, "subs")

    exact = await _subscribe(client, tenant, endpoint_id, "invoice.paid")
    assert exact.status_code == 201
    assert exact.json()["wildcard"] is False
    wildcard = await _subscribe(client, tenant, endpoint_id, "invoice.*")
    assert wildcard.status_code == 201
    assert wildcard.json()["wildcard"] is True
    # Wildcards may precede the types they will match.
    assert (await _subscribe(client, tenant, endpoint_id, "shipment.*")).status_code == 201

    dup = await _subscribe(client, tenant, endpoint_id, "invoice.paid")
    assert dup.status_code == 409
    assert _code(dup) == "subscription_exists"
    unknown = await _subscribe(client, tenant, endpoint_id, "invoice.refunded")
    assert unknown.status_code == 422
    assert _code(unknown) == "unknown_event_type"
    bad_pattern = await _subscribe(client, tenant, endpoint_id, "invoice.*.paid")
    assert bad_pattern.status_code == 422

    listed = await client.get(
        f"/v1/endpoints/{endpoint_id}/subscriptions", params={"limit": 2}, headers=tenant.headers
    )
    assert listed.json()["has_more"] is True
    rest = await client.get(
        f"/v1/endpoints/{endpoint_id}/subscriptions",
        params={"cursor": listed.json()["next_cursor"]},
        headers=tenant.headers,
    )
    patterns = {s["event_type"] for s in listed.json()["data"] + rest.json()["data"]}
    assert patterns == {"invoice.paid", "invoice.*", "shipment.*"}

    sub_id = exact.json()["id"]
    path = f"/v1/endpoints/{endpoint_id}/subscriptions/{sub_id}"
    assert (await client.delete(path, headers=tenant.headers)).status_code == 204
    gone = await client.delete(path, headers=tenant.headers)
    assert gone.status_code == 404
    assert _code(gone) == "subscription_not_found"


async def test_subscription_roles_and_tenant_scope(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    endpoint_id = await _endpoint(client, tenant, "scope")
    reader = bearer(await issue_key(client, tenant, "reader"))
    denied = await client.post(
        f"/v1/endpoints/{endpoint_id}/subscriptions", json={"event_type": "*"}, headers=reader
    )
    assert denied.status_code == 403
    ok = await client.get(f"/v1/endpoints/{endpoint_id}/subscriptions", headers=reader)
    assert ok.status_code == 200

    other = await create_tenant(client)
    foreign = await client.post(
        f"/v1/endpoints/{endpoint_id}/subscriptions",
        json={"event_type": "*"},
        headers=other.headers,
    )
    assert foreign.status_code == 404
    assert _code(foreign) == "endpoint_not_found"
    sub = await _subscribe(client, tenant, endpoint_id, "*")
    foreign_delete = await client.delete(
        f"/v1/endpoints/{endpoint_id}/subscriptions/{sub.json()['id']}", headers=other.headers
    )
    assert foreign_delete.status_code == 404


async def test_deleting_an_endpoint_removes_its_subscriptions(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    endpoint_id = await _endpoint(client, tenant, "cascade")
    await _subscribe(client, tenant, endpoint_id, "*")
    await client.delete(f"/v1/endpoints/{endpoint_id}", headers=tenant.headers)
    listed = await client.get(f"/v1/endpoints/{endpoint_id}/subscriptions", headers=tenant.headers)
    assert listed.status_code == 404
    match = await client.get(
        "/v1/subscriptions/match", params={"event_type": "a.b"}, headers=tenant.headers
    )
    assert match.json()["endpoints"] == []


async def test_match_agrees_with_the_reference_matcher(
    client: httpx.AsyncClient, tenant: TenantCtx
) -> None:
    names = ["invoice.paid", "invoice.paid.partial", "invoice.created", "user.created", "user"]
    for name in names:
        await _event_type(client, tenant, name)
    patterns = ["*", "invoice.*", "invoice.paid", "invoice.paid.*", "user.*", "user", "inv.*"]
    endpoints: dict[str, list[str]] = {}
    for i, chosen in enumerate(
        [patterns[:2], patterns[2:4], [patterns[4]], patterns[5:], ["invoice.paid", "user.*"]]
    ):
        endpoint_id = await _endpoint(client, tenant, f"m{i}")
        for pattern in chosen:
            assert (await _subscribe(client, tenant, endpoint_id, pattern)).status_code == 201
        endpoints[endpoint_id] = chosen

    # A disabled endpoint and a deleted endpoint must never be selected.
    disabled = await _endpoint(client, tenant, "disabled")
    await _subscribe(client, tenant, disabled, "*")
    await client.patch(
        f"/v1/endpoints/{disabled}",
        json={"enabled": False},
        headers={**tenant.headers, "If-Match": "*"},
    )
    deleted = await _endpoint(client, tenant, "deleted")
    await _subscribe(client, tenant, deleted, "*")
    await client.delete(f"/v1/endpoints/{deleted}", headers=tenant.headers)
    # Another tenant's catch-all must not leak in.
    other = await create_tenant(client)
    await _subscribe(client, other, await _endpoint(client, other, "other"), "*")

    for event_type in [*names, "invoices.paid", "unregistered.thing"]:
        response = await client.get(
            "/v1/subscriptions/match", params={"event_type": event_type}, headers=tenant.headers
        )
        assert response.status_code == 200
        got = {e["id"]: e["matched_patterns"] for e in response.json()["endpoints"]}
        expected = {
            endpoint_id: sorted(p for p in chosen if pattern_matches(p, event_type))
            for endpoint_id, chosen in endpoints.items()
            if any(pattern_matches(p, event_type) for p in chosen)
        }
        assert got == expected, event_type


async def test_match_validates_the_event_type(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    for value in ["", "Bad Type", "a.*"]:
        response = await client.get(
            "/v1/subscriptions/match", params={"event_type": value}, headers=tenant.headers
        )
        assert response.status_code == 422


def test_reference_matcher_is_consistent_with_prefix_semantics() -> None:
    segments = ["a", "b", "ab"]
    types = [".".join(p) for n in (1, 2, 3) for p in itertools.product(segments, repeat=n)]
    for event_type in types:
        assert pattern_matches("*", event_type)
        assert pattern_matches(event_type, event_type)
        head = event_type.split(".")[0]
        assert pattern_matches(f"{head}.*", event_type) == ("." in event_type)
