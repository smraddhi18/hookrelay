"""POST /v1/events: validation, limits, roles, outbox atomicity and the read API."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import httpx
import pytest

from hookrelay.db.engine import Database
from hookrelay.main import create_app
from hookrelay.services import idempotency
from tests.conftest import serve
from tests.integration.conftest import (
    TenantCtx,
    _asyncpg_dsn,
    api_client,
    api_settings,
    bearer,
    create_tenant,
    issue_key,
    publish,
    register_event_type,
)


def _code(response: httpx.Response) -> str:
    return str(response.json()["error"]["code"])


async def _count(url: str, sql: str, *args: object) -> int:
    conn = await asyncpg.connect(_asyncpg_dsn(url))
    try:
        value: int = await conn.fetchval(sql, *args)
        return value
    finally:
        await conn.close()


@pytest.fixture
async def typed_tenant(client: httpx.AsyncClient, tenant: TenantCtx) -> TenantCtx:
    await register_event_type(client, tenant, "invoice.paid")
    return tenant


async def test_publish_stores_event_and_outbox_row(
    client: httpx.AsyncClient, typed_tenant: TenantCtx, migrated_database_url: str
) -> None:
    payload = {"invoice_id": "inv_1", "amount": 4200, "lines": [{"sku": "a", "qty": 2}]}
    response = await publish(client, typed_tenant, "invoice.paid", payload)
    assert response.status_code == 202, response.text
    body = response.json()
    assert response.headers["Location"] == f"/v1/events/{body['id']}"
    assert "Idempotent-Replayed" not in response.headers
    assert body["event_type"] == "invoice.paid"
    assert body["external_id"] is None
    created = datetime.fromisoformat(body["created_at"])
    assert datetime.fromisoformat(body["retain_until"]) - created == timedelta(days=30)
    assert body["occurred_at"] == body["created_at"]  # defaults to acceptance time

    fetched = await client.get(f"/v1/events/{body['id']}", headers=typed_tenant.headers)
    assert fetched.status_code == 200
    event = fetched.json()
    assert event["payload"] == payload
    assert event["fanned_out_at"] is None
    assert event["payload_size"] == len(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    )
    outbox = await _count(
        migrated_database_url,
        "SELECT count(*) FROM event_outbox WHERE event_id = $1 AND processed_at IS NULL",
        uuid.UUID(body["id"]),
    )
    assert outbox == 1


async def test_publish_requires_publisher_role(
    client: httpx.AsyncClient, typed_tenant: TenantCtx
) -> None:
    event = {"event_type": "invoice.paid", "payload": {}}
    anonymous = await client.post("/v1/events", json=event)
    assert anonymous.status_code == 401
    reader = bearer(await issue_key(client, typed_tenant, "reader"))
    forbidden = await client.post("/v1/events", json=event, headers=reader)
    assert forbidden.status_code == 403
    assert _code(forbidden) == "insufficient_role"
    publisher = bearer(await issue_key(client, typed_tenant, "publisher"))
    accepted = await client.post("/v1/events", json=event, headers=publisher)
    assert accepted.status_code == 202
    # Readers can read what publishers wrote.
    listed = await client.get("/v1/events", headers=reader)
    assert listed.status_code == 200
    assert [e["id"] for e in listed.json()["data"]] == [accepted.json()["id"]]


async def test_event_type_must_be_registered_and_live(
    client: httpx.AsyncClient, typed_tenant: TenantCtx
) -> None:
    unknown = await publish(client, typed_tenant, "invoice.voided")
    assert unknown.status_code == 422
    assert _code(unknown) == "unknown_event_type"
    archived = await client.patch(
        "/v1/event-types/invoice.paid",
        json={"archived": True},
        headers={**typed_tenant.headers, "If-Match": '"1"'},
    )
    assert archived.status_code == 200
    rejected = await publish(client, typed_tenant, "invoice.paid")
    assert rejected.status_code == 422
    assert _code(rejected) == "event_type_archived"
    malformed = await publish(client, typed_tenant, "Invoice Paid")
    assert malformed.status_code == 422
    assert _code(malformed) == "validation_error"
    # Another tenant's registry does not count.
    other = await create_tenant(client)
    assert (await publish(client, other, "invoice.paid")).status_code == 422


@pytest.mark.parametrize(
    ("raw", "fragment"),
    [
        (b'{"event_type": "invoice.paid", "payload": [1, 2]}', "dict"),
        (b'{"event_type": "invoice.paid"}', "payload"),
        (b'{"event_type": "invoice.paid", "payload": {}, "extra": 1}', "extra"),
        (b'{"event_type": "invoice.paid", "payload": {"x": NaN}}', "NaN"),
        (b'{"event_type": "invoice.paid", "payload": {"x": 1e999}}', "out of range"),
        (b'{"event_type": "invoice.paid", "payload": {"x": "a\\u0000b"}}', "u0000"),
        (b'{"event_type": "invoice.paid", "payload": {"x": "\\ud800"}}', "surrogate"),
        (b'{"event_type": "invoice.paid", "payload": {"x": 1}', "Invalid JSON"),
        (b"[]", "dict"),
        (b"   ", "empty"),
        (b'{"event_type": "invoice.paid", "payload": {"x": "\xff"}}', "UTF-8"),
        (
            b'{"event_type": "invoice.paid", "payload": {"x": '
            + b'{"a": ' * 70
            + b"1"
            + b"}" * 70
            + b"}}",
            "nesting",
        ),
    ],
)
async def test_malformed_bodies_are_422(
    client: httpx.AsyncClient, typed_tenant: TenantCtx, raw: bytes, fragment: str
) -> None:
    response = await client.post(
        "/v1/events",
        content=raw,
        headers={**typed_tenant.headers, "Content-Type": "application/json"},
    )
    assert response.status_code == 422, response.text
    assert _code(response) == "validation_error"
    assert fragment in json.dumps(response.json()["error"]["details"])


async def test_very_deep_json_is_rejected_not_crashed(
    client: httpx.AsyncClient, typed_tenant: TenantCtx
) -> None:
    raw = b"[" * 50_000 + b"]" * 50_000
    response = await client.post(
        "/v1/events",
        content=raw,
        headers={**typed_tenant.headers, "Content-Type": "application/json"},
    )
    assert response.status_code == 422


async def test_content_type_must_be_json(
    client: httpx.AsyncClient, typed_tenant: TenantCtx
) -> None:
    body = b'{"event_type": "invoice.paid", "payload": {}}'
    for content_type in ("text/plain", "application/x-www-form-urlencoded", None):
        headers = dict(typed_tenant.headers)
        if content_type:
            headers["Content-Type"] = content_type
        response = await client.post("/v1/events", content=body, headers=headers)
        assert response.status_code == 415, content_type
        assert _code(response) == "unsupported_media_type"
    vendor = await client.post(
        "/v1/events",
        content=body,
        headers={
            **typed_tenant.headers,
            "Content-Type": "application/vnd.acme+json; charset=utf-8",
        },
    )
    assert vendor.status_code == 202


async def test_payload_and_request_size_limits(migrated_database_url: str) -> None:
    async with api_client(migrated_database_url, event_max_payload_bytes=1_024) as client:
        tenant = await create_tenant(client)
        await register_event_type(client, tenant, "blob.created")
        fits = await publish(client, tenant, "blob.created", {"d": "x" * 1_000})
        assert fits.status_code == 202, fits.text

        too_big = await publish(client, tenant, "blob.created", {"d": "x" * 1_100})
        assert too_big.status_code == 413
        assert _code(too_big) == "payload_too_large"
        assert too_big.json()["error"]["details"] == {"max_bytes": 1_024, "payload_bytes": 1_108}

        # Content-Length over the hard cap: rejected before the body is read.
        huge = {"event_type": "blob.created", "payload": {"d": "x" * 20_000}}
        declared = await client.post("/v1/events", json=huge, headers=tenant.headers)
        assert declared.status_code == 413
        assert _code(declared) == "request_too_large"
        assert declared.json()["error"]["details"]["max_bytes"] == 1_024 + 16_384

        # No Content-Length (chunked): the stream is cut off at the cap.
        async def chunks() -> AsyncIterator[bytes]:
            yield b'{"event_type": "blob.created", "payload": {"d": "'
            for _ in range(40):
                yield b"x" * 1_000
            yield b'"}}'

        chunked = await client.post(
            "/v1/events",
            content=chunks(),
            headers={**tenant.headers, "Content-Type": "application/json"},
        )
        assert chunked.status_code == 413
        assert _code(chunked) == "request_too_large"


async def test_occurred_at_rules(client: httpx.AsyncClient, typed_tenant: TenantCtx) -> None:
    past = "2026-01-15T10:00:00+05:30"
    accepted = await publish(client, typed_tenant, "invoice.paid", occurred_at=past)
    assert accepted.status_code == 202
    assert datetime.fromisoformat(accepted.json()["occurred_at"]) == datetime.fromisoformat(past)

    soon = (datetime.now(UTC) + timedelta(seconds=60)).isoformat()
    assert (
        await publish(client, typed_tenant, "invoice.paid", occurred_at=soon)
    ).status_code == 202

    future = (datetime.now(UTC) + timedelta(hours=2)).isoformat()
    rejected = await publish(client, typed_tenant, "invoice.paid", occurred_at=future)
    assert rejected.status_code == 422
    assert _code(rejected) == "occurred_at_in_future"

    naive = await publish(client, typed_tenant, "invoice.paid", occurred_at="2026-01-15T10:00:00")
    assert naive.status_code == 422
    assert _code(naive) == "validation_error"


async def test_external_id_is_unique_per_tenant(
    client: httpx.AsyncClient, typed_tenant: TenantCtx, migrated_database_url: str
) -> None:
    first = await publish(client, typed_tenant, "invoice.paid", external_id="inv_9:paid")
    assert first.status_code == 202
    assert first.json()["external_id"] == "inv_9:paid"
    dup = await publish(
        client, typed_tenant, "invoice.paid", {"other": 1}, external_id="inv_9:paid"
    )
    assert dup.status_code == 409
    assert _code(dup) == "event_exists"
    assert dup.json()["error"]["details"] == {"event_id": first.json()["id"]}
    events = await _count(
        migrated_database_url,
        "SELECT count(*) FROM events WHERE tenant_id = $1",
        uuid.UUID(typed_tenant.id),
    )
    assert events == 1  # the duplicate left no event and no outbox row behind
    outbox = await _count(
        migrated_database_url,
        "SELECT count(*) FROM event_outbox WHERE tenant_id = $1",
        uuid.UUID(typed_tenant.id),
    )
    assert outbox == 1

    other = await create_tenant(client)
    await register_event_type(client, other, "invoice.paid")
    assert (
        await publish(client, other, "invoice.paid", external_id="inv_9:paid")
    ).status_code == 202
    for bad in ("", "has space", "-leading-dash", "x" * 129, "naïve"):
        response = await publish(client, typed_tenant, "invoice.paid", external_id=bad)
        assert response.status_code == 422, bad


async def test_failure_after_insert_rolls_back_event_outbox_and_key(
    migrated_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = api_settings(migrated_database_url)
    app = create_app(settings, database=Database.from_settings(settings))
    async with serve(app, raise_app_exceptions=False) as client:
        tenant = await create_tenant(client)
        await register_event_type(client, tenant, "invoice.paid")

        async def boom(*_: Any, **__: Any) -> None:
            raise RuntimeError("storage failed after the event insert")

        monkeypatch.setattr(idempotency, "complete", boom)
        failed = await publish(client, tenant, "invoice.paid", key="atomic-1")
        assert failed.status_code == 500
        tenant_id = uuid.UUID(tenant.id)
        for table in ("events", "event_outbox", "idempotency_keys"):
            count = await _count(
                migrated_database_url,
                f"SELECT count(*) FROM {table} WHERE tenant_id = $1",  # noqa: S608
                tenant_id,
            )
            assert count == 0, table
        monkeypatch.undo()
        retried = await publish(client, tenant, "invoice.paid", key="atomic-1")
        assert retried.status_code == 202
        assert "Idempotent-Replayed" not in retried.headers


async def test_list_events_filters_and_pages(client: httpx.AsyncClient, tenant: TenantCtx) -> None:
    await register_event_type(client, tenant, "a.one")
    await register_event_type(client, tenant, "b.two")
    ids = []
    for i in range(5):
        response = await publish(client, tenant, "a.one" if i % 2 == 0 else "b.two", {"i": i})
        ids.append(response.json()["id"])
    other = await create_tenant(client)
    await register_event_type(client, other, "a.one")
    await publish(client, other, "a.one")

    page1 = await client.get("/v1/events", params={"limit": 2}, headers=tenant.headers)
    body1 = page1.json()
    assert [e["id"] for e in body1["data"]] == ids[::-1][:2]
    assert body1["has_more"] is True
    assert "payload" not in body1["data"][0]
    assert body1["data"][0]["payload_size"] > 0
    page2 = await client.get(
        "/v1/events", params={"limit": 10, "cursor": body1["next_cursor"]}, headers=tenant.headers
    )
    assert [e["id"] for e in page2.json()["data"]] == ids[::-1][2:]
    assert page2.json()["has_more"] is False

    only_a = await client.get("/v1/events", params={"event_type": "a.one"}, headers=tenant.headers)
    assert [e["id"] for e in only_a.json()["data"]] == [ids[4], ids[2], ids[0]]

    events = page2.json()["data"] + body1["data"]
    pivot = next(e for e in events if e["id"] == ids[2])["created_at"]
    after = await client.get("/v1/events", params={"created_after": pivot}, headers=tenant.headers)
    assert [e["id"] for e in after.json()["data"]] == [ids[4], ids[3], ids[2]]
    before = await client.get(
        "/v1/events", params={"created_before": pivot}, headers=tenant.headers
    )
    assert [e["id"] for e in before.json()["data"]] == [ids[1], ids[0]]

    naive = await client.get(
        "/v1/events", params={"created_after": "2026-01-01T00:00:00"}, headers=tenant.headers
    )
    assert naive.status_code == 422
    bad_cursor = await client.get("/v1/events", params={"cursor": "!!"}, headers=tenant.headers)
    assert bad_cursor.status_code == 400


async def test_reads_are_tenant_scoped(client: httpx.AsyncClient, typed_tenant: TenantCtx) -> None:
    event_id = (await publish(client, typed_tenant, "invoice.paid")).json()["id"]
    other = await create_tenant(client)
    for path in (f"/v1/events/{event_id}", f"/v1/events/{event_id}/deliveries"):
        response = await client.get(path, headers=other.headers)
        assert response.status_code == 404
        assert _code(response) == "event_not_found"
    missing = await client.get(f"/v1/events/{uuid.uuid4()}", headers=typed_tenant.headers)
    assert missing.status_code == 404
    deliveries = await client.get(f"/v1/events/{event_id}/deliveries", headers=typed_tenant.headers)
    assert deliveries.status_code == 200
    assert deliveries.json() == {"data": [], "next_cursor": None, "has_more": False}
