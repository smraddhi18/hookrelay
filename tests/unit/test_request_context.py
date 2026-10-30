from __future__ import annotations

import re

import pytest

from hookrelay.core.request_context import coerce_request_id, new_request_id
from tests.conftest import serve

HEX32 = re.compile(r"^[0-9a-f]{32}$")


def test_new_request_id_is_uuid_hex() -> None:
    assert HEX32.fullmatch(new_request_id())
    assert new_request_id() != new_request_id()


@pytest.mark.parametrize("value", ["abc-123", "req_01HZX.y:z", "a" * 128])
def test_coerce_keeps_safe_ids(value: str) -> None:
    assert coerce_request_id(value) == value


@pytest.mark.parametrize("value", [None, "", "a" * 129, "has space", "line\nbreak", "\x1b[31m"])
def test_coerce_replaces_unsafe_ids(value: str | None) -> None:
    assert HEX32.fullmatch(coerce_request_id(value))


async def test_generates_request_id_when_missing(app_factory) -> None:
    async with serve(app_factory()) as client:
        resp = await client.get("/health")
    assert HEX32.fullmatch(resp.headers["x-request-id"])


async def test_echoes_valid_incoming_request_id(app_factory) -> None:
    async with serve(app_factory()) as client:
        resp = await client.get("/health", headers={"X-Request-ID": "trace-abc-1"})
    assert resp.headers["x-request-id"] == "trace-abc-1"


async def test_replaces_invalid_incoming_request_id(app_factory) -> None:
    async with serve(app_factory()) as client:
        resp = await client.get("/health", headers={"X-Request-ID": "x" * 300})
    assert HEX32.fullmatch(resp.headers["x-request-id"])


async def test_each_request_gets_its_own_id(app_factory) -> None:
    async with serve(app_factory()) as client:
        a = await client.get("/health")
        b = await client.get("/health")
    assert a.headers["x-request-id"] != b.headers["x-request-id"]


async def test_custom_header_name(app_factory) -> None:
    async with serve(app_factory(request_id_header="X-Correlation-ID")) as client:
        resp = await client.get("/health", headers={"X-Correlation-ID": "corr-1"})
    assert resp.headers["x-correlation-id"] == "corr-1"
    assert "x-request-id" not in resp.headers


async def test_request_id_is_on_error_responses(app_factory) -> None:
    async with serve(app_factory()) as client:
        resp = await client.get("/nope", headers={"X-Request-ID": "err-1"})
    assert resp.status_code == 404
    assert resp.headers["x-request-id"] == "err-1"
    assert resp.json()["error"]["request_id"] == "err-1"
