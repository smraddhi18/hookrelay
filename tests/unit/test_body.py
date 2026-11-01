"""Strict, bounded JSON bodies for ingestion (api/body.py)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import pytest
from fastapi import Request
from fastapi.exceptions import RequestValidationError

from hookrelay.api.body import MAX_JSON_DEPTH, canonical_json, check_storable, parse_json, read_body
from hookrelay.core.errors import PayloadTooLargeError, UnsupportedMediaTypeError


def _request(chunks: Sequence[bytes], headers: dict[str, str]) -> Request:
    messages = [
        {"type": "http.request", "body": chunk, "more_body": i < len(chunks) - 1}
        for i, chunk in enumerate(chunks)
    ] or [{"type": "http.request", "body": b"", "more_body": False}]

    async def receive() -> dict[str, Any]:
        return messages.pop(0)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/v1/events",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
    }
    return Request(scope, receive)


JSON = {"content-type": "application/json"}


async def test_reads_chunked_bodies_under_the_limit() -> None:
    request = _request([b'{"a":', b" 1}"], JSON)
    assert await read_body(request, max_bytes=100) == b'{"a": 1}'


@pytest.mark.parametrize(
    "content_type",
    [
        "application/json",
        "application/json; charset=utf-8",
        "Application/JSON",
        "application/cloudevents+json",
    ],
)
async def test_accepts_json_media_types(content_type: str) -> None:
    request = _request([b"{}"], {"content-type": content_type})
    assert await read_body(request, max_bytes=10) == b"{}"


@pytest.mark.parametrize("content_type", [None, "", "text/json", "text/plain", "application/xml"])
async def test_rejects_other_media_types(content_type: str | None) -> None:
    headers = {} if content_type is None else {"content-type": content_type}
    with pytest.raises(UnsupportedMediaTypeError):
        await read_body(_request([b"{}"], headers), max_bytes=10)


async def test_declared_length_over_the_limit_is_413_before_reading() -> None:
    def explode() -> None:
        raise AssertionError("body must not be read")

    request = _request([b"{}"], {**JSON, "content-length": "11"})
    request._receive = explode  # type: ignore[assignment]
    with pytest.raises(PayloadTooLargeError) as info:
        await read_body(request, max_bytes=10)
    assert info.value.details == {"max_bytes": 10, "content_length": 11}


async def test_stream_is_cut_off_when_the_header_lies() -> None:
    request = _request([b"x" * 6, b"x" * 6], {**JSON, "content-length": "2"})
    with pytest.raises(PayloadTooLargeError) as info:
        await read_body(request, max_bytes=10)
    assert info.value.details == {"max_bytes": 10}


async def test_non_integer_content_length_is_a_validation_error() -> None:
    with pytest.raises(RequestValidationError):
        await read_body(_request([b"{}"], {**JSON, "content-length": "ten"}), max_bytes=10)


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"  \n",
        b"\xff\xfe",
        b"{",
        b'{"a": NaN}',
        b'{"a": -Infinity}',
        b'{"a": 1e400}',
        b'{"a": "\\u0000"}',
        b'{"\\u0000": 1}',
        b'{"a": "\\udc00"}',
        b"[" * 100_000 + b"]" * 100_000,
    ],
)
def test_parse_json_rejects(raw: bytes) -> None:
    with pytest.raises(RequestValidationError) as info:
        parse_json(raw)
    assert info.value.errors()[0]["loc"] == ("body",)


def test_parse_json_accepts_ordinary_documents() -> None:
    doc = {"s": "naïve ☃", "n": [1, 2.5, -3e10, True, None], "o": {"x": {}}}
    assert parse_json(json.dumps(doc).encode()) == doc


def test_depth_limit_is_exact() -> None:
    ok: Any = 1
    for _ in range(MAX_JSON_DEPTH):
        ok = [ok]
    check_storable(ok)
    with pytest.raises(ValueError, match="nesting"):
        check_storable([ok])
    with pytest.raises(ValueError, match="nesting"):
        check_storable({"a": ok})


def test_canonical_json_is_order_and_whitespace_independent() -> None:
    a = parse_json(b'{"b": [1, {"y": 2, "x": 1}], "a": "\xc3\xa9"}')
    b = parse_json(b'{"a":"\\u00e9","b":[1,{"x":1,"y":2}]}')
    assert canonical_json(a) == canonical_json(b) == '{"a":"é","b":[1,{"x":1,"y":2}]}'.encode()
    with pytest.raises(ValueError, match="Out of range float"):
        canonical_json({"x": float("nan")})
