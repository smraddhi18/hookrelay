"""Bounded, strict JSON request bodies for ingestion.

``POST /v1/events`` reads its body itself instead of declaring a pydantic body parameter:

* authentication (a router dependency) runs before a single body byte is read, so an
  anonymous client cannot make the server buffer megabytes;
* an oversized ``Content-Length`` is rejected with 413 before reading, and the stream is cut
  off at the cap when the header lies or is absent (chunked uploads);
* JSON that Postgres ``jsonb`` cannot store (``NaN``/``Infinity``, ``\\u0000``, lone
  surrogates) or that is pathologically deep is rejected as a client error, never a 500.
"""

from __future__ import annotations

import json
import math
from typing import Any

from fastapi import Request
from fastapi.exceptions import RequestValidationError

from hookrelay.core.errors import PayloadTooLargeError, UnsupportedMediaTypeError

MAX_JSON_DEPTH = 64


def _is_json_media_type(content_type: str | None) -> bool:
    if not content_type:
        return False
    media = content_type.split(";", 1)[0].strip().lower()
    if media == "application/json":
        return True
    return media.startswith("application/") and media.endswith("+json")


def _too_large(limit: int, actual: int | None = None) -> PayloadTooLargeError:
    details: dict[str, int] = {"max_bytes": limit}
    if actual is not None:
        details["content_length"] = actual
    return PayloadTooLargeError(
        f"Request body exceeds the {limit}-byte limit", code="request_too_large", details=details
    )


def _invalid(message: str, error_type: str = "json_invalid") -> RequestValidationError:
    return RequestValidationError([{"loc": ("body",), "msg": message, "type": error_type}])


async def read_body(request: Request, *, max_bytes: int) -> bytes:
    """Read the raw body, enforcing ``max_bytes`` from the header first and the stream second."""
    if not _is_json_media_type(request.headers.get("content-type")):
        raise UnsupportedMediaTypeError(
            "Content-Type must be application/json", details={"accepted": ["application/json"]}
        )
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            declared_bytes = int(declared)
        except ValueError:
            raise _invalid("Content-Length is not an integer", "content_length_invalid") from None
        if declared_bytes > max_bytes:
            raise _too_large(max_bytes, declared_bytes)
    chunks: list[bytes] = []
    received = 0
    async for chunk in request.stream():
        received += len(chunk)
        if received > max_bytes:
            raise _too_large(max_bytes)
        chunks.append(chunk)
    return b"".join(chunks)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"{name} is not valid JSON")


def _finite_float(literal: str) -> float:
    value = float(literal)
    if not math.isfinite(value):
        raise ValueError(f"number {literal[:32]} is out of range")
    return value


def _check_string(value: str) -> None:
    if "\x00" in value:
        raise ValueError("strings must not contain \\u0000")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("strings must not contain unpaired surrogates") from None


def check_storable(document: Any) -> None:
    """Iteratively validate depth and string content (no recursion: input is untrusted)."""
    stack: list[tuple[Any, int]] = [(document, 1)]
    while stack:
        node, depth = stack.pop()
        if isinstance(node, dict):
            if depth > MAX_JSON_DEPTH:
                raise ValueError(f"nesting deeper than {MAX_JSON_DEPTH} levels")
            for key, value in node.items():
                _check_string(key)
                stack.append((value, depth + 1))
        elif isinstance(node, list):
            if depth > MAX_JSON_DEPTH:
                raise ValueError(f"nesting deeper than {MAX_JSON_DEPTH} levels")
            stack.extend((value, depth + 1) for value in node)
        elif isinstance(node, str):
            _check_string(node)


def parse_json(raw: bytes) -> Any:
    """Strictly parse ``raw``; any problem becomes a 422 ``validation_error`` on ``body``."""
    if not raw.strip():
        raise _invalid("Request body is empty", "missing")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise _invalid("Request body is not valid UTF-8") from None
    try:
        document = json.loads(text, parse_constant=_reject_constant, parse_float=_finite_float)
    except RecursionError:
        raise _invalid(f"JSON nesting deeper than {MAX_JSON_DEPTH} levels") from None
    except ValueError as exc:
        raise _invalid(f"Invalid JSON: {exc}") from None
    try:
        check_storable(document)
    except ValueError as exc:
        raise _invalid(f"Invalid JSON: {exc}") from None
    return document


def canonical_json(document: Any) -> bytes:
    """Deterministic serialisation: sorted keys, no whitespace, UTF-8.

    Used for the payload size limit and the Idempotency-Key fingerprint, so two requests that
    differ only in key order or whitespace are the same request.
    """
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
