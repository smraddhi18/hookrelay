"""Request correlation: accept or mint a request ID, expose it to logs and responses."""

from __future__ import annotations

import re
import time
import uuid
from typing import Final

import structlog
from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_VALID_REQUEST_ID: Final = re.compile(r"^[A-Za-z0-9._:\-]{1,128}$")
REQUEST_ID_STATE_KEY: Final = "request_id"

access_logger = structlog.stdlib.get_logger("hookrelay.access")


def new_request_id() -> str:
    return uuid.uuid4().hex


def coerce_request_id(candidate: str | None) -> str:
    """Trust a caller-supplied ID only if it is short and log-safe; otherwise mint a new one.

    This prevents log injection (newlines, ANSI escapes) and unbounded header values from
    ending up in every log line of the request.
    """
    if candidate and _VALID_REQUEST_ID.fullmatch(candidate):
        return candidate
    return new_request_id()


def request_id_from_scope(scope: Scope) -> str | None:
    state = scope.get("state")
    if isinstance(state, dict):
        value = state.get(REQUEST_ID_STATE_KEY)
        return value if isinstance(value, str) else None
    return None


class RequestContextMiddleware:
    """Pure ASGI middleware (no BaseHTTPMiddleware overhead or streaming pitfalls).

    * reads ``X-Request-ID`` (configurable) or generates one,
    * stores it in ``scope["state"]`` so exception handlers can echo it even for 500s,
    * binds it into structlog contextvars for every log line of the request,
    * sets it on the response and emits one structured access-log line.
    """

    def __init__(self, app: ASGIApp, header_name: str = "X-Request-ID") -> None:
        self.app = app
        self.header_name = header_name
        self._header_key = header_name.lower().encode("latin-1")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        incoming: str | None = None
        for key, value in scope.get("headers", []):
            if key == self._header_key:
                incoming = value.decode("latin-1")
                break
        request_id = coerce_request_id(incoming)
        scope.setdefault("state", {})[REQUEST_ID_STATE_KEY] = request_id

        status_code = 500
        started = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                headers = MutableHeaders(scope=message)
                headers[self.header_name] = request_id
            await send(message)

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            access_logger.info(
                "http.request",
                method=scope.get("method"),
                path=scope.get("path"),
                status_code=status_code,
                duration_ms=duration_ms,
                client=_client_host(scope),
            )
            structlog.contextvars.clear_contextvars()


def _client_host(scope: Scope) -> str | None:
    client = scope.get("client")
    if client:
        host = client[0]
        return str(host)
    return None
