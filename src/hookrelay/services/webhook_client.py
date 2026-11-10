"""Outbound webhook HTTP: an httpx client on an SSRF-guarded transport, plus one-shot sending.

* :class:`GuardedTransport` is an ``httpx`` transport over an ``httpcore`` connection pool whose
  network backend is :class:`~hookrelay.services.ssrf.GuardedNetworkBackend`; connections are
  pooled per hostname, so keep-alive works and TLS/SNI use the hostname.
* :class:`WebhookSender` sends one signed request with the endpoint's total timeout (connect is
  capped separately), never follows redirects, reads at most ``response_max_bytes`` of the body
  (raw, no decompression: no zip bombs) and turns every outcome into a :class:`SendResult`
  rather than an exception.
"""

from __future__ import annotations

import asyncio
import functools
import math
import ssl
import time
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Final

import httpcore
import httpx

from hookrelay.services.delivery_policy import (
    ErrorClass,
    error_class_for_status,
    parse_retry_after,
    truncate_body,
    truncate_message,
)
from hookrelay.services.ssrf import (
    AddressPolicy,
    DestinationBlockedError,
    GuardedNetworkBackend,
    ResolutionError,
    Resolver,
    system_resolver,
)

USER_AGENT: Final = "hookrelay-webhooks/1.0"
_DEFAULT_PORTS: Final = {"http": 80, "https": 443}

# Most specific first: the first matching entry wins.
_HTTPCORE_TO_HTTPX: Final[tuple[tuple[type[Exception], type[httpx.HTTPError]], ...]] = (
    (httpcore.ConnectTimeout, httpx.ConnectTimeout),
    (httpcore.ReadTimeout, httpx.ReadTimeout),
    (httpcore.WriteTimeout, httpx.WriteTimeout),
    (httpcore.PoolTimeout, httpx.PoolTimeout),
    (httpcore.TimeoutException, httpx.TimeoutException),
    (httpcore.ConnectError, httpx.ConnectError),
    (httpcore.ReadError, httpx.ReadError),
    (httpcore.WriteError, httpx.WriteError),
    (httpcore.NetworkError, httpx.NetworkError),
    (httpcore.RemoteProtocolError, httpx.RemoteProtocolError),
    (httpcore.LocalProtocolError, httpx.LocalProtocolError),
    (httpcore.ProtocolError, httpx.ProtocolError),
)


@contextmanager
def _map_httpcore_errors() -> Iterator[None]:
    try:
        yield
    except tuple(src for src, _ in _HTTPCORE_TO_HTTPX) as exc:
        for source, target in _HTTPCORE_TO_HTTPX:  # pragma: no branch - one entry always matches
            if isinstance(exc, source):
                raise target(str(exc)) from exc


class _ResponseStream(httpx.AsyncByteStream):
    def __init__(self, stream: httpcore.Response, on_close: Callable[[], None]) -> None:
        self._response = stream
        self._on_close: Callable[[], None] | None = on_close

    async def __aiter__(self) -> AsyncIterator[bytes]:
        with _map_httpcore_errors():
            async for chunk in self._response.aiter_stream():
                yield chunk

    async def aclose(self) -> None:
        try:
            await self._response.aclose()
        finally:
            if self._on_close is not None:
                self._on_close()
                self._on_close = None


# httpcore scans every connection of a pool (expiry check, socket poll) on each request and
# release, so one pool of N connections costs O(N) per request. Measured on the box: with a
# single 128-connection pool that scan dominated the worker profile, and sharding took a
# 128-slot worker from 120.7 to 472.0 deliveries/s (docs/capacity.md). Sharding keeps each
# scan to about this many connections.
POOL_SHARD_CONNECTIONS = 16


class GuardedTransport(httpx.AsyncBaseTransport):
    """``httpx`` transport whose sockets only reach addresses the SSRF policy allows.

    Requests are spread over ``ceil(max_connections / POOL_SHARD_CONNECTIONS)`` connection
    pools, each request going to the pool with the fewest requests in flight. Every pool may
    open up to ``max_connections`` (so a request never queues behind a busy shard while the
    caller is within its own concurrency), but keeps at most its share alive when idle.
    """

    def __init__(
        self,
        backend: GuardedNetworkBackend,
        *,
        ssl_context: ssl.SSLContext | None = None,
        max_connections: int = 100,
        keepalive_expiry: float = 30.0,
        shard_connections: int = POOL_SHARD_CONNECTIONS,
    ) -> None:
        shards = max(1, math.ceil(max_connections / max(1, shard_connections)))
        keepalive = math.ceil(max_connections / shards)
        context = ssl_context or httpx.create_ssl_context()
        self._pools = [
            httpcore.AsyncConnectionPool(
                ssl_context=context,
                max_connections=max_connections,
                max_keepalive_connections=keepalive,
                keepalive_expiry=keepalive_expiry,
                http1=True,
                http2=False,
                retries=0,  # the delivery retry policy owns retries
                network_backend=backend,
            )
            for _ in range(shards)
        ]
        self._active = [0] * shards

    @property
    def shards(self) -> int:
        return len(self._pools)

    def _release(self, shard: int) -> None:
        self._active[shard] -= 1

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)  # noqa: S101
        core_request = httpcore.Request(
            method=request.method,
            url=httpcore.URL(
                scheme=request.url.raw_scheme,
                host=request.url.raw_host,
                port=request.url.port,
                target=request.url.raw_path,
            ),
            headers=request.headers.raw,
            content=request.stream,
            extensions=request.extensions,
        )
        shard = min(range(len(self._active)), key=self._active.__getitem__)
        self._active[shard] += 1
        try:
            with _map_httpcore_errors():
                response = await self._pools[shard].handle_async_request(core_request)
        except BaseException:
            self._release(shard)
            raise
        return httpx.Response(
            status_code=response.status,
            headers=response.headers,
            stream=_ResponseStream(response, functools.partial(self._release, shard)),
            extensions=response.extensions,
        )

    async def aclose(self) -> None:
        for pool in self._pools:
            await pool.aclose()


@dataclass(frozen=True, slots=True)
class WebhookRequest:
    url: str
    headers: Mapping[str, str]
    body: bytes
    timeout_s: float


@dataclass(frozen=True, slots=True)
class SendResult:
    status_code: int | None
    error_class: ErrorClass | None
    error_message: str | None
    latency_ms: int | None
    response_body: str | None
    response_truncated: bool
    retry_after_s: float | None

    @property
    def succeeded(self) -> bool:
        return self.error_class is None


@dataclass(frozen=True, slots=True)
class SenderConfig:
    allow_http: bool
    allowed_ports: frozenset[int]
    connect_timeout_s: float
    response_max_bytes: int
    retry_after_max_s: float


def check_destination(url: httpx.URL, *, allow_http: bool, allowed_ports: frozenset[int]) -> None:
    """Scheme / host / port allow-list (addresses are vetted later, at connect time)."""
    scheme = url.scheme
    if scheme != "https" and not (scheme == "http" and allow_http):
        raise DestinationBlockedError(f"scheme {scheme or '(none)'!r} is not allowed")
    if not url.host:
        raise DestinationBlockedError("URL has no host")
    if url.userinfo:
        raise DestinationBlockedError("credentials in the URL are not allowed")
    port = url.port or _DEFAULT_PORTS[scheme]
    if port not in allowed_ports:
        raise DestinationBlockedError(f"port {port} is not allowed")


def _classify_exception(exc: BaseException) -> tuple[ErrorClass, str]:  # noqa: PLR0911
    match exc:
        case DestinationBlockedError():
            return ErrorClass.DESTINATION_BLOCKED, str(exc)
        case ResolutionError():
            return ErrorClass.DNS_ERROR, str(exc)
        case httpx.TimeoutException() | TimeoutError():
            return ErrorClass.TIMEOUT, str(exc) or "request timed out"
        case httpx.ConnectError() if _is_tls(exc):
            return ErrorClass.TLS_ERROR, str(exc)
        case httpx.ConnectError() | httpx.NetworkError() | OSError():
            return ErrorClass.CONNECT_ERROR, str(exc) or type(exc).__name__
        case httpx.ProtocolError() | httpx.DecodingError() | httpx.InvalidURL():
            return ErrorClass.PROTOCOL_ERROR, str(exc) or type(exc).__name__
        case _:
            return ErrorClass.CONNECT_ERROR, f"{type(exc).__name__}: {exc}"


def _is_tls(exc: BaseException) -> bool:
    text = str(exc).lower()
    return "ssl" in text or "certificate" in text or "tls" in text


class WebhookSender:
    """Sends webhooks through one shared, guarded httpx client (create once per worker)."""

    def __init__(
        self,
        config: SenderConfig,
        *,
        policy: AddressPolicy,
        resolver: Resolver = system_resolver,
        transport: httpx.AsyncBaseTransport | None = None,
        max_connections: int = 100,
    ) -> None:
        self._config = config
        self._transport = transport or GuardedTransport(
            GuardedNetworkBackend(policy, resolver=resolver), max_connections=max_connections
        )
        self._client = httpx.AsyncClient(
            transport=self._transport,
            follow_redirects=False,
            trust_env=False,  # never route webhooks through an ambient HTTP(S)_PROXY
            headers={"user-agent": USER_AGENT},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def send(self, request: WebhookRequest, *, now: datetime) -> SendResult:
        started = time.perf_counter()
        status_code: int | None = None
        retry_after: float | None = None
        body = b""
        truncated = False
        try:
            url = httpx.URL(request.url)
            check_destination(
                url, allow_http=self._config.allow_http, allowed_ports=self._config.allowed_ports
            )
            total = request.timeout_s
            timeout = httpx.Timeout(total, connect=min(self._config.connect_timeout_s, total))
            built = self._client.build_request(
                "POST",
                url,
                content=request.body,
                headers={**request.headers, "accept-encoding": "identity"},
                timeout=timeout,
            )
            # The httpx timeouts are per phase; this bounds the whole attempt (a receiver that
            # trickles one byte per second cannot hold a worker slot past the endpoint timeout).
            async with asyncio.timeout(total):
                response = await self._client.send(built, stream=True)
                try:
                    status_code = response.status_code
                    retry_after = parse_retry_after(
                        response.headers.get("retry-after"),
                        now,
                        self._config.retry_after_max_s,
                    )
                    body, truncated = await self._read_capped(response)
                finally:
                    await response.aclose()
        except Exception as exc:
            if status_code is None:
                error_class, message = _classify_exception(exc)
                return SendResult(
                    status_code=None,
                    error_class=error_class,
                    error_message=truncate_message(message),
                    latency_ms=_elapsed_ms(started),
                    response_body=None,
                    response_truncated=False,
                    retry_after_s=None,
                )
            # Headers arrived but the body read failed or timed out: the status code decides.
            truncated = True
        text, cut = truncate_body(body, self._config.response_max_bytes)
        status_error = error_class_for_status(status_code)
        return SendResult(
            status_code=status_code,
            error_class=status_error,
            error_message=None if status_error is None else f"receiver returned HTTP {status_code}",
            latency_ms=_elapsed_ms(started),
            response_body=text,
            response_truncated=truncated or cut,
            retry_after_s=retry_after,
        )

    async def _read_capped(self, response: httpx.Response) -> tuple[bytes, bool]:
        limit = self._config.response_max_bytes
        buffer = bytearray()
        async for chunk in response.aiter_raw():
            buffer.extend(chunk)
            if len(buffer) > limit:
                return bytes(buffer[: limit + 1]), True
        return bytes(buffer), False


def _elapsed_ms(started: float) -> int:
    return max(0, round((time.perf_counter() - started) * 1000))
