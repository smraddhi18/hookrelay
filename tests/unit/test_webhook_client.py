"""WebhookSender against a real local socket: status handling, caps, timeouts and the guard."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from hookrelay.services.delivery_policy import ErrorClass
from hookrelay.services.ssrf import (
    AddressPolicy,
    DestinationBlockedError,
    GuardedNetworkBackend,
    ResolutionError,
)
from hookrelay.services.webhook_client import (
    USER_AGENT,
    GuardedTransport,
    SenderConfig,
    WebhookRequest,
    WebhookSender,
    _classify_exception,
    check_destination,
)
from tests.receiver import Reply, local_resolver, running_receiver

NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


def _sender(port: int, *, private: bool = True, **overrides: Any) -> WebhookSender:
    config: dict[str, Any] = {
        "allow_http": True,
        "allowed_ports": frozenset({port, 443}),
        "connect_timeout_s": 1.0,
        "response_max_bytes": 64,
        "retry_after_max_s": 600.0,
    }
    config.update(overrides)
    return WebhookSender(
        SenderConfig(**config),
        policy=AddressPolicy(allow_private_networks=private),
        resolver=local_resolver,
        max_connections=4,
    )


def _request(url: str, timeout_s: float = 2.0) -> WebhookRequest:
    return WebhookRequest(
        url=url, headers={"content-type": "application/json"}, body=b'{"a":1}', timeout_s=timeout_s
    )


async def _send(reply: Reply, *, timeout_s: float = 2.0, **overrides: Any) -> Any:
    async with running_receiver(lambda _r: reply) as receiver:
        sender = _sender(receiver.port, **overrides)
        try:
            result = await sender.send(_request(receiver.url, timeout_s), now=NOW)
        finally:
            await sender.aclose()
        return result, receiver


async def test_success_posts_the_body_and_records_the_response() -> None:
    result, receiver = await _send(Reply(200, b'{"ok":true}'))
    assert result.succeeded
    assert result.status_code == 200
    assert result.error_class is None
    assert result.response_body == '{"ok":true}'
    assert not result.response_truncated
    assert result.latency_ms is not None
    [received] = receiver.requests
    assert received.method == "POST"
    assert received.path == "/hook"
    assert received.body == b'{"a":1}'
    assert received.headers["user-agent"] == USER_AGENT
    assert received.headers["accept-encoding"] == "identity"
    assert received.headers["host"] == f"receiver.test:{receiver.port}"  # Host keeps the name


async def test_redirects_are_not_followed() -> None:
    result, receiver = await _send(Reply(302, b"", {"location": "http://169.254.169.254/"}))
    assert result.status_code == 302
    assert result.error_class is ErrorClass.REDIRECT
    assert len(receiver.requests) == 1


async def test_server_error_with_retry_after() -> None:
    result, _ = await _send(Reply(503, b"busy", {"retry-after": "7"}))
    assert result.error_class is ErrorClass.HTTP_STATUS
    assert result.error_message == "receiver returned HTTP 503"
    assert result.retry_after_s == 7.0
    assert not result.succeeded


async def test_response_body_is_capped() -> None:
    result, _ = await _send(Reply(500, b"x" * 10_000))
    assert result.response_body == "x" * 64
    assert result.response_truncated


async def test_zero_byte_cap_stores_nothing() -> None:
    result, _ = await _send(Reply(200, b"hello"), response_max_bytes=0)
    assert result.response_body is None
    assert result.response_truncated


async def test_slow_headers_time_out() -> None:
    result, _ = await _send(Reply(200, delay_s=1.0), timeout_s=0.2)
    assert result.status_code is None
    assert result.error_class is ErrorClass.TIMEOUT


async def test_slow_body_after_status_keeps_the_status() -> None:
    result, _ = await _send(Reply(200, b"late", body_delay_s=1.0), timeout_s=0.3)
    assert result.status_code == 200
    assert result.succeeded
    assert result.response_truncated


async def test_garbage_response_is_a_protocol_error() -> None:
    result, _ = await _send(Reply(raw=b"NOT HTTP AT ALL\r\n\r\n"))
    assert result.error_class is ErrorClass.PROTOCOL_ERROR


async def test_connection_closed_without_response() -> None:
    result, _ = await _send(Reply(raw=b""))
    assert result.error_class in {ErrorClass.PROTOCOL_ERROR, ErrorClass.CONNECT_ERROR}


async def test_connection_refused() -> None:
    async with running_receiver() as receiver:
        port = receiver.port
    sender = _sender(port)
    try:
        result = await sender.send(_request(f"http://receiver.test:{port}/hook"), now=NOW)
    finally:
        await sender.aclose()
    assert result.error_class is ErrorClass.CONNECT_ERROR


async def test_tls_to_a_plain_http_server_is_a_tls_error() -> None:
    async with running_receiver() as receiver:
        sender = _sender(receiver.port)
        try:
            result = await sender.send(_request(f"https://receiver.test:{receiver.port}/"), now=NOW)
        finally:
            await sender.aclose()
    assert result.error_class in {ErrorClass.TLS_ERROR, ErrorClass.CONNECT_ERROR}


async def test_private_destination_blocked_without_opt_in() -> None:
    result, receiver = await _send(Reply(), private=False)
    assert result.error_class is ErrorClass.DESTINATION_BLOCKED
    assert "loopback" in (result.error_message or "")
    assert receiver.requests == []


@pytest.mark.parametrize(
    ("url", "error_class"),
    [
        ("http://metadata.test/latest", ErrorClass.DESTINATION_BLOCKED),
        ("http://rebind.test/", ErrorClass.DESTINATION_BLOCKED),
        ("http://nowhere.test/", ErrorClass.DNS_ERROR),
        ("http://169.254.169.254/", ErrorClass.DESTINATION_BLOCKED),
        ("ftp://receiver.test/", ErrorClass.DESTINATION_BLOCKED),
        ("http://receiver.test:22/", ErrorClass.DESTINATION_BLOCKED),
        ("http://user:pw@receiver.test/", ErrorClass.DESTINATION_BLOCKED),
        ("http://[::1", ErrorClass.PROTOCOL_ERROR),
    ],
)
async def test_refused_destinations(url: str, error_class: ErrorClass) -> None:
    sender = _sender(80)
    try:
        result = await sender.send(_request(url), now=NOW)
    finally:
        await sender.aclose()
    assert result.error_class is error_class, result.error_message


def test_check_destination_rules() -> None:
    ports = frozenset({443})
    check_destination(httpx.URL("https://a.example/x"), allow_http=False, allowed_ports=ports)
    with pytest.raises(DestinationBlockedError, match="scheme"):
        check_destination(httpx.URL("http://a.example/"), allow_http=False, allowed_ports=ports)
    with pytest.raises(DestinationBlockedError, match="port 80"):
        check_destination(httpx.URL("http://a.example/"), allow_http=True, allowed_ports=ports)
    with pytest.raises(DestinationBlockedError, match="no host"):
        check_destination(httpx.URL("https:///path"), allow_http=False, allowed_ports=ports)


@pytest.mark.parametrize(
    ("exc", "error_class"),
    [
        (ResolutionError("x"), ErrorClass.DNS_ERROR),
        (TimeoutError(), ErrorClass.TIMEOUT),
        (httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED]"), ErrorClass.TLS_ERROR),
        (httpx.ConnectError("refused"), ErrorClass.CONNECT_ERROR),
        (httpx.ReadError(""), ErrorClass.CONNECT_ERROR),
        (ConnectionResetError(), ErrorClass.CONNECT_ERROR),
        (httpx.RemoteProtocolError("bad"), ErrorClass.PROTOCOL_ERROR),
        (RuntimeError("weird"), ErrorClass.CONNECT_ERROR),
    ],
)
def test_exception_classification(exc: BaseException, error_class: ErrorClass) -> None:
    classified, message = _classify_exception(exc)
    assert classified is error_class
    assert message


async def test_connections_are_reused() -> None:
    async with running_receiver() as receiver:
        sender = _sender(receiver.port)
        try:
            for _ in range(3):
                assert (await sender.send(_request(receiver.url), now=NOW)).succeeded
        finally:
            await sender.aclose()
        assert len(receiver.requests) == 3


async def test_concurrent_sends() -> None:
    async with running_receiver(lambda _r: Reply(204, b"")) as receiver:
        sender = _sender(receiver.port)
        try:
            results = await asyncio.gather(
                *(sender.send(_request(receiver.url), now=NOW) for _ in range(10))
            )
        finally:
            await sender.aclose()
    assert all(r.status_code == 204 for r in results)
    assert all(r.response_body is None for r in results)


# -- Milestone 8: sharded connection pools ---------------------------------------------------


def _sharded_sender(port: int, transport: GuardedTransport) -> WebhookSender:
    return WebhookSender(
        SenderConfig(
            allow_http=True,
            allowed_ports=frozenset({port}),
            connect_timeout_s=1.0,
            response_max_bytes=64,
            retry_after_max_s=600.0,
        ),
        policy=AddressPolicy(allow_private_networks=True),
        transport=transport,
    )


def test_pool_shard_count_follows_max_connections() -> None:
    backend = GuardedNetworkBackend(AddressPolicy(), resolver=local_resolver)
    assert GuardedTransport(backend, max_connections=4).shards == 1
    assert GuardedTransport(backend, max_connections=16).shards == 1
    assert GuardedTransport(backend, max_connections=17).shards == 2
    assert GuardedTransport(backend, max_connections=128).shards == 8
    assert GuardedTransport(backend, max_connections=40, shard_connections=0).shards == 40


async def test_concurrent_requests_spread_over_shards_and_release_them() -> None:
    backend = GuardedNetworkBackend(
        AddressPolicy(allow_private_networks=True), resolver=local_resolver
    )
    transport = GuardedTransport(backend, max_connections=48, shard_connections=16)
    assert transport.shards == 3
    async with running_receiver(lambda _r: Reply(204, b"")) as receiver:
        sender = _sharded_sender(receiver.port, transport)
        try:
            results = await asyncio.gather(
                *(sender.send(_request(receiver.url), now=NOW) for _ in range(9))
            )
            in_flight_after = list(transport._active)
            used = [len(pool.connections) for pool in transport._pools]
        finally:
            await sender.aclose()
    assert all(r.status_code == 204 for r in results)
    assert in_flight_after == [0, 0, 0]
    assert all(n >= 1 for n in used), used  # every shard carried traffic


async def test_failed_requests_release_their_shard() -> None:
    backend = GuardedNetworkBackend(
        AddressPolicy(allow_private_networks=True), resolver=local_resolver
    )
    transport = GuardedTransport(backend, max_connections=32, shard_connections=16)
    async with running_receiver() as receiver:
        port = receiver.port
    sender = _sharded_sender(port, transport)  # the receiver is gone: connection refused
    try:
        result = await sender.send(_request(f"http://receiver.test:{port}/hook"), now=NOW)
    finally:
        await sender.aclose()
    assert result.error_class is ErrorClass.CONNECT_ERROR
    assert transport._active == [0, 0]
