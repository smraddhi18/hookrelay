"""SSRF guard: address categories (incl. v4-in-v6 tricks), the policy switch and the backend."""

from __future__ import annotations

import asyncio
import ipaddress
import socket

import httpcore
import pytest

from hookrelay.services.ssrf import (
    AddressCategory,
    AddressPolicy,
    DestinationBlockedError,
    GuardedNetworkBackend,
    ResolutionError,
    categorize,
    system_resolver,
)

STRICT = AddressPolicy()
DEV = AddressPolicy(allow_private_networks=True)


@pytest.mark.parametrize(
    ("raw", "category"),
    [
        ("0.0.0.0", AddressCategory.UNSPECIFIED),  # noqa: S104
        ("0.1.2.3", AddressCategory.UNSPECIFIED),
        ("127.0.0.1", AddressCategory.LOOPBACK),
        ("127.255.255.254", AddressCategory.LOOPBACK),
        ("10.0.0.1", AddressCategory.PRIVATE),
        ("172.16.5.4", AddressCategory.PRIVATE),
        ("172.31.255.255", AddressCategory.PRIVATE),
        ("192.168.1.1", AddressCategory.PRIVATE),
        ("100.64.0.1", AddressCategory.SHARED),  # CGNAT
        ("100.127.255.254", AddressCategory.SHARED),
        ("169.254.1.1", AddressCategory.LINK_LOCAL),
        ("169.254.169.254", AddressCategory.METADATA),  # AWS/GCP/Azure IMDS
        ("169.254.170.2", AddressCategory.METADATA),  # ECS task metadata
        ("100.100.100.200", AddressCategory.METADATA),  # Alibaba
        ("224.0.0.1", AddressCategory.MULTICAST),
        ("255.255.255.255", AddressCategory.BROADCAST),
        ("240.0.0.1", AddressCategory.RESERVED),
        ("192.0.0.8", AddressCategory.RESERVED),
        ("192.0.2.10", AddressCategory.RESERVED),  # TEST-NET-1
        ("198.18.0.1", AddressCategory.RESERVED),  # benchmarking
        ("198.51.100.7", AddressCategory.RESERVED),
        ("203.0.113.9", AddressCategory.RESERVED),
        ("::", AddressCategory.UNSPECIFIED),
        ("::1", AddressCategory.LOOPBACK),
        ("fc00::1", AddressCategory.PRIVATE),
        ("fd12:3456::1", AddressCategory.PRIVATE),
        ("fe80::1", AddressCategory.LINK_LOCAL),
        ("ff02::1", AddressCategory.MULTICAST),
        ("2001:db8::1", AddressCategory.RESERVED),
        ("fd00:ec2::254", AddressCategory.METADATA),  # AWS IMDS over IPv6
        # IPv4 smuggled inside IPv6 is judged by the embedded address.
        ("::ffff:127.0.0.1", AddressCategory.LOOPBACK),
        ("::ffff:169.254.169.254", AddressCategory.METADATA),
        ("::ffff:10.1.2.3", AddressCategory.PRIVATE),
        ("64:ff9b::a9fe:a9fe", AddressCategory.METADATA),  # NAT64 of 169.254.169.254
        ("64:ff9b::7f00:1", AddressCategory.LOOPBACK),
        ("2002:7f00:1::", AddressCategory.LOOPBACK),  # 6to4 of 127.0.0.1
        ("2002:a00:1::", AddressCategory.PRIVATE),  # 6to4 of 10.0.0.1
        ("::127.0.0.1", AddressCategory.RESERVED),  # deprecated IPv4-compatible
    ],
)
def test_blocked_categories(raw: str, category: AddressCategory) -> None:
    assert categorize(ipaddress.ip_address(raw)) is category


@pytest.mark.parametrize(
    "raw", ["93.184.215.14", "8.8.8.8", "1.1.1.1", "2606:4700::1111", "::ffff:8.8.8.8"]
)
def test_public_addresses_are_allowed(raw: str) -> None:
    assert categorize(ipaddress.ip_address(raw)) is None
    assert str(STRICT.check(raw)) == str(ipaddress.ip_address(raw))


@pytest.mark.parametrize("raw", ["127.0.0.1", "10.0.0.1", "100.64.0.1", "::1", "fd00::1"])
def test_private_opt_in_allows_loopback_private_and_cgnat(raw: str) -> None:
    with pytest.raises(DestinationBlockedError):
        STRICT.check(raw)
    assert DEV.check(raw) == ipaddress.ip_address(raw)


@pytest.mark.parametrize(
    "raw",
    [
        "169.254.169.254",
        "::ffff:169.254.169.254",
        "169.254.1.1",
        "0.0.0.0",  # noqa: S104
        "224.0.0.1",
        "255.255.255.255",
        "fe80::1",
    ],
)
def test_metadata_and_link_local_stay_blocked_even_in_dev(raw: str) -> None:
    with pytest.raises(DestinationBlockedError) as info:
        DEV.check(raw)
    assert info.value.address is not None


def test_scoped_and_garbage_addresses_are_refused() -> None:
    with pytest.raises(DestinationBlockedError, match="scoped"):
        DEV.check("fe80::1%eth0")
    with pytest.raises(DestinationBlockedError, match="unparseable"):
        DEV.check("not-an-ip")
    err = DestinationBlockedError("nope")
    assert str(err) == "nope"
    assert err.address is None


# -- backend ---------------------------------------------------------------------------------


class _RecordingBackend(httpcore.AsyncNetworkBackend):
    def __init__(self, fail_for: set[str] | None = None) -> None:
        self.connected: list[str] = []
        self.slept: list[float] = []
        self.fail_for = fail_for or set()

    async def connect_tcp(self, host, port, timeout=None, local_address=None, socket_options=None):  # noqa: ASYNC109
        self.connected.append(host)
        if host in self.fail_for:
            raise httpcore.ConnectError("refused")
        return object()

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)


def _resolver(answers: dict[str, list[str]]):
    calls: list[str] = []

    async def resolve(host: str, port: int) -> list[str]:
        calls.append(host)
        return answers[host]

    resolve.calls = calls  # type: ignore[attr-defined]
    return resolve


async def test_backend_connects_to_the_vetted_ip_not_the_name() -> None:
    inner = _RecordingBackend()
    backend = GuardedNetworkBackend(
        STRICT, resolver=_resolver({"hooks.example": ["93.184.215.14"]}), inner=inner
    )
    await backend.connect_tcp("hooks.example", 443, timeout=1)
    assert inner.connected == ["93.184.215.14"]


async def test_backend_refuses_if_any_resolved_address_is_blocked() -> None:
    """DNS rebinding / mixed answers: one internal address poisons the whole answer."""
    inner = _RecordingBackend()
    backend = GuardedNetworkBackend(
        STRICT,
        resolver=_resolver({"evil.example": ["93.184.215.14", "169.254.169.254"]}),
        inner=inner,
    )
    with pytest.raises(DestinationBlockedError, match="metadata"):
        await backend.connect_tcp("evil.example", 443)
    assert inner.connected == []


async def test_backend_resolves_on_every_connection() -> None:
    """The name is re-resolved and re-vetted per connection, so a rebinding is caught."""
    answers = {"flip.example": ["93.184.215.14"]}
    resolver = _resolver(answers)
    inner = _RecordingBackend()
    backend = GuardedNetworkBackend(STRICT, resolver=resolver, inner=inner)
    await backend.connect_tcp("flip.example", 443)
    answers["flip.example"] = ["127.0.0.1"]
    with pytest.raises(DestinationBlockedError, match="loopback"):
        await backend.connect_tcp("flip.example", 443)
    assert resolver.calls == ["flip.example", "flip.example"]


async def test_backend_vets_literal_ips_without_dns() -> None:
    resolver = _resolver({})
    backend = GuardedNetworkBackend(STRICT, resolver=resolver, inner=_RecordingBackend())
    with pytest.raises(DestinationBlockedError):
        await backend.connect_tcp("127.0.0.1", 80)
    with pytest.raises(DestinationBlockedError):
        await backend.connect_tcp("[::1]", 80)
    assert resolver.calls == []


async def test_backend_falls_back_to_the_next_address() -> None:
    inner = _RecordingBackend(fail_for={"93.184.215.14"})
    backend = GuardedNetworkBackend(
        STRICT, resolver=_resolver({"h.example": ["93.184.215.14", "1.1.1.1"]}), inner=inner
    )
    await backend.connect_tcp("h.example", 443)
    assert inner.connected == ["93.184.215.14", "1.1.1.1"]


async def test_backend_raises_the_last_connect_error() -> None:
    inner = _RecordingBackend(fail_for={"1.1.1.1"})
    backend = GuardedNetworkBackend(
        STRICT, resolver=_resolver({"h.example": ["1.1.1.1"]}), inner=inner
    )
    with pytest.raises(httpcore.ConnectError):
        await backend.connect_tcp("h.example", 443)


async def test_backend_empty_answer_and_dns_timeout() -> None:
    backend = GuardedNetworkBackend(STRICT, resolver=_resolver({"e.example": []}))
    with pytest.raises(ResolutionError, match="no addresses"):
        await backend.connect_tcp("e.example", 443)

    async def slow(host: str, port: int) -> list[str]:
        await asyncio.sleep(5)
        return []  # pragma: no cover

    backend = GuardedNetworkBackend(STRICT, resolver=slow)
    with pytest.raises(httpcore.ConnectTimeout, match="DNS"):
        await backend.connect_tcp("slow.example", 443, timeout=0.01)


async def test_backend_refuses_unix_sockets_and_delegates_sleep() -> None:
    inner = _RecordingBackend()
    backend = GuardedNetworkBackend(DEV, inner=inner)
    with pytest.raises(DestinationBlockedError, match="unix"):
        await backend.connect_unix_socket("/var/run/docker.sock")
    await backend.sleep(0.5)
    assert inner.slept == [0.5]


async def test_system_resolver_dedupes_answers() -> None:
    assert await system_resolver("127.0.0.1", 80) == ["127.0.0.1"]


async def test_system_resolver_maps_resolution_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = asyncio.get_running_loop()

    async def fail(*_args: object, **_kwargs: object) -> list[object]:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(loop, "getaddrinfo", fail)
    with pytest.raises(ResolutionError, match="could not resolve"):
        await system_resolver("does-not-exist.invalid", 80)
