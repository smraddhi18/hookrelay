"""SSRF guard for outbound webhooks: resolve, vet every address, then connect to a vetted IP.

Endpoint URLs are tenant-controlled, so without a guard a tenant could make the worker call
``http://169.254.169.254/`` (cloud credentials), ``http://localhost:5432`` or anything else
inside the VPC. URL validation at registration time (Milestone 1) is not enough because DNS can
change afterwards ("DNS rebinding": the name resolves to a public IP when checked and to
``127.0.0.1`` when used). This module closes that gap at **connect time**:

1. the worker's HTTP connection pool uses :class:`GuardedNetworkBackend`, whose ``connect_tcp``
   resolves the host itself;
2. **every** returned address is vetted (one bad record fails the attempt, so a mixed answer
   cannot be used to race the check);
3. the socket connects to the vetted IP literal, so there is no second resolution to rebind.

The URL, ``Host`` header and TLS SNI / certificate check all keep using the hostname (httpcore
passes ``server_hostname`` from the request URL to ``start_tls``), so HTTPS still verifies the
receiver's certificate. Redirects are never followed by the client, so a 3xx cannot bounce the
worker to an internal address either.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

import httpcore

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
Resolver = Callable[[str, int], Awaitable[list[str]]]


class AddressCategory(StrEnum):
    UNSPECIFIED = "unspecified"
    LOOPBACK = "loopback"
    PRIVATE = "private"  # RFC 1918, IPv6 unique-local
    SHARED = "shared"  # RFC 6598 carrier-grade NAT (100.64.0.0/10)
    LINK_LOCAL = "link_local"
    METADATA = "metadata"  # cloud instance-metadata services
    MULTICAST = "multicast"
    BROADCAST = "broadcast"
    RESERVED = "reserved"  # documentation, benchmarking, IETF protocol, future use, ...


# Categories DELIVERY_ALLOW_PRIVATE_NETWORKS unlocks (local receivers in tests/dev). Metadata,
# link-local, multicast, broadcast, unspecified and reserved ranges stay blocked regardless.
_PRIVATE_OPT_IN: Final = frozenset(
    {AddressCategory.LOOPBACK, AddressCategory.PRIVATE, AddressCategory.SHARED}
)

_METADATA_ADDRESSES: Final = frozenset(
    {
        ipaddress.ip_address("169.254.169.254"),  # AWS, GCP, Azure, OpenStack, DigitalOcean
        ipaddress.ip_address("169.254.170.2"),  # AWS ECS task metadata
        ipaddress.ip_address("100.100.100.200"),  # Alibaba Cloud
        ipaddress.ip_address("fd00:ec2::254"),  # AWS IMDS over IPv6
    }
)

_V4_RANGES: Final[tuple[tuple[ipaddress.IPv4Network, AddressCategory], ...]] = tuple(
    (ipaddress.IPv4Network(net), cat)
    for net, cat in (
        ("0.0.0.0/8", AddressCategory.UNSPECIFIED),
        ("10.0.0.0/8", AddressCategory.PRIVATE),
        ("100.64.0.0/10", AddressCategory.SHARED),
        ("127.0.0.0/8", AddressCategory.LOOPBACK),
        ("169.254.0.0/16", AddressCategory.LINK_LOCAL),
        ("172.16.0.0/12", AddressCategory.PRIVATE),
        ("192.0.0.0/24", AddressCategory.RESERVED),
        ("192.0.2.0/24", AddressCategory.RESERVED),
        ("192.88.99.0/24", AddressCategory.RESERVED),
        ("192.168.0.0/16", AddressCategory.PRIVATE),
        ("198.18.0.0/15", AddressCategory.RESERVED),
        ("198.51.100.0/24", AddressCategory.RESERVED),
        ("203.0.113.0/24", AddressCategory.RESERVED),
        ("224.0.0.0/4", AddressCategory.MULTICAST),
        ("255.255.255.255/32", AddressCategory.BROADCAST),
        ("240.0.0.0/4", AddressCategory.RESERVED),
    )
)

_V6_RANGES: Final[tuple[tuple[ipaddress.IPv6Network, AddressCategory], ...]] = tuple(
    (ipaddress.IPv6Network(net), cat)
    for net, cat in (
        ("::/128", AddressCategory.UNSPECIFIED),
        ("::1/128", AddressCategory.LOOPBACK),
        ("::/96", AddressCategory.RESERVED),  # deprecated IPv4-compatible
        ("64:ff9b:1::/48", AddressCategory.RESERVED),  # local-use NAT64
        ("100::/64", AddressCategory.RESERVED),  # discard-only
        ("2001::/23", AddressCategory.RESERVED),  # IETF protocol assignments incl. Teredo
        ("2001:db8::/32", AddressCategory.RESERVED),  # documentation
        ("3fff::/20", AddressCategory.RESERVED),  # documentation (RFC 9637)
        ("fc00::/7", AddressCategory.PRIVATE),  # unique local
        ("fe80::/10", AddressCategory.LINK_LOCAL),
        ("fec0::/10", AddressCategory.RESERVED),  # deprecated site-local
        ("ff00::/8", AddressCategory.MULTICAST),
    )
)
_NAT64: Final = ipaddress.IPv6Network("64:ff9b::/96")


class DestinationBlockedError(Exception):
    """The destination is not allowed (scheme, port or a resolved address). Never retried."""

    def __init__(self, reason: str, *, address: str | None = None) -> None:
        super().__init__(reason if address is None else f"{reason} ({address})")
        self.reason = reason
        self.address = address


class ResolutionError(Exception):
    """The host name did not resolve to any address."""


def _embedded_ipv4(address: ipaddress.IPv6Address) -> ipaddress.IPv4Address | None:
    """The IPv4 address carried by an IPv4-mapped, NAT64 (64:ff9b::/96) or 6to4 address."""
    if address.ipv4_mapped is not None:
        return address.ipv4_mapped
    if address in _NAT64:
        return ipaddress.IPv4Address(int(address) & 0xFFFFFFFF)
    return address.sixtofour


def categorize(address: IPAddress) -> AddressCategory | None:
    """The blocked category of ``address``, or ``None`` if it is a public unicast address."""
    if address in _METADATA_ADDRESSES:
        return AddressCategory.METADATA
    ranges: Iterable[tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, AddressCategory]]
    if isinstance(address, ipaddress.IPv6Address):
        # Addresses that embed an IPv4 address are judged by the embedded address, so
        # ``::ffff:127.0.0.1``, ``64:ff9b::a9fe:a9fe`` or ``2002:7f00:1::`` cannot sneak through.
        embedded = _embedded_ipv4(address)
        if embedded is not None:
            return categorize(embedded)
        ranges = _V6_RANGES
    else:
        ranges = _V4_RANGES
    category = next((cat for network, cat in ranges if address in network), None)
    # Backstop for anything IANA does not list as globally reachable.
    if category is None and not address.is_global:
        category = AddressCategory.RESERVED
    return category


@dataclass(frozen=True, slots=True)
class AddressPolicy:
    allow_private_networks: bool = False

    def check(self, raw: str) -> IPAddress:
        """Parse and vet one resolved address; raise ``DestinationBlockedError`` if refused."""
        try:
            address = ipaddress.ip_address(raw.split("%", 1)[0])
        except ValueError as exc:
            raise DestinationBlockedError("unparseable address", address=raw) from exc
        if "%" in raw:  # zone index: only meaningful for link-local, never a webhook target
            raise DestinationBlockedError("scoped address", address=raw)
        category = categorize(address)
        if category is None:
            return address
        if self.allow_private_networks and category in _PRIVATE_OPT_IN:
            return address
        raise DestinationBlockedError(f"{category.value} address", address=str(address))


async def system_resolver(host: str, port: int) -> list[str]:
    """All addresses ``host`` resolves to (stream sockets), deduplicated in resolver order."""
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise ResolutionError(f"could not resolve {host!r}: {exc.strerror}") from exc
    seen: dict[str, None] = {}
    for _family, _type, _proto, _canon, sockaddr in infos:
        seen.setdefault(str(sockaddr[0]), None)
    return list(seen)


class GuardedNetworkBackend(httpcore.AsyncNetworkBackend):
    """httpcore network backend that only ever connects to addresses the policy allows."""

    def __init__(
        self,
        policy: AddressPolicy,
        *,
        resolver: Resolver = system_resolver,
        inner: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        self._policy = policy
        self._resolver = resolver
        self._inner = inner or httpcore.AnyIOBackend()

    async def resolve(self, host: str, port: int, timeout_s: float | None) -> list[IPAddress]:
        try:
            literal = ipaddress.ip_address(host.strip("[]"))
        except ValueError:
            literal = None
        if literal is not None:
            raw_addresses = [str(literal)]
        else:
            try:
                async with asyncio.timeout(timeout_s):
                    raw_addresses = await self._resolver(host, port)
            except TimeoutError as exc:
                raise httpcore.ConnectTimeout(f"DNS resolution of {host!r} timed out") from exc
        if not raw_addresses:
            raise ResolutionError(f"{host!r} resolved to no addresses")
        return [self._policy.check(raw) for raw in raw_addresses]

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109 - signature fixed by httpcore
        local_address: str | None = None,
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        addresses = await self.resolve(host, port, timeout)
        last_error: Exception | None = None
        for address in addresses:
            try:
                return await self._inner.connect_tcp(
                    str(address),
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except (httpcore.ConnectError, httpcore.ConnectTimeout) as exc:
                last_error = exc
        assert last_error is not None  # noqa: S101 - addresses is never empty here
        raise last_error

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109 - signature fixed by httpcore
        socket_options: Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise DestinationBlockedError("unix sockets are not webhook destinations")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)
