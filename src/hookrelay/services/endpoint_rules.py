"""Pure validation rules for endpoint configuration (no I/O, unit-testable in isolation).

URL checks here are an early, friendly rejection at configuration time. They are *not* the
SSRF defence: a hostname can resolve to a private address later (DNS rebinding), so the
delivery worker (Milestone 3) resolves, vets and pins the IP on every connection.
"""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass
from typing import Final
from urllib.parse import SplitResult, urlsplit

from hookrelay.db.models.endpoint import MAX_URL_LENGTH

MAX_CUSTOM_HEADERS: Final = 20
MAX_HEADER_NAME_LENGTH: Final = 64
MAX_HEADER_VALUE_LENGTH: Final = 1_024
MAX_HEADERS_TOTAL_BYTES: Final = 8_192

# RFC 9110 token characters.
_HEADER_NAME_RE: Final = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")
# Visible ASCII + space/tab; no CR/LF/NUL (header injection), no leading/trailing whitespace.
_HEADER_VALUE_RE: Final = re.compile(r"^[\x21-\x7e]([\x20-\x7e\t]*[\x21-\x7e])?$")

# Hop-by-hop headers (RFC 9110 §7.6.1), framing/routing headers the HTTP client must own, and
# headers that carry hookrelay's own security guarantees (signature, idempotency, tracing).
DENIED_HEADERS: Final = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "content-type",
        "content-encoding",
        "expect",
        "cookie",
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
        "x-real-ip",
        "user-agent",
        "webhook-id",
        "webhook-timestamp",
        "webhook-signature",
        "traceparent",
        "tracestate",
    }
)
DENIED_HEADER_PREFIXES: Final = ("x-hookrelay-", "proxy-", "sec-", "webhook-")

_LOCAL_HOSTNAMES: Final = ("localhost", "localhost.localdomain")
_LOCAL_SUFFIXES: Final = (".localhost", ".local", ".internal", ".home.arpa")


@dataclass(frozen=True, slots=True)
class UrlPolicy:
    allow_http: bool = False
    allow_private_networks: bool = False


def _host_is_private(host: str) -> bool:
    lowered = host.lower().rstrip(".")
    if lowered in _LOCAL_HOSTNAMES or lowered.endswith(_LOCAL_SUFFIXES):
        return True
    try:
        address = ipaddress.ip_address(lowered)
    except ValueError:
        return "." not in lowered  # single-label names resolve via the local search domain
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        address = address.ipv4_mapped
    return not address.is_global


def _split(url: str) -> SplitResult:
    try:
        parts = urlsplit(url)
        _ = parts.port  # raises ValueError on a non-numeric / out-of-range port
    except ValueError as exc:
        raise ValueError("URL is not valid") from exc
    return parts


def validate_endpoint_url(raw: str, policy: UrlPolicy) -> str:
    """Return the normalised URL or raise ``ValueError`` with a client-safe message."""
    url = raw.strip()
    if not url or len(url) > MAX_URL_LENGTH:
        raise ValueError(f"URL must be 1 to {MAX_URL_LENGTH} characters")
    if any(ord(c) < 0x21 or ord(c) > 0x7E for c in url):  # noqa: PLR2004 - printable ASCII
        raise ValueError("URL must be printable ASCII without spaces (percent-encode the rest)")
    parts = _split(url)
    scheme = parts.scheme.lower()
    if scheme not in ("https", "http"):
        raise ValueError("URL scheme must be https")
    if scheme == "http" and not policy.allow_http:
        raise ValueError("URL scheme must be https (plain http is disabled)")
    if parts.username is not None or parts.password is not None:
        raise ValueError("URL must not contain credentials; use a custom header instead")
    if parts.fragment:
        raise ValueError("URL must not contain a fragment")
    host = parts.hostname
    if not host:
        raise ValueError("URL must include a host")
    if not policy.allow_private_networks and _host_is_private(host):
        raise ValueError("URL must point to a public host (private/loopback targets are blocked)")
    netloc = parts.netloc.lower()  # no userinfo left, so this only folds host case
    return SplitResult(scheme, netloc, parts.path or "/", parts.query, "").geturl()


def validate_custom_headers(headers: dict[str, str]) -> dict[str, str]:
    """Validate tenant-supplied delivery headers; raise ``ValueError`` on the first problem."""
    if len(headers) > MAX_CUSTOM_HEADERS:
        raise ValueError(f"At most {MAX_CUSTOM_HEADERS} custom headers are allowed")
    seen: set[str] = set()
    total = 0
    for name, value in headers.items():
        lowered = name.lower()
        if not (0 < len(name) <= MAX_HEADER_NAME_LENGTH) or not _HEADER_NAME_RE.fullmatch(name):
            raise ValueError(f"Header name {name!r} is not a valid HTTP token")
        if lowered in DENIED_HEADERS or lowered.startswith(DENIED_HEADER_PREFIXES):
            raise ValueError(f"Header {name!r} is reserved and cannot be overridden")
        if lowered in seen:
            raise ValueError(f"Header {name!r} is duplicated (names are case-insensitive)")
        seen.add(lowered)
        if len(value) > MAX_HEADER_VALUE_LENGTH or not _HEADER_VALUE_RE.fullmatch(value):
            raise ValueError(
                f"Header {name!r} value must be 1 to {MAX_HEADER_VALUE_LENGTH} printable ASCII "
                "characters without line breaks"
            )
        total += len(name) + len(value)
    if total > MAX_HEADERS_TOTAL_BYTES:
        raise ValueError(f"Custom headers exceed {MAX_HEADERS_TOTAL_BYTES} bytes in total")
    return dict(headers)


def mask_headers(headers: dict[str, str]) -> dict[str, str]:
    """Header values often carry receiver credentials; only admins see them unmasked."""
    return dict.fromkeys(headers, "********")
