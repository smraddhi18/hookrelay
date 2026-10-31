from __future__ import annotations

import pytest

from hookrelay.services.endpoint_rules import (
    MAX_CUSTOM_HEADERS,
    UrlPolicy,
    mask_headers,
    validate_custom_headers,
    validate_endpoint_url,
)

STRICT = UrlPolicy()
LAX = UrlPolicy(allow_http=True, allow_private_networks=True)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://hooks.example.com/in", "https://hooks.example.com/in"),
        ("HTTPS://Hooks.Example.COM/In?x=1", "https://hooks.example.com/In?x=1"),
        ("https://hooks.example.com", "https://hooks.example.com/"),
        ("  https://example.com:8443/a  ", "https://example.com:8443/a"),
        ("https://8.8.8.8/hook", "https://8.8.8.8/hook"),
        ("https://[2606:4700::1111]/x", "https://[2606:4700::1111]/x"),
    ],
)
def test_accepts_and_normalises_public_https_urls(raw: str, expected: str) -> None:
    assert validate_endpoint_url(raw, STRICT) == expected


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        ("", "1 to"),
        ("https://" + "a" * 2050 + ".com", "1 to"),
        ("http://example.com/x", "plain http"),
        ("ftp://example.com/x", "scheme"),
        ("javascript:alert(1)", "scheme"),
        ("https://user:pw@example.com/", "credentials"),
        ("https://example.com/#frag", "fragment"),
        ("https:///path", "host"),
        ("https://example.com:99999/", "not valid"),
        ("https://exa mple.com/", "printable"),
        ("https://example.com/ü", "printable"),
        ("https://localhost/x", "public"),
        ("https://api.localhost/x", "public"),
        ("https://printer.local/x", "public"),
        ("https://intranet/x", "public"),
        ("https://127.0.0.1/x", "public"),
        ("https://10.1.2.3/x", "public"),
        ("https://192.168.0.10/x", "public"),
        ("https://169.254.169.254/latest/meta-data", "public"),
        ("https://[::1]/x", "public"),
        ("https://[::ffff:127.0.0.1]/x", "public"),
        ("https://0.0.0.0/x", "public"),
    ],
)
def test_rejects_unsafe_urls(raw: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_endpoint_url(raw, STRICT)


def test_relaxed_policy_allows_http_and_private_targets() -> None:
    assert validate_endpoint_url("http://localhost:9000/in", LAX) == "http://localhost:9000/in"
    assert validate_endpoint_url("https://10.0.0.5/x", LAX) == "https://10.0.0.5/x"
    with pytest.raises(ValueError, match="credentials"):
        validate_endpoint_url("http://u@localhost/", LAX)


def test_valid_headers_pass_through() -> None:
    headers = {"X-Receiver-Token": "abc 123", "Authorization": "Bearer t0k3n"}
    assert validate_custom_headers(headers) == headers
    assert validate_custom_headers({}) == {}


@pytest.mark.parametrize(
    "name",
    [
        "Connection",
        "keep-alive",
        "Transfer-Encoding",
        "TE",
        "Upgrade",
        "Host",
        "Content-Length",
        "content-type",
        "Proxy-Authorization",
        "Proxy-Anything",
        "Webhook-Signature",
        "webhook-id",
        "Webhook-Custom",
        "X-Hookrelay-Attempt",
        "Sec-Fetch-Mode",
        "Cookie",
        "X-Forwarded-For",
        "traceparent",
    ],
)
def test_reserved_headers_are_rejected(name: str) -> None:
    with pytest.raises(ValueError, match="reserved"):
        validate_custom_headers({name: "v"})


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ({"Bad Name": "v"}, "valid HTTP token"),
        ({"": "v"}, "valid HTTP token"),
        ({"X-" + "a" * 70: "v"}, "valid HTTP token"),
        ({"X-A": "line\r\nInjected: 1"}, "printable"),
        ({"X-A": "nul\x00"}, "printable"),
        ({"X-A": ""}, "printable"),
        ({"X-A": " padded"}, "printable"),
        ({"X-A": "é"}, "printable"),
        ({"X-A": "a" * 1025}, "printable"),
        ({"X-A": "1", "x-a": "2"}, "duplicated"),
        ({f"X-H{i}": "v" for i in range(MAX_CUSTOM_HEADERS + 1)}, "At most"),
        ({f"X-H{i}": "v" * 1000 for i in range(9)}, "in total"),
    ],
)
def test_invalid_headers(headers: dict[str, str], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        validate_custom_headers(headers)


def test_mask_headers_hides_values_only() -> None:
    assert mask_headers({"X-Token": "secret"}) == {"X-Token": "********"}
