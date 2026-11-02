"""Standard Webhooks signing: the published test vector, round trips and every rejection path."""

from __future__ import annotations

import base64

import pytest

from hookrelay.core import signing
from hookrelay.core.signing import WebhookVerificationError, decode_secret, sign, verify

# Test vector from the Standard Webhooks reference libraries.
VECTOR_SECRET = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
VECTOR_ID = "msg_p5jXN8AQM9LWM0D4loKWxJek"
VECTOR_TS = 1614265330
VECTOR_BODY = b'{"test": 2432232314}'
VECTOR_SIGNATURE = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE="


def _headers(signature: str = VECTOR_SIGNATURE, ts: int = VECTOR_TS) -> dict[str, str]:
    return {"webhook-id": VECTOR_ID, "webhook-timestamp": str(ts), "webhook-signature": signature}


def test_sign_matches_reference_vector() -> None:
    signed = sign(VECTOR_SECRET, VECTOR_ID, VECTOR_TS, VECTOR_BODY)
    assert signed.signature == VECTOR_SIGNATURE
    assert signed.as_dict() == _headers()


def test_verify_reference_vector_returns_id() -> None:
    assert verify(VECTOR_SECRET, _headers(), VECTOR_BODY, now=VECTOR_TS) == VECTOR_ID


def test_secret_prefix_is_optional() -> None:
    bare = VECTOR_SECRET.removeprefix("whsec_")
    assert sign(bare, VECTOR_ID, VECTOR_TS, VECTOR_BODY).signature == VECTOR_SIGNATURE


def test_header_names_are_case_insensitive() -> None:
    headers = {k.title(): v for k, v in _headers().items()}
    assert verify(VECTOR_SECRET, headers, VECTOR_BODY, now=VECTOR_TS) == VECTOR_ID


def test_any_of_several_signatures_may_match() -> None:
    headers = _headers(f"v2,abc v1,bm90LWl0 {VECTOR_SIGNATURE}")
    assert verify(VECTOR_SECRET, headers, VECTOR_BODY, now=VECTOR_TS) == VECTOR_ID


@pytest.mark.parametrize(
    ("headers", "body", "message"),
    [
        (_headers(), b'{"test": 2432232315}', "no matching signature"),
        (_headers("v1,AAAA"), VECTOR_BODY, "no matching signature"),
        (_headers(VECTOR_SIGNATURE.replace("v1", "v2")), VECTOR_BODY, "no matching signature"),
        (_headers("v1," + "A" * 5000), VECTOR_BODY, "too long"),
        ({"webhook-id": VECTOR_ID, "webhook-timestamp": "1"}, VECTOR_BODY, "missing"),
        ({**_headers(), "webhook-timestamp": "12a"}, VECTOR_BODY, "invalid webhook-timestamp"),
        ({**_headers(), "webhook-timestamp": "-5"}, VECTOR_BODY, "invalid webhook-timestamp"),
    ],
)
def test_verify_rejects(headers: dict[str, str], body: bytes, message: str) -> None:
    with pytest.raises(WebhookVerificationError, match=message):
        verify(VECTOR_SECRET, headers, body, now=VECTOR_TS)


def test_verify_rejects_a_different_secret() -> None:
    other = "whsec_" + base64.b64encode(b"another-secret-key-32-bytes-long").decode()
    with pytest.raises(WebhookVerificationError, match="no matching signature"):
        verify(other, _headers(), VECTOR_BODY, now=VECTOR_TS)


def test_verify_rejects_an_invalid_secret() -> None:
    with pytest.raises(WebhookVerificationError, match="invalid signing secret"):
        verify("whsec_***", _headers(), VECTOR_BODY, now=VECTOR_TS)


def test_timestamp_tolerance_both_directions() -> None:
    assert verify(VECTOR_SECRET, _headers(), VECTOR_BODY, now=VECTOR_TS + 300) == VECTOR_ID
    assert verify(VECTOR_SECRET, _headers(), VECTOR_BODY, now=VECTOR_TS - 300) == VECTOR_ID
    with pytest.raises(WebhookVerificationError, match="too old"):
        verify(VECTOR_SECRET, _headers(), VECTOR_BODY, now=VECTOR_TS + 301)
    with pytest.raises(WebhookVerificationError, match="future"):
        verify(VECTOR_SECRET, _headers(), VECTOR_BODY, now=VECTOR_TS - 301)
    assert (
        verify(VECTOR_SECRET, _headers(), VECTOR_BODY, now=VECTOR_TS + 1000, tolerance_s=1000)
        == VECTOR_ID
    )


def test_verify_defaults_to_wall_clock() -> None:
    with pytest.raises(WebhookVerificationError, match="too old"):
        verify(VECTOR_SECRET, _headers(), VECTOR_BODY)


@pytest.mark.parametrize("bad_id", ["", "a b", "a.b", "a,b", "a\nb"])
def test_sign_rejects_ids_that_break_the_signed_content(bad_id: str) -> None:
    with pytest.raises(ValueError, match="webhook id"):
        sign(VECTOR_SECRET, bad_id, VECTOR_TS, VECTOR_BODY)


@pytest.mark.parametrize("secret", ["whsec_not base64!", "whsec_", ""])
def test_decode_secret_rejects_bad_secrets(secret: str) -> None:
    with pytest.raises(ValueError, match="signing secret"):
        decode_secret(secret)


def test_signing_module_is_standalone() -> None:
    """Receivers may vendor the module: it must not import anything from hookrelay."""
    source = signing.__loader__.get_source(signing.__name__)  # type: ignore[union-attr]
    assert source is not None
    assert "from hookrelay" not in source
    assert "import hookrelay" not in source
