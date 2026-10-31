from __future__ import annotations

import base64
import re

import pytest

from hookrelay.core.security import (
    DUMMY_API_KEY_DIGEST,
    SecretBox,
    SecretDecryptionError,
    api_key_matches,
    generate_api_key,
    generate_signing_secret,
    hash_api_key,
    operator_token_matches,
    parse_api_key_prefix,
)
from tests.conftest import TEST_ENCRYPTION_KEY, make_settings

KEY_A = bytes(range(32))
KEY_B = bytes(range(32, 64))


def test_generated_api_key_shape_and_digest() -> None:
    key = generate_api_key()
    assert re.fullmatch(r"hrk_[a-z0-9]{12}_[A-Za-z0-9_-]{43}", key.plaintext)
    assert key.plaintext.split("_")[1] == key.prefix
    assert key.digest == hash_api_key(key.plaintext)
    assert len(key.digest) == 32
    assert api_key_matches(key.plaintext, key.digest)
    assert not api_key_matches(key.plaintext + "x", key.digest)


def test_generated_keys_are_unique() -> None:
    keys = {generate_api_key().plaintext for _ in range(200)}
    assert len(keys) == 200


@pytest.mark.parametrize(
    "candidate",
    [
        "",
        "hrk_",
        "Bearer hrk_abc",
        "hrk_ABCDEFGHIJKL_" + "a" * 43,  # uppercase prefix
        "hrk_abcdefghijk_" + "a" * 43,  # 11-char prefix
        "hrk_abcdefghijkl_" + "a" * 42,  # short secret
        "hrk_abcdefghijkl_" + "a" * 44,  # long secret
        "xyz_abcdefghijkl_" + "a" * 43,
        "hrk_abcdefghijkl_" + "a" * 42 + "!",
        "hrk_abcdefghijkl_" + "a" * 43 + "\n",
    ],
)
def test_parse_rejects_malformed_keys(candidate: str) -> None:
    assert parse_api_key_prefix(candidate) is None


def test_parse_extracts_prefix() -> None:
    key = generate_api_key()
    assert parse_api_key_prefix(key.plaintext) == key.prefix


def test_dummy_digest_never_matches_a_real_key() -> None:
    assert len(DUMMY_API_KEY_DIGEST) == 32
    assert not api_key_matches(generate_api_key().plaintext, DUMMY_API_KEY_DIGEST)


def test_operator_token_comparison() -> None:
    token = "x" * 40
    assert operator_token_matches(token, token)
    assert not operator_token_matches(token[:-1], token)
    assert not operator_token_matches("", token)


def test_signing_secret_format() -> None:
    secret = generate_signing_secret()
    assert secret.startswith("whsec_")
    assert len(base64.b64decode(secret.removeprefix("whsec_"))) == 32
    assert generate_signing_secret() != secret


def test_secret_box_round_trip_is_randomised() -> None:
    box = SecretBox(KEY_A)
    first = box.encrypt("whsec_hello", aad=b"t:e")
    second = box.encrypt("whsec_hello", aad=b"t:e")
    assert first != second  # fresh nonce every time
    assert first.startswith(f"v1.{box.primary_key_id}.")
    assert "whsec_hello" not in first
    assert box.decrypt(first, aad=b"t:e") == "whsec_hello"


def test_secret_box_binds_associated_data() -> None:
    box = SecretBox(KEY_A)
    token = box.encrypt("s", aad=b"tenant-1:endpoint-1")
    with pytest.raises(SecretDecryptionError):
        box.decrypt(token, aad=b"tenant-2:endpoint-1")


def test_secret_box_rejects_tampering_and_unknown_keys() -> None:
    box = SecretBox(KEY_A)
    token = box.encrypt("s", aad=b"a")
    version, kid, body = token.split(".")
    flipped = body[:-2] + ("A" if body[-2] != "A" else "B") + body[-1]
    for bad in (
        f"{version}.{kid}.{flipped}",
        f"v2.{kid}.{body}",
        f"{version}.deadbeef.{body}",
        f"{version}.{kid}.",
        f"{version}.{kid}.@@@",
        "garbage",
    ):
        with pytest.raises(SecretDecryptionError):
            box.decrypt(bad, aad=b"a")
    with pytest.raises(SecretDecryptionError):
        SecretBox(KEY_B).decrypt(token, aad=b"a")


def test_secret_box_key_rotation() -> None:
    old_box = SecretBox(KEY_A)
    token = old_box.encrypt("s", aad=b"a")
    rotated = SecretBox(KEY_B, previous=KEY_A)
    assert rotated.decrypt(token, aad=b"a") == "s"  # old ciphertexts stay readable
    fresh = rotated.encrypt("s", aad=b"a")
    assert fresh.split(".")[1] == rotated.primary_key_id != old_box.primary_key_id
    assert SecretBox(KEY_B).decrypt(fresh, aad=b"a") == "s"


def test_secret_box_from_settings() -> None:
    configured = SecretBox.from_settings(make_settings(secrets_encryption_key=TEST_ENCRYPTION_KEY))
    dev = SecretBox.from_settings(make_settings())
    assert configured.primary_key_id != dev.primary_key_id
    with_previous = SecretBox.from_settings(
        make_settings(
            secrets_encryption_key=base64.urlsafe_b64encode(KEY_B).decode(),
            secrets_encryption_key_previous=TEST_ENCRYPTION_KEY,
        )
    )
    token = configured.encrypt("s", aad=b"x")
    assert with_previous.decrypt(token, aad=b"x") == "s"
