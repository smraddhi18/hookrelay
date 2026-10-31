"""Credential primitives: tenant API keys, the operator token, endpoint signing secrets and
AES-256-GCM encryption of secrets at rest.

API keys look like ``hrk_<prefix>_<secret>``: a 12-char public prefix used as the lookup key
(unique index) and 256 bits of randomness. Only ``SHA-256(full key)`` is stored. A fast hash is
the right tool here (unlike passwords): the input is uniformly random and long, so there is
nothing for a slow KDF to protect against, and auth sits on every request's hot path.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import os
import re
import secrets
import string
from dataclasses import dataclass
from typing import Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from hookrelay.core.config import Settings, decode_encryption_key

API_KEY_SCHEME: Final = "hrk"
API_KEY_PREFIX_LENGTH: Final = 12
_PREFIX_ALPHABET: Final = string.ascii_lowercase + string.digits
_API_KEY_RE: Final = re.compile(r"^hrk_([a-z0-9]{12})_([A-Za-z0-9_-]{43})$")
SIGNING_SECRET_SCHEME: Final = "whsec_"  # noqa: S105 - format prefix, not a secret
_SIGNING_SECRET_BYTES: Final = 32
_NONCE_BYTES: Final = 12
_CIPHERTEXT_VERSION: Final = "v1"
# Deterministic, publicly known key used only outside production when no key is configured.
_DEV_KEY: Final = hashlib.sha256(b"hookrelay-insecure-development-key").digest()


@dataclass(frozen=True, slots=True)
class GeneratedApiKey:
    plaintext: str  # returned to the caller exactly once, never stored or logged
    prefix: str
    digest: bytes


def hash_api_key(plaintext: str) -> bytes:
    return hashlib.sha256(plaintext.encode("utf-8")).digest()


def generate_api_key() -> GeneratedApiKey:
    prefix = "".join(secrets.choice(_PREFIX_ALPHABET) for _ in range(API_KEY_PREFIX_LENGTH))
    plaintext = f"{API_KEY_SCHEME}_{prefix}_{secrets.token_urlsafe(32)}"
    return GeneratedApiKey(plaintext=plaintext, prefix=prefix, digest=hash_api_key(plaintext))


def parse_api_key_prefix(candidate: str) -> str | None:
    """Return the lookup prefix of a well-formed key, or ``None`` (cheap pre-DB rejection)."""
    match = _API_KEY_RE.fullmatch(candidate)
    return match.group(1) if match else None


def api_key_matches(plaintext: str, digest: bytes) -> bool:
    return hmac.compare_digest(hash_api_key(plaintext), digest)


# Compared against when the prefix is unknown, so "no such key" costs the same as "wrong key".
DUMMY_API_KEY_DIGEST: Final = hash_api_key("hrk_000000000000_" + "0" * 43)


def operator_token_matches(presented: str, expected: str) -> bool:
    """Constant-time comparison of fixed-length digests: leaks neither content nor length."""
    return hmac.compare_digest(
        hashlib.sha256(presented.encode("utf-8")).digest(),
        hashlib.sha256(expected.encode("utf-8")).digest(),
    )


def generate_signing_secret() -> str:
    """Standard Webhooks format: ``whsec_`` + base64(32 random bytes)."""
    return SIGNING_SECRET_SCHEME + base64.b64encode(os.urandom(_SIGNING_SECRET_BYTES)).decode()


class SecretDecryptionError(Exception):
    """Ciphertext is malformed, was produced by an unknown key, or failed authentication."""


def _key_id(key: bytes) -> str:
    return hashlib.sha256(b"hookrelay-kid:" + key).hexdigest()[:8]


class SecretBox:
    """AES-256-GCM with a key id in every ciphertext so keys can be rotated.

    Format: ``v1.<key id>.<base64url(nonce || ciphertext+tag)>``. Callers pass associated data
    (e.g. tenant id + endpoint id) that is authenticated but not stored, so a ciphertext copied
    onto another row fails to decrypt instead of silently signing with someone else's secret.
    """

    def __init__(self, primary: bytes, previous: bytes | None = None) -> None:
        self._primary_id = _key_id(primary)
        self._keys: dict[str, AESGCM] = {self._primary_id: AESGCM(primary)}
        if previous is not None:
            self._keys.setdefault(_key_id(previous), AESGCM(previous))

    @classmethod
    def from_settings(cls, settings: Settings) -> SecretBox:
        if settings.secrets_encryption_key is None:
            # Settings already refuses this combination in production.
            return cls(_DEV_KEY)
        primary = decode_encryption_key(settings.secrets_encryption_key.get_secret_value())
        previous = (
            decode_encryption_key(settings.secrets_encryption_key_previous.get_secret_value())
            if settings.secrets_encryption_key_previous is not None
            else None
        )
        return cls(primary, previous)

    @property
    def primary_key_id(self) -> str:
        return self._primary_id

    def encrypt(self, plaintext: str, *, aad: bytes) -> str:
        nonce = os.urandom(_NONCE_BYTES)
        sealed = self._keys[self._primary_id].encrypt(nonce, plaintext.encode("utf-8"), aad)
        body = base64.urlsafe_b64encode(nonce + sealed).decode().rstrip("=")
        return f"{_CIPHERTEXT_VERSION}.{self._primary_id}.{body}"

    def decrypt(self, token: str, *, aad: bytes) -> str:
        version, _, rest = token.partition(".")
        key_id, _, body = rest.partition(".")
        aead = self._keys.get(key_id)
        if version != _CIPHERTEXT_VERSION or aead is None or not body:
            raise SecretDecryptionError("unknown ciphertext version or key id")
        try:
            raw = base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))
            return aead.decrypt(raw[:_NONCE_BYTES], raw[_NONCE_BYTES:], aad).decode("utf-8")
        except (InvalidTag, binascii.Error, ValueError) as exc:
            raise SecretDecryptionError("ciphertext failed authentication") from exc
