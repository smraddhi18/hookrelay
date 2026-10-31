from __future__ import annotations

import base64

import pytest
from pydantic import ValidationError

from hookrelay.core.config import (
    Environment,
    LogFormat,
    Settings,
    decode_encryption_key,
    normalize_database_url,
)
from tests.conftest import TEST_ENCRYPTION_KEY, TEST_OPERATOR_TOKEN, make_settings


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Settings read the process environment; make these tests independent of the shell/CI."""
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("postgres://u:p@h:5432/d", "postgresql+asyncpg://u:p@h:5432/d"),
        ("postgresql://u:p@h/d", "postgresql+asyncpg://u:p@h/d"),
        ("postgresql+asyncpg://u:p@h/d?ssl=require", "postgresql+asyncpg://u:p@h/d?ssl=require"),
    ],
)
def test_normalize_database_url_pins_asyncpg(raw: str, expected: str) -> None:
    assert normalize_database_url(raw) == expected


@pytest.mark.parametrize(
    "raw", ["mysql://u:p@h/d", "sqlite:///x.db", "postgresql+psycopg://u@h/d", "not a url", "pg://"]
)
def test_normalize_database_url_rejects_non_postgres(raw: str) -> None:
    with pytest.raises(ValueError, match="DATABASE_URL"):
        normalize_database_url(raw)


def test_defaults_are_sane() -> None:
    s = Settings(_env_file=None)
    assert s.app_env is Environment.DEVELOPMENT
    assert s.log_format is LogFormat.JSON
    assert s.database_dsn.startswith("postgresql+asyncpg://")
    assert s.redis_url is None
    assert s.request_id_header == "X-Request-ID"


def test_reads_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgres://a:b@db:5432/x")
    monkeypatch.setenv("DB_POOL_SIZE", "25")
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/0")
    monkeypatch.setenv("SECRETS_ENCRYPTION_KEY", TEST_ENCRYPTION_KEY)
    monkeypatch.setenv("OPERATOR_TOKEN", TEST_OPERATOR_TOKEN)
    s = Settings(_env_file=None)
    assert s.database_dsn == "postgresql+asyncpg://a:b@db:5432/x"
    assert s.db_pool_size == 25
    assert s.is_production
    assert s.redis_url == "redis://cache:6379/0"
    assert s.operator_token is not None
    assert s.operator_token.get_secret_value() == TEST_OPERATOR_TOKEN


def test_database_password_is_not_in_repr() -> None:
    s = make_settings(database_url="postgresql://user:hunter2@h/d")
    assert "hunter2" not in repr(s)
    assert "hunter2" not in str(s.model_dump())
    assert "hunter2" in s.database_dsn


def test_settings_are_immutable() -> None:
    s = make_settings()
    with pytest.raises(ValidationError):
        s.port = 1  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("port", 0),
        ("port", 70000),
        ("db_pool_size", 0),
        ("db_pool_timeout_s", 0),
        ("db_statement_timeout_ms", 10),
        ("readiness_timeout_s", 0),
        ("readiness_timeout_s", 120),
        ("log_level", "TRACE"),
        ("app_env", "staging-ish"),
    ],
)
def test_rejects_out_of_range_values(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        make_settings(**{field: value})


@pytest.mark.parametrize(
    "overrides",
    [
        {"log_format": "console"},
        {"db_echo": True},
        {"log_level": "DEBUG"},
    ],
)
def test_production_guards(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError, match="production"):
        make_settings(app_env="production", secrets_encryption_key=TEST_ENCRYPTION_KEY, **overrides)


def test_production_requires_an_encryption_key() -> None:
    with pytest.raises(ValidationError, match="SECRETS_ENCRYPTION_KEY is required"):
        make_settings(app_env="production")


def test_production_with_safe_values_is_accepted() -> None:
    s = make_settings(
        app_env="production", log_format="json", secrets_encryption_key=TEST_ENCRYPTION_KEY
    )
    assert s.is_production


def test_m1_defaults() -> None:
    s = Settings(_env_file=None)
    assert s.operator_token is None
    assert s.secrets_encryption_key is None
    assert s.api_key_rotation_overlap_s == 86_400
    assert s.api_key_last_used_resolution_s == 60
    assert not s.endpoint_allow_http
    assert not s.endpoint_allow_private_networks
    assert (s.endpoint_default_timeout_ms, s.endpoint_max_timeout_ms) == (10_000, 30_000)


def test_empty_operator_token_disables_the_operator_api() -> None:
    assert make_settings(operator_token="").operator_token is None
    assert make_settings(operator_token="   ").operator_token is None


def test_short_operator_token_is_rejected() -> None:
    with pytest.raises(ValidationError, match="OPERATOR_TOKEN"):
        make_settings(operator_token="too-short")


def test_operator_token_is_not_in_repr() -> None:
    s = make_settings(operator_token=TEST_OPERATOR_TOKEN)
    assert TEST_OPERATOR_TOKEN not in repr(s)
    assert TEST_OPERATOR_TOKEN not in str(s.model_dump())


@pytest.mark.parametrize("field", ["secrets_encryption_key", "secrets_encryption_key_previous"])
@pytest.mark.parametrize(
    "value",
    ["not base64 !!", "c2hvcnQ", base64.urlsafe_b64encode(b"x" * 33).decode()],
)
def test_rejects_bad_encryption_keys(field: str, value: str) -> None:
    with pytest.raises(ValidationError, match="SECRETS_ENCRYPTION_KEY"):
        make_settings(**{field: value})


def test_accepts_padded_and_unpadded_encryption_keys() -> None:
    raw = bytes(range(32))
    padded = base64.urlsafe_b64encode(raw).decode()
    assert decode_encryption_key(padded) == raw
    assert decode_encryption_key(padded.rstrip("=")) == raw
    assert make_settings(secrets_encryption_key=padded).secrets_encryption_key is not None


def test_empty_encryption_key_means_unset() -> None:
    assert make_settings(secrets_encryption_key="").secrets_encryption_key is None


@pytest.mark.parametrize(
    "overrides",
    [
        {"endpoint_default_timeout_ms": 999},
        {"endpoint_max_timeout_ms": 60_001},
        {"endpoint_default_timeout_ms": 40_000, "endpoint_max_timeout_ms": 30_000},
        {"api_key_rotation_overlap_s": -1},
        {"api_key_rotation_overlap_s": 8 * 86_400},
        {"api_key_last_used_resolution_s": 3_601},
    ],
)
def test_rejects_bad_m1_values(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        make_settings(**overrides)


@pytest.mark.parametrize("value", ["http://cache:6379", "cache:6379"])
def test_rejects_bad_redis_url(value: str) -> None:
    with pytest.raises(ValidationError, match="REDIS_URL"):
        make_settings(redis_url=value)


def test_empty_redis_url_means_disabled() -> None:
    assert make_settings(redis_url="").redis_url is None


@pytest.mark.parametrize("value", ["", "X Request Id", "X-Request-ID\n"])
def test_rejects_invalid_request_id_header(value: str) -> None:
    with pytest.raises(ValidationError, match="REQUEST_ID_HEADER"):
        make_settings(request_id_header=value)
