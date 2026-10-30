from __future__ import annotations

import pytest
from pydantic import ValidationError

from hookrelay.core.config import Environment, LogFormat, Settings, normalize_database_url
from tests.conftest import make_settings


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
    s = Settings(_env_file=None)
    assert s.database_dsn == "postgresql+asyncpg://a:b@db:5432/x"
    assert s.db_pool_size == 25
    assert s.is_production
    assert s.redis_url == "redis://cache:6379/0"


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
        make_settings(app_env="production", **overrides)


def test_production_with_safe_values_is_accepted() -> None:
    assert make_settings(app_env="production", log_format="json").is_production


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
