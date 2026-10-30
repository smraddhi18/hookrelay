"""Typed application settings loaded from environment variables (and an optional .env file).

Every knob has a safe default for local development except where running in production
without one would be a mistake; those are validated in :meth:`Settings._check_production`.
"""

from __future__ import annotations

from enum import StrEnum
from functools import lru_cache
from typing import Annotated, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ASYNC_DRIVER = "postgresql+asyncpg"
_ACCEPTED_SCHEMES = ("postgres", "postgresql", "postgresql+asyncpg")


class Environment(StrEnum):
    DEVELOPMENT = "development"
    TEST = "test"
    PRODUCTION = "production"


class LogFormat(StrEnum):
    JSON = "json"
    CONSOLE = "console"


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


def normalize_database_url(raw: str) -> str:
    """Accept the common ``postgres://`` / ``postgresql://`` spellings and pin the asyncpg driver.

    Anything that is not Postgres is rejected: the platform relies on Postgres-only features
    (``SKIP LOCKED``, advisory locks, partial indexes), so another backend can never work.
    """
    scheme, sep, rest = raw.partition("://")
    if not sep or not rest:
        raise ValueError("DATABASE_URL must look like postgresql+asyncpg://user:pass@host:port/db")
    if scheme not in _ACCEPTED_SCHEMES:
        raise ValueError(
            f"DATABASE_URL scheme {scheme!r} is not supported; use one of {_ACCEPTED_SCHEMES}"
        )
    return f"{_ASYNC_DRIVER}://{rest}"


PositiveFloat = Annotated[float, Field(gt=0)]


class Settings(BaseSettings):
    """Application configuration. Field names map 1:1 to upper-case environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        frozen=True,
    )

    app_name: str = "hookrelay"
    app_env: Environment = Environment.DEVELOPMENT

    # HTTP server
    host: str = "0.0.0.0"  # noqa: S104 - binding all interfaces is intended inside a container
    port: Annotated[int, Field(ge=1, le=65535)] = 8000
    docs_enabled: bool = True

    # Logging
    log_level: LogLevel = LogLevel.INFO
    log_format: LogFormat = LogFormat.JSON

    # Postgres (source of truth)
    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay"
    )
    db_pool_size: Annotated[int, Field(ge=1, le=200)] = 10
    db_max_overflow: Annotated[int, Field(ge=0, le=200)] = 5
    db_pool_timeout_s: PositiveFloat = 5.0
    db_pool_recycle_s: Annotated[int, Field(ge=60)] = 1800
    db_connect_timeout_s: PositiveFloat = 5.0
    db_statement_timeout_ms: Annotated[int, Field(ge=100, le=600_000)] = 15_000
    db_echo: bool = False

    # Readiness / shutdown
    readiness_timeout_s: Annotated[float, Field(gt=0, le=30)] = 2.0

    # Redis (not used until Milestone 6; validated now so compose/env stay honest)
    redis_url: str | None = None

    # Request correlation
    request_id_header: str = "X-Request-ID"

    @field_validator("database_url", mode="before")
    @classmethod
    def _normalize_db_url(cls, value: object) -> object:
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        if isinstance(raw, str):
            return SecretStr(normalize_database_url(raw.strip()))
        return value

    @field_validator("redis_url")
    @classmethod
    def _check_redis_url(cls, value: str | None) -> str | None:
        if value is None or value == "":
            return None
        if not value.startswith(("redis://", "rediss://")):
            raise ValueError("REDIS_URL must start with redis:// or rediss://")
        return value

    @field_validator("request_id_header")
    @classmethod
    def _check_header_name(cls, value: str) -> str:
        if not value or not all(c.isalnum() or c == "-" for c in value):
            raise ValueError("REQUEST_ID_HEADER must be a valid HTTP header name")
        return value

    @model_validator(mode="after")
    def _check_production(self) -> Self:
        if self.app_env is Environment.PRODUCTION:
            if self.log_format is not LogFormat.JSON:
                raise ValueError("LOG_FORMAT must be 'json' in production")
            if self.db_echo:
                raise ValueError("DB_ECHO must be false in production (it logs bound parameters)")
            if self.log_level is LogLevel.DEBUG:
                raise ValueError("LOG_LEVEL=DEBUG is not allowed in production")
        return self

    @property
    def is_production(self) -> bool:
        return self.app_env is Environment.PRODUCTION

    @property
    def database_dsn(self) -> str:
        """SQLAlchemy URL (``postgresql+asyncpg://...``). Never log it: it holds the password."""
        return self.database_url.get_secret_value()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton (tests build their own ``Settings`` instead)."""
    return Settings()
