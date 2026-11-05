"""Typed application settings loaded from environment variables (and an optional .env file).

Every knob has a safe default for local development except where running in production
without one would be a mistake; those are validated in :meth:`Settings._check_production`.
"""

from __future__ import annotations

import base64
import binascii
from enum import StrEnum
from functools import lru_cache
from typing import Annotated, Self

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

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
TimeoutMs = Annotated[int, Field(ge=1000, le=60_000)]

OPERATOR_TOKEN_MIN_LENGTH = 32
ENCRYPTION_KEY_BYTES = 32
# event_type, external_id, occurred_at and JSON punctuation around the payload.
EVENT_ENVELOPE_HEADROOM_BYTES = 16_384
WORKER_ID_MAX_LENGTH = 128
MAX_PORT = 65_535


def decode_encryption_key(raw: str) -> bytes:
    """Decode a base64 / base64url AES-256 key (32 bytes). Padding is optional."""
    value = raw.strip()
    try:
        key = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError) as exc:
        raise ValueError("SECRETS_ENCRYPTION_KEY must be base64-encoded") from exc
    if len(key) != ENCRYPTION_KEY_BYTES:
        raise ValueError("SECRETS_ENCRYPTION_KEY must decode to exactly 32 bytes (AES-256)")
    return key


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

    # Redis (Milestone 6). Optional: unset disables wake-ups, subscription cache and Redis
    # rate limits; Postgres paths remain correct. Features default on when the URL is set.
    redis_url: str | None = None
    redis_key_prefix: str = "hookrelay"
    redis_connect_timeout_ms: Annotated[int, Field(ge=50, le=60_000)] = 1_000
    redis_command_timeout_ms: Annotated[int, Field(ge=50, le=60_000)] = 500
    redis_reconnect_base_ms: Annotated[int, Field(ge=10, le=60_000)] = 100
    redis_reconnect_max_ms: Annotated[int, Field(ge=100, le=300_000)] = 5_000
    # When REDIS_URL is set, /ready reports a redis check. Soft by default: Redis down does
    # not make the API 503 (workers/API degrade). Set REDIS_REQUIRED_FOR_READY=true to hard-fail.
    redis_required_for_ready: bool = False
    # Worker wake-ups via pub/sub. None = default on when REDIS_URL is set.
    wakeup_enabled: bool | None = None
    wakeup_coalesce_ms: Annotated[int, Field(ge=0, le=10_000)] = 50
    # Cached fan-out membership (tenant + event_type -> endpoint ids).
    subscription_cache_enabled: bool | None = None
    subscription_cache_ttl_s: Annotated[int, Field(ge=1, le=86_400)] = 60
    # Redis Lua token bucket for per-endpoint rate_limit_per_s; Postgres remains the fallback.
    rate_limit_redis_enabled: bool | None = None

    # Request correlation
    request_id_header: str = "X-Request-ID"

    # Operator (platform admin) API under /admin. Unset = every /admin call is rejected.
    operator_token: SecretStr | None = None

    # AES-256-GCM key for endpoint signing secrets at rest (base64, 32 bytes). Required in
    # production; the previous key stays readable during a key rotation.
    secrets_encryption_key: SecretStr | None = None
    secrets_encryption_key_previous: SecretStr | None = None

    # Tenant API keys
    api_key_rotation_overlap_s: Annotated[int, Field(ge=0, le=7 * 86_400)] = 86_400
    api_key_last_used_resolution_s: Annotated[int, Field(ge=0, le=3_600)] = 60

    # Endpoints (the full resolve-then-connect SSRF guard lands with delivery in M3)
    endpoint_allow_http: bool = False
    endpoint_allow_private_networks: bool = False
    endpoint_default_timeout_ms: TimeoutMs = 10_000
    endpoint_max_timeout_ms: TimeoutMs = 30_000

    # Event ingestion (POST /v1/events)
    event_max_payload_bytes: Annotated[int, Field(ge=1_024, le=10 * 1_048_576)] = 262_144
    event_occurred_at_max_future_s: Annotated[int, Field(ge=0, le=86_400)] = 300
    event_retention_days: Annotated[int, Field(ge=1, le=3_650)] = 30

    # Idempotency-Key records: how long a key is remembered, and how long a request waits for
    # a concurrent request holding the same key before answering 409 + Retry-After.
    idempotency_key_ttl_s: Annotated[int, Field(ge=60, le=7 * 86_400)] = 86_400
    idempotency_lock_timeout_ms: Annotated[int, Field(ge=50, le=30_000)] = 2_000

    # Fan-out relay (python -m hookrelay.relay)
    relay_batch_size: Annotated[int, Field(ge=1, le=1_000)] = 100
    relay_poll_min_ms: Annotated[int, Field(ge=10, le=10_000)] = 100
    relay_poll_max_ms: Annotated[int, Field(ge=10, le=60_000)] = 2_000
    relay_retry_max_s: Annotated[int, Field(ge=1, le=3_600)] = 300
    relay_housekeeping_interval_s: Annotated[int, Field(ge=1, le=86_400)] = 60
    relay_outbox_retention_s: Annotated[int, Field(ge=0, le=30 * 86_400)] = 3_600
    relay_heartbeat_file: str = "/tmp/hookrelay-relay.heartbeat"  # noqa: S108 - container-local
    relay_heartbeat_max_age_s: Annotated[int, Field(ge=5, le=3_600)] = 60

    # Delivery worker process (python -m hookrelay.worker)
    worker_id: str | None = None
    worker_concurrency: Annotated[int, Field(ge=1, le=512)] = 16
    worker_batch_size: Annotated[int, Field(ge=1, le=500)] = 50
    worker_poll_min_ms: Annotated[int, Field(ge=10, le=10_000)] = 100
    worker_poll_max_ms: Annotated[int, Field(ge=10, le=60_000)] = 2_000
    worker_lease_s: Annotated[int, Field(ge=5, le=3_600)] = 60
    worker_lease_renew_interval_s: Annotated[float, Field(gt=0, le=1_200)] = 15.0
    worker_reclaim_interval_s: Annotated[float, Field(gt=0, le=3_600)] = 10.0
    worker_drain_timeout_s: Annotated[float, Field(ge=0, le=600)] = 25.0
    worker_heartbeat_file: str = "/tmp/hookrelay-worker.heartbeat"  # noqa: S108 - container-local
    worker_heartbeat_max_age_s: Annotated[int, Field(ge=5, le=3_600)] = 60

    # Outbound webhook HTTP and retry policy (used by the worker)
    delivery_connect_timeout_ms: TimeoutMs = 5_000
    delivery_response_max_bytes: Annotated[int, Field(ge=0, le=65_536)] = 4_096
    delivery_max_attempts: Annotated[int, Field(ge=1, le=100)] = 8
    delivery_max_age_s: Annotated[int, Field(ge=60, le=30 * 86_400)] = 3 * 86_400
    delivery_backoff_base_s: Annotated[float, Field(gt=0, le=3_600)] = 5.0
    delivery_backoff_max_s: Annotated[float, Field(gt=0, le=86_400)] = 3_600.0
    delivery_retry_after_max_s: Annotated[int, Field(ge=0, le=86_400)] = 3_600
    delivery_allowed_ports: Annotated[frozenset[int], NoDecode] = frozenset({80, 443, 8080, 8443})
    # Lets the worker reach loopback / RFC 1918 / CGNAT targets (local receivers in tests and
    # development). Refused in production; cloud metadata addresses stay blocked regardless.
    delivery_allow_private_networks: bool = False

    # Back-pressure and failure isolation (Milestone 4). Per-endpoint limits are enforced in
    # Postgres at lease time, so they hold across any number of workers.
    # In-flight cap for endpoints that do not set max_in_flight, and the most one may ask for.
    endpoint_default_max_in_flight: Annotated[int, Field(ge=1, le=1_000)] = 10
    endpoint_max_in_flight: Annotated[int, Field(ge=1, le=1_000)] = 100
    # Highest rate_limit_per_s (deliveries per second, token bucket) an endpoint may configure.
    endpoint_max_rate_limit_per_s: Annotated[int, Field(ge=1, le=10_000)] = 1_000
    # Circuit breaker: consecutive availability failures that open it, and the open cooldown
    # (doubles on every re-trip, capped) before a single half-open probe is let through.
    breaker_failure_threshold: Annotated[int, Field(ge=1, le=1_000)] = 5
    breaker_cooldown_base_s: Annotated[float, Field(gt=0, le=3_600)] = 30.0
    breaker_cooldown_max_s: Annotated[float, Field(gt=0, le=86_400)] = 600.0
    # Disable an endpoint whose breaker has seen only failures for this long (0 = never).
    endpoint_auto_disable_after_s: Annotated[int, Field(ge=0, le=30 * 86_400)] = 3 * 86_400

    # Replay API: per-tenant request budget, rows requeued per request, widest time window.
    replay_rate_limit_per_minute: Annotated[int, Field(ge=1, le=10_000)] = 30
    replay_max_deliveries: Annotated[int, Field(ge=1, le=10_000)] = 1_000
    replay_max_window_s: Annotated[int, Field(ge=60, le=90 * 86_400)] = 7 * 86_400

    # Operator notifications (endpoint auto-disabled, ...), dispatched by the relay.
    notification_poll_interval_s: Annotated[float, Field(gt=0, le=3_600)] = 5.0
    notification_max_attempts: Annotated[int, Field(ge=1, le=100)] = 10
    notification_retention_s: Annotated[int, Field(ge=0, le=90 * 86_400)] = 7 * 86_400

    # Operability for tenants (Milestone 5).
    # Signing-secret rotation: how long the replaced secret keeps signing alongside the new
    # one (a second v1 signature) by default, and the longest overlap a tenant may ask for.
    endpoint_secret_rotation_overlap_s: Annotated[int, Field(ge=0, le=30 * 86_400)] = 86_400
    endpoint_secret_max_overlap_s: Annotated[int, Field(ge=0, le=30 * 86_400)] = 7 * 86_400
    # Endpoint health summary: default and widest window, and the most recent attempts it
    # reads at most (bounded work per request; the response says when it sampled).
    endpoint_health_default_window_s: Annotated[int, Field(ge=60, le=30 * 86_400)] = 86_400
    endpoint_health_max_window_s: Annotated[int, Field(ge=60, le=30 * 86_400)] = 7 * 86_400
    endpoint_health_max_attempts: Annotated[int, Field(ge=100, le=1_000_000)] = 10_000
    # Payload retention: drop event payloads (and stored response bodies) this many days after
    # the event once every delivery is terminal; 0 keeps them until EVENT_RETENTION_DAYS.
    event_payload_retention_days: Annotated[int, Field(ge=0, le=3_650)] = 0
    # Retention job (python -m hookrelay.retention, and one bounded pass per relay
    # housekeeping run): rows per batch transaction, batches per task per run, pause between.
    retention_batch_size: Annotated[int, Field(ge=1, le=10_000)] = 500
    retention_max_batches: Annotated[int, Field(ge=1, le=100_000)] = 200
    retention_batch_pause_ms: Annotated[int, Field(ge=0, le=60_000)] = 50

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

    @field_validator("operator_token", mode="before")
    @classmethod
    def _check_operator_token(cls, value: object) -> object:
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return None
        if isinstance(raw, str) and len(raw.strip()) < OPERATOR_TOKEN_MIN_LENGTH:
            raise ValueError(
                f"OPERATOR_TOKEN must be at least {OPERATOR_TOKEN_MIN_LENGTH} characters"
            )
        return value

    @field_validator("secrets_encryption_key", "secrets_encryption_key_previous", mode="before")
    @classmethod
    def _check_encryption_key(cls, value: object) -> object:
        raw = value.get_secret_value() if isinstance(value, SecretStr) else value
        if raw is None or (isinstance(raw, str) and not raw.strip()):
            return None
        if isinstance(raw, str):
            decode_encryption_key(raw)
        return value

    @model_validator(mode="after")
    def _check_endpoint_timeouts(self) -> Self:
        if self.endpoint_default_timeout_ms > self.endpoint_max_timeout_ms:
            raise ValueError("ENDPOINT_DEFAULT_TIMEOUT_MS must not exceed ENDPOINT_MAX_TIMEOUT_MS")
        return self

    @field_validator("worker_id")
    @classmethod
    def _check_worker_id(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        value = value.strip()
        if len(value) > WORKER_ID_MAX_LENGTH or not value.isprintable():
            raise ValueError(f"WORKER_ID must be 1 to {WORKER_ID_MAX_LENGTH} printable characters")
        return value

    @field_validator("delivery_allowed_ports", mode="before")
    @classmethod
    def _parse_ports(cls, value: object) -> object:
        """Accept ``"443,8443"`` (env var) as well as any iterable of ints."""
        if isinstance(value, str):
            items = [part.strip() for part in value.split(",") if part.strip()]
            try:
                value = [int(item) for item in items]
            except ValueError as exc:
                raise ValueError("DELIVERY_ALLOWED_PORTS must be a comma-separated list") from exc
        if isinstance(value, (list, tuple, set, frozenset)):
            ports = frozenset(int(p) for p in value)
            if not ports:
                raise ValueError("DELIVERY_ALLOWED_PORTS must list at least one port")
            if any(not 1 <= p <= MAX_PORT for p in ports):
                raise ValueError("DELIVERY_ALLOWED_PORTS entries must be between 1 and 65535")
            return ports
        return value

    @model_validator(mode="after")
    def _check_worker(self) -> Self:
        if self.worker_poll_min_ms > self.worker_poll_max_ms:
            raise ValueError("WORKER_POLL_MIN_MS must not exceed WORKER_POLL_MAX_MS")
        # Two renewals must fit in one lease, so one slow renewal round never loses a lease.
        if self.worker_lease_renew_interval_s * 2 > self.worker_lease_s:
            raise ValueError("WORKER_LEASE_RENEW_INTERVAL_S must be at most WORKER_LEASE_S / 2")
        if self.delivery_backoff_base_s > self.delivery_backoff_max_s:
            raise ValueError("DELIVERY_BACKOFF_BASE_S must not exceed DELIVERY_BACKOFF_MAX_S")
        return self

    @model_validator(mode="after")
    def _check_backpressure(self) -> Self:
        if self.endpoint_default_max_in_flight > self.endpoint_max_in_flight:
            raise ValueError(
                "ENDPOINT_DEFAULT_MAX_IN_FLIGHT must not exceed ENDPOINT_MAX_IN_FLIGHT"
            )
        if self.breaker_cooldown_base_s > self.breaker_cooldown_max_s:
            raise ValueError("BREAKER_COOLDOWN_BASE_S must not exceed BREAKER_COOLDOWN_MAX_S")
        return self

    @model_validator(mode="after")
    def _check_operability(self) -> Self:
        if self.endpoint_secret_rotation_overlap_s > self.endpoint_secret_max_overlap_s:
            raise ValueError(
                "ENDPOINT_SECRET_ROTATION_OVERLAP_S must not exceed ENDPOINT_SECRET_MAX_OVERLAP_S"
            )
        if self.endpoint_health_default_window_s > self.endpoint_health_max_window_s:
            raise ValueError(
                "ENDPOINT_HEALTH_DEFAULT_WINDOW_S must not exceed ENDPOINT_HEALTH_MAX_WINDOW_S"
            )
        days = self.event_payload_retention_days
        if days and days >= self.event_retention_days:
            raise ValueError(
                "EVENT_PAYLOAD_RETENTION_DAYS must be less than EVENT_RETENTION_DAYS "
                "(or 0 to keep payloads until the event expires)"
            )
        return self

    @property
    def event_payload_retention_s(self) -> int | None:
        """Payload retention in seconds, or None when payloads live as long as the event."""
        return self.event_payload_retention_days * 86_400 or None

    @model_validator(mode="after")
    def _check_relay_polling(self) -> Self:
        if self.relay_poll_min_ms > self.relay_poll_max_ms:
            raise ValueError("RELAY_POLL_MIN_MS must not exceed RELAY_POLL_MAX_MS")
        return self

    @property
    def event_max_request_bytes(self) -> int:
        """Hard cap on the whole POST /v1/events body: the payload plus envelope headroom."""
        return self.event_max_payload_bytes + EVENT_ENVELOPE_HEADROOM_BYTES

    @model_validator(mode="after")
    def _check_production(self) -> Self:
        if self.app_env is Environment.PRODUCTION:
            if self.log_format is not LogFormat.JSON:
                raise ValueError("LOG_FORMAT must be 'json' in production")
            if self.db_echo:
                raise ValueError("DB_ECHO must be false in production (it logs bound parameters)")
            if self.log_level is LogLevel.DEBUG:
                raise ValueError("LOG_LEVEL=DEBUG is not allowed in production")
            if self.secrets_encryption_key is None:
                raise ValueError("SECRETS_ENCRYPTION_KEY is required in production")
            if self.delivery_allow_private_networks:
                raise ValueError(
                    "DELIVERY_ALLOW_PRIVATE_NETWORKS is for tests and development only; "
                    "it is refused in production (SSRF guard)"
                )
        return self

    @model_validator(mode="after")
    def _check_redis(self) -> Self:
        if self.redis_reconnect_base_ms > self.redis_reconnect_max_ms:
            raise ValueError("REDIS_RECONNECT_BASE_MS must not exceed REDIS_RECONNECT_MAX_MS")
        if (
            not self.redis_key_prefix
            or not self.redis_key_prefix.replace(":", "")
            .replace("-", "")
            .replace("_", "")
            .isalnum()
        ):
            raise ValueError(
                "REDIS_KEY_PREFIX must be a non-empty alphanumeric string (also :_- allowed)"
            )
        return self

    @property
    def wakeup_enabled_effective(self) -> bool:
        """Wake-ups run when REDIS_URL is set, unless WAKEUP_ENABLED=false."""
        if self.redis_url is None:
            return False
        return True if self.wakeup_enabled is None else self.wakeup_enabled

    @property
    def subscription_cache_enabled_effective(self) -> bool:
        if self.redis_url is None:
            return False
        return True if self.subscription_cache_enabled is None else self.subscription_cache_enabled

    @property
    def rate_limit_redis_enabled_effective(self) -> bool:
        if self.redis_url is None:
            return False
        return True if self.rate_limit_redis_enabled is None else self.rate_limit_redis_enabled

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
