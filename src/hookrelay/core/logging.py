"""structlog configuration: JSON in production, pretty console locally, stdlib bridged.

Uvicorn, SQLAlchemy and Alembic log through the stdlib ``logging`` module; routing them through
structlog's ``ProcessorFormatter`` means *every* line shares one format and carries the bound
``request_id``.
"""

from __future__ import annotations

import logging
import re
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor, WrappedLogger

from hookrelay.core.config import LogFormat, Settings

_SENSITIVE_KEYS = frozenset(
    {
        "password",
        "secret",
        "authorization",
        "api_key",
        "token",
        "operator_token",
        "signing_secret",
        "key_hash",
        "secret_ciphertext",
        "headers",
    }
)
# Credential shapes minted by hookrelay (API keys, endpoint signing secrets), wherever they appear.
_SECRET_VALUE = re.compile(r"\b(?:hrk_[a-z0-9]{12}_[A-Za-z0-9_-]{20,}|whsec_[A-Za-z0-9+/=_-]{20,})")
REDACTED = "[REDACTED]"
_DICT_TRACEBACKS = structlog.processors.ExceptionRenderer(
    structlog.tracebacks.ExceptionDictTransformer(show_locals=False)
)


def redact_text(value: str) -> str:
    """Replace any hookrelay credential embedded in free text (exception messages, URLs)."""
    return _SECRET_VALUE.sub(REDACTED, value)


def _redact_sensitive(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Defense in depth: never let an obviously sensitive field or value reach the log sink."""
    for key, value in list(event_dict.items()):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = REDACTED
        elif isinstance(value, str) and ("hrk_" in value or "whsec_" in value):
            event_dict[key] = redact_text(value)
    return event_dict


def _drop_color_message(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    # uvicorn adds a duplicate ANSI-coloured copy of the message.
    event_dict.pop("color_message", None)
    return event_dict


def shared_processors() -> list[Processor]:
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _drop_color_message,
        _redact_sensitive,
    ]


def configure_logging(settings: Settings, *, stream: Any = None) -> None:
    """Configure structlog + stdlib logging once per process. Safe to call repeatedly."""
    level = logging.getLevelNamesMapping()[settings.log_level.value]
    shared = shared_processors()

    renderer: Processor
    if settings.log_format is LogFormat.JSON:
        renderer = structlog.processors.JSONRenderer()
        # Structured tracebacks *without* frame locals: locals routinely hold request headers,
        # API keys and decrypted secrets, which must never reach the log pipeline.
        shared = [*shared, _DICT_TRACEBACKS]
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)

    structlog.configure(
        processors=[*shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(level),
        cache_logger_on_first_use=False,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, renderer],
    )
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Our access log replaces uvicorn's; keep uvicorn's error logger for server lifecycle.
    for name in ("uvicorn", "uvicorn.error"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers.clear()
        uv_logger.propagate = True
    logging.getLogger("uvicorn.access").disabled = True
    logging.getLogger("sqlalchemy.engine").setLevel(
        logging.INFO if settings.db_echo else logging.WARNING
    )
