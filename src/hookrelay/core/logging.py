"""structlog configuration: JSON in production, pretty console locally, stdlib bridged.

Uvicorn, SQLAlchemy and Alembic log through the stdlib ``logging`` module; routing them through
structlog's ``ProcessorFormatter`` means *every* line shares one format and carries the bound
``request_id``.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor, WrappedLogger

from hookrelay.core.config import LogFormat, Settings

_SENSITIVE_KEYS = frozenset({"password", "secret", "authorization", "api_key", "token"})


def _redact_sensitive(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Defense in depth: never let an obviously sensitive field reach the log sink."""
    for key in list(event_dict):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = "[REDACTED]"
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
        shared = [*shared, structlog.processors.dict_tracebacks]
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
