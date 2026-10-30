from __future__ import annotations

import io
import json
import logging

import structlog

from hookrelay.core.logging import configure_logging
from tests.conftest import make_settings, serve


def _lines(stream: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line.strip()]


def test_json_logs_include_context_and_redact_secrets() -> None:
    stream = io.StringIO()
    configure_logging(make_settings(log_level="INFO"), stream=stream)
    structlog.contextvars.bind_contextvars(request_id="req-1")
    try:
        structlog.stdlib.get_logger("t").info("hello", password="hunter2", tenant="acme")
    finally:
        structlog.contextvars.clear_contextvars()
    (line,) = _lines(stream)
    assert line["event"] == "hello"
    assert line["request_id"] == "req-1"
    assert line["level"] == "info"
    assert line["logger"] == "t"
    assert line["password"] == "[REDACTED]"
    assert line["tenant"] == "acme"
    assert str(line["timestamp"]).endswith("Z")


def test_stdlib_loggers_are_rendered_as_json() -> None:
    stream = io.StringIO()
    configure_logging(make_settings(log_level="INFO"), stream=stream)
    logging.getLogger("uvicorn.error").info("server %s", "started")
    (line,) = _lines(stream)
    assert line["event"] == "server started"
    assert line["logger"] == "uvicorn.error"


def test_level_filtering() -> None:
    stream = io.StringIO()
    configure_logging(make_settings(log_level="WARNING"), stream=stream)
    log = structlog.stdlib.get_logger("t")
    log.info("dropped")
    log.warning("kept")
    assert [line["event"] for line in _lines(stream)] == ["kept"]


def test_console_format_is_human_readable() -> None:
    stream = io.StringIO()
    configure_logging(make_settings(log_format="console", log_level="INFO"), stream=stream)
    structlog.stdlib.get_logger("t").info("hi there", k=1)
    out = stream.getvalue()
    assert "hi there" in out
    assert "k=1" in out


def test_exceptions_are_structured_in_json() -> None:
    stream = io.StringIO()
    configure_logging(make_settings(log_level="INFO"), stream=stream)
    try:
        raise ValueError("bad")  # noqa: TRY301
    except ValueError:
        structlog.stdlib.get_logger("t").exception("failed")
    (line,) = _lines(stream)
    frames = line["exception"]
    assert isinstance(frames, list)
    assert frames[0]["exc_type"] == "ValueError"


async def test_access_log_line_per_request(app_factory) -> None:
    app = app_factory(log_level="INFO")
    stream = io.StringIO()
    configure_logging(make_settings(log_level="INFO"), stream=stream)
    async with serve(app) as client:
        await client.get("/health", headers={"X-Request-ID": "acc-1"})
    access = [line for line in _lines(stream) if line["event"] == "http.request"]
    assert len(access) == 1
    entry = access[0]
    assert entry["request_id"] == "acc-1"
    assert entry["method"] == "GET"
    assert entry["path"] == "/health"
    assert entry["status_code"] == 200
    assert isinstance(entry["duration_ms"], float)
