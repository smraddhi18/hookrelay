"""Mock webhook receiver for load and chaos testing: ``hookrelay-mock-receiver``.

A small, fast ASGI app (served by uvicorn) that plays a tenant's webhook endpoint:

* **configurable behaviour**: base latency + uniform jitter, a failure rate with the status
  codes to fail with (and an optional ``Retry-After``), a "hang" rate that holds the request
  long enough to trip the sender's timeout, and the success status. Defaults come from the
  CLI / ``MOCK_RECEIVER_*`` env vars, can be replaced at runtime (``PUT /config``, with
  per-path overrides) and overridden per endpoint through the URL query string, e.g.
  ``http://receiver:9000/hook/slow?latency_ms=800&failure_rate=0.2``. Randomness comes from a
  seeded RNG so a run is reproducible for the same request order.
* **accounting that proves delivery semantics**: every request is keyed by
  ``(path, webhook-id)``. ``received`` counts arrivals, ``acked`` counts 2xx responses that
  were actually written to a still-connected client (a sender that timed out and hung up is
  not acknowledged). ``acked_duplicates`` > 0 means a receiver *processed* the same message
  twice, the only kind of duplicate at-least-once delivery may produce, and only after a lost
  acknowledgement. End-to-end lag is measured from the envelope's ``timestamp``
  (``occurred_at``, set at ingest) to arrival.

Control routes: ``GET /stats`` (JSON summary), ``POST /reset``, ``GET /config``,
``PUT /config`` and ``GET /health``. Everything else accepts ``POST`` as a webhook. Not for
production use: it trusts every caller.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import random
import socket
import sys
import time
from collections import Counter
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Mapping,
    MutableMapping,
)
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl

import uvicorn

Scope = MutableMapping[str, Any]
Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]

ENV_PREFIX = "MOCK_RECEIVER_"
DEFAULT_MAX_SAMPLES = 200_000
MAX_BODY_BYTES = 1 << 20
_RETRYABLE_HINT = frozenset({429, 503})
_HTTP_STATUS_MIN = 200
_HTTP_STATUS_MAX = 599
_SUCCESS_MAX = 299
_NO_BODY = frozenset({204, 205})  # a body (or Content-Length > 0) here is a protocol error
_MAX_DELAY_MS = 600_000
_MAX_RETRY_AFTER_S = 86_400


class BehaviourError(ValueError):
    """An invalid behaviour setting (CLI, env, ``PUT /config`` or URL query)."""


def _parse_statuses(value: object) -> tuple[int, ...]:
    if isinstance(value, str):
        items: list[object] = [p.strip() for p in value.split(",") if p.strip()]
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        raise BehaviourError("failure_status must be a list or a comma-separated string")
    try:
        statuses = tuple(int(str(item)) for item in items)
    except ValueError as exc:
        raise BehaviourError("failure_status entries must be integers") from exc
    if not statuses or any(not _HTTP_STATUS_MIN <= s <= _HTTP_STATUS_MAX for s in statuses):
        raise BehaviourError("failure_status must list HTTP status codes 200-599")
    return statuses


@dataclass(frozen=True, slots=True)
class Behaviour:
    latency_ms: float = 0.0
    jitter_ms: float = 0.0
    failure_rate: float = 0.0
    failure_status: tuple[int, ...] = (500,)
    retry_after_s: int | None = None
    hang_rate: float = 0.0
    hang_ms: float = 30_000.0
    status: int = 204

    def __post_init__(self) -> None:
        for name in ("latency_ms", "jitter_ms", "hang_ms"):
            if not 0 <= getattr(self, name) <= _MAX_DELAY_MS:
                raise BehaviourError(f"{name} must be between 0 and {_MAX_DELAY_MS}")
        for name in ("failure_rate", "hang_rate"):
            if not 0 <= getattr(self, name) <= 1:
                raise BehaviourError(f"{name} must be between 0 and 1")
        if not _HTTP_STATUS_MIN <= self.status <= _SUCCESS_MAX:
            raise BehaviourError("status must be a 2xx code")
        if self.retry_after_s is not None and not (0 <= self.retry_after_s <= _MAX_RETRY_AFTER_S):
            raise BehaviourError(f"retry_after_s must be between 0 and {_MAX_RETRY_AFTER_S}")

    def merged(self, overrides: Mapping[str, object]) -> Behaviour:
        """A copy with ``overrides`` (strings from a query string, or JSON values) applied."""
        if not overrides:
            return self
        changes: dict[str, Any] = {}
        for key, raw in overrides.items():
            try:
                if key in {"latency_ms", "jitter_ms", "failure_rate", "hang_rate", "hang_ms"}:
                    value = float(str(raw))
                    if not math.isfinite(value):
                        raise BehaviourError(f"{key} must be finite")
                    changes[key] = value
                elif key == "status":
                    changes[key] = int(str(raw))
                elif key == "retry_after_s":
                    changes[key] = None if raw in (None, "") else int(str(raw))
                elif key == "failure_status":
                    changes[key] = _parse_statuses(raw)
                else:
                    raise BehaviourError(f"unknown setting {key!r}")
            except ValueError as exc:
                if isinstance(exc, BehaviourError):
                    raise
                raise BehaviourError(f"invalid value for {key}") from exc
        return replace(self, **changes)

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["failure_status"] = list(self.failure_status)
        return data


@dataclass(frozen=True, slots=True)
class Plan:
    """What to do with one request, drawn from a behaviour and the RNG."""

    delay_s: float
    status: int
    hang: bool
    headers: tuple[tuple[bytes, bytes], ...] = ()


def plan_response(behaviour: Behaviour, rng: random.Random) -> Plan:
    if behaviour.hang_rate and rng.random() < behaviour.hang_rate:
        return Plan(delay_s=behaviour.hang_ms / 1000, status=behaviour.status, hang=True)
    delay_ms = behaviour.latency_ms
    if behaviour.jitter_ms:
        delay_ms += rng.uniform(0, behaviour.jitter_ms)
    if behaviour.failure_rate and rng.random() < behaviour.failure_rate:
        status = rng.choice(behaviour.failure_status)
        headers: tuple[tuple[bytes, bytes], ...] = ()
        if behaviour.retry_after_s is not None and status in _RETRYABLE_HINT:
            headers = ((b"retry-after", str(behaviour.retry_after_s).encode()),)
        return Plan(delay_s=delay_ms / 1000, status=status, hang=False, headers=headers)
    return Plan(delay_s=delay_ms / 1000, status=behaviour.status, hang=False)


def percentile(sorted_values: list[float], q: float) -> float | None:
    """Nearest-rank percentile of an already sorted list (``None`` when empty)."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil(q / 100 * len(sorted_values)))
    return sorted_values[min(rank, len(sorted_values)) - 1]


@dataclass(slots=True)
class Recorder:
    """In-memory delivery accounting (single event loop, so no locking is needed)."""

    max_samples: int = DEFAULT_MAX_SAMPLES
    received: Counter[tuple[str, str]] = field(default_factory=Counter)
    acked: Counter[tuple[str, str]] = field(default_factory=Counter)
    by_status: Counter[int] = field(default_factory=Counter)
    disconnected: int = 0
    hung: int = 0
    missing_id: int = 0
    unsigned: int = 0
    lag_ms: list[float] = field(default_factory=list)
    lag_seen: int = 0
    first_at: float | None = None
    last_at: float | None = None
    rng: random.Random = field(default_factory=lambda: random.Random(0))  # noqa: S311

    def reset(self) -> None:
        self.received.clear()
        self.acked.clear()
        self.by_status.clear()
        self.disconnected = self.hung = self.missing_id = self.unsigned = self.lag_seen = 0
        self.lag_ms.clear()
        self.first_at = self.last_at = None

    def arrival(self, key: tuple[str, str], *, signed: bool, lag_ms: float | None) -> None:
        now = time.time()
        self.first_at = now if self.first_at is None else self.first_at
        self.last_at = now
        self.received[key] += 1
        if not key[1]:
            self.missing_id += 1
        if not signed:
            self.unsigned += 1
        if lag_ms is not None:
            self._sample(lag_ms)

    def _sample(self, value: float) -> None:
        """Reservoir sampling keeps the percentiles honest past ``max_samples``."""
        self.lag_seen += 1
        if len(self.lag_ms) < self.max_samples:
            self.lag_ms.append(value)
            return
        slot = self.rng.randrange(self.lag_seen)
        if slot < self.max_samples:
            self.lag_ms[slot] = value

    def answered(self, key: tuple[str, str], status: int) -> None:
        self.by_status[status] += 1
        if _HTTP_STATUS_MIN <= status <= _SUCCESS_MAX:
            self.acked[key] += 1

    def stats(self) -> dict[str, Any]:
        received = sum(self.received.values())
        acked = sum(self.acked.values())
        lag = sorted(self.lag_ms)
        elapsed = None
        if self.first_at is not None and self.last_at is not None:
            elapsed = self.last_at - self.first_at
        return {
            "received": received,
            "unique": len(self.received),
            "duplicates": received - len(self.received),
            "acked": acked,
            "acked_unique": len(self.acked),
            "acked_duplicates": acked - len(self.acked),
            "max_acked_per_message": max(self.acked.values(), default=0),
            "by_status": {str(k): v for k, v in sorted(self.by_status.items())},
            "disconnected": self.disconnected,
            "hung": self.hung,
            "missing_webhook_id": self.missing_id,
            "unsigned": self.unsigned,
            "window_s": elapsed,
            "lag_ms": {
                "samples": self.lag_seen,
                "p50": percentile(lag, 50),
                "p95": percentile(lag, 95),
                "p99": percentile(lag, 99),
                "max": lag[-1] if lag else None,
            },
        }

    def count(self, webhook_id: str, *, path: str | None = None, acked: bool = False) -> int:
        counter = self.acked if acked else self.received
        return sum(n for (p, wid), n in counter.items() if wid == webhook_id and path in (None, p))


def envelope_lag_ms(body: bytes, now: float) -> float | None:
    """Milliseconds from the Standard Webhooks envelope ``timestamp`` to ``now``."""
    try:
        stamp = json.loads(body).get("timestamp")
        occurred = datetime.fromisoformat(str(stamp))
    except (ValueError, AttributeError, TypeError):
        return None
    if occurred.tzinfo is None:
        return None
    return max(0.0, (now - occurred.timestamp()) * 1000)


class MockReceiver:
    """The ASGI application."""

    def __init__(
        self,
        behaviour: Behaviour | None = None,
        *,
        seed: int | None = 0,
        max_samples: int = DEFAULT_MAX_SAMPLES,
    ) -> None:
        self.default = behaviour or Behaviour()
        self.paths: dict[str, Behaviour] = {}
        self.rng = random.Random(seed)  # noqa: S311 - simulation, not security
        self.recorder = Recorder(max_samples=max_samples)

    def behaviour_for(self, path: str, query: str) -> Behaviour:
        base = self.paths.get(path, self.default)
        return base.merged(dict(parse_qsl(query, keep_blank_values=True)))

    def configure(self, data: Mapping[str, Any]) -> None:
        """``{"default": {...}, "paths": {"/hook/a": {...}}}``; replaces the previous config."""
        default = Behaviour().merged(data.get("default") or {})
        paths_raw = data.get("paths") or {}
        if not isinstance(paths_raw, Mapping):
            raise BehaviourError("paths must be an object")
        paths = {str(p): default.merged(v or {}) for p, v in paths_raw.items()}
        self.default, self.paths = default, paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "lifespan":
            await _lifespan(receive, send)
            return
        if scope["type"] != "http":  # pragma: no cover - websockets are disabled
            return
        method, path = scope["method"], scope["path"]
        if path == "/stats" and method == "GET":
            await _json(send, 200, self.recorder.stats())
        elif path == "/reset" and method == "POST":
            self.recorder.reset()
            await _json(send, 200, {"reset": True})
        elif path == "/health" and method == "GET":
            await _json(send, 200, {"status": "ok"})
        elif path == "/config" and method == "GET":
            await _json(send, 200, self._config())
        elif path == "/config" and method == "PUT":
            await self._put_config(receive, send)
        elif method == "POST":
            await self._webhook(scope, receive, send)
        else:
            await _json(send, 405, {"error": "method not allowed"})

    def _config(self) -> dict[str, Any]:
        return {
            "default": self.default.as_dict(),
            "paths": {p: b.as_dict() for p, b in self.paths.items()},
        }

    async def _put_config(self, receive: Receive, send: Send) -> None:
        body, _ = await _read_body(receive)
        try:
            data = json.loads(body or b"{}")
            if not isinstance(data, dict):
                raise BehaviourError("config must be a JSON object")
            self.configure(data)
        except (ValueError, TypeError) as exc:
            await _json(send, 422, {"error": str(exc)})
            return
        await _json(send, 200, self._config())

    async def _webhook(self, scope: Scope, receive: Receive, send: Send) -> None:
        path = scope["path"]
        try:
            behaviour = self.behaviour_for(path, scope.get("query_string", b"").decode("latin-1"))
        except BehaviourError as exc:
            await _json(send, 400, {"error": str(exc)})
            return
        body, disconnected = await _read_body(receive)
        if disconnected:
            self.recorder.disconnected += 1
            return
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}
        key = (path, headers.get("webhook-id", ""))
        self.recorder.arrival(
            key,
            signed="webhook-signature" in headers,
            lag_ms=envelope_lag_ms(body, time.time()),
        )
        plan = plan_response(behaviour, self.rng)
        if plan.hang:
            self.recorder.hung += 1
        if plan.delay_s > 0 and await _sleep_unless_disconnected(plan.delay_s, receive):
            self.recorder.disconnected += 1
            return
        body = b"" if plan.status in _NO_BODY else b"ok"
        await send(
            {
                "type": "http.response.start",
                "status": plan.status,
                "headers": [(b"content-length", str(len(body)).encode()), *plan.headers],
            }
        )
        await send({"type": "http.response.body", "body": body})
        self.recorder.answered(key, plan.status)


async def _lifespan(receive: Receive, send: Send) -> None:
    while True:
        message = await receive()
        if message["type"] == "lifespan.startup":
            await send({"type": "lifespan.startup.complete"})
        elif message["type"] == "lifespan.shutdown":
            await send({"type": "lifespan.shutdown.complete"})
            return


async def _read_body(receive: Receive) -> tuple[bytes, bool]:
    chunks: list[bytes] = []
    size = 0
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return b"", True
        chunk = message.get("body", b"")
        size += len(chunk)
        if size <= MAX_BODY_BYTES:
            chunks.append(chunk)
        if not message.get("more_body", False):
            return b"".join(chunks), False


async def _sleep_unless_disconnected(delay_s: float, receive: Receive) -> bool:
    """Sleep ``delay_s``; ``True`` if the client hung up first (a sender timeout)."""
    waiter = asyncio.ensure_future(receive())
    try:
        done, _ = await asyncio.wait({waiter}, timeout=delay_s)
        return bool(done) and waiter.result().get("type") == "http.disconnect"
    finally:
        waiter.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await waiter


async def _json(send: Send, status: int, payload: object) -> None:
    body = json.dumps(payload, separators=(",", ":")).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


@dataclass(slots=True)
class RunningReceiver:
    app: MockReceiver
    host: str
    port: int

    def url(self, path: str = "/hook", **query: object) -> str:
        suffix = "&".join(f"{k}={v}" for k, v in query.items())
        return f"http://{self.host}:{self.port}{path}" + (f"?{suffix}" if suffix else "")


def uvicorn_config(app: MockReceiver, host: str, port: int, *, log_level: str) -> uvicorn.Config:
    return uvicorn.Config(
        app,
        host=host,
        port=port,
        lifespan="on",
        ws="none",
        access_log=False,
        log_level=log_level,
        log_config=None,
        timeout_keep_alive=75,
        timeout_graceful_shutdown=1,
        backlog=4096,
    )


class _EmbeddedServer(uvicorn.Server):
    """Leaves signal handling to the host process (tests, workers sharing the loop)."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


@contextlib.asynccontextmanager
async def serve_in_background(
    app: MockReceiver, *, host: str = "127.0.0.1", port: int = 0
) -> AsyncIterator[RunningReceiver]:
    """Run ``app`` on this event loop (port 0 = pick a free one); stop it on exit."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, port))
    bound_port = sock.getsockname()[1]
    server = _EmbeddedServer(uvicorn_config(app, host, bound_port, log_level="warning"))
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        while not server.started:  # uvicorn exposes a flag, not an event
            if task.done():
                task.result()
            await asyncio.sleep(0.01)
        yield RunningReceiver(app=app, host=host, port=bound_port)
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(task, 10)
        sock.close()


_MAX_PORT = 65_535
# Kubernetes "service links" inject ``<SERVICE>_PORT=tcp://<cluster-ip>:<port>`` for every
# Service in the namespace; a Service named ``mock-receiver`` therefore sets
# ``MOCK_RECEIVER_PORT``. Recognise the shape so the error says what to change.
_SERVICE_LINK_PREFIXES = ("tcp://", "udp://", "sctp://")


def _env(name: str, default: str) -> str:
    """Raw ``MOCK_RECEIVER_<name>`` value. Left as a string: argparse converts (and validates)
    a string default with the option's ``type`` only when the option is not on the command
    line, so a bad env var never breaks a run that passes the flag explicitly."""
    return os.environ.get(ENV_PREFIX + name, default)


def _number_type(
    kind: Callable[[str], float], label: str, *, env: str, low: float | None = None
) -> Callable[[str], Any]:
    """argparse ``type=`` that names the flag *and* the env var instead of a bare traceback."""

    def convert(raw: str) -> Any:
        try:
            value = kind(raw)
        except ValueError:
            hint = ""
            if raw.strip().lower().startswith(_SERVICE_LINK_PREFIXES):
                hint = (
                    f"; {ENV_PREFIX}{env} looks like a Kubernetes service link injected for a "
                    "Service named mock-receiver: set enableServiceLinks: false on the pod "
                    "(or pass the flag explicitly)"
                )
            msg = f"expected {label}, got {raw!r} (from the flag or {ENV_PREFIX}{env}){hint}"
            raise argparse.ArgumentTypeError(msg) from None
        if low is not None and value < low:
            msg = f"expected {label}, got {raw!r} (from the flag or {ENV_PREFIX}{env})"
            raise argparse.ArgumentTypeError(msg)
        return value

    return convert


def _port(raw: str) -> int:
    value = int(raw)
    if not 0 <= value <= _MAX_PORT:  # 0 = any free port (tests)
        raise ValueError(raw)
    return value


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="hookrelay-mock-receiver",
        description="Mock webhook receiver for load/chaos tests (latency, failures, accounting)",
        epilog=f"Every option defaults to the {ENV_PREFIX}<OPTION> env var (e.g. "
        f"{ENV_PREFIX}PORT); a flag on the command line wins.",
    )
    num = _number_type
    p.add_argument("--host", default=_env("HOST", "127.0.0.1"))
    p.add_argument(
        "--port",
        type=num(_port, "a TCP port (0-65535)", env="PORT"),
        default=_env("PORT", "9000"),
    )
    p.add_argument(
        "--latency-ms",
        type=num(float, "milliseconds >= 0", env="LATENCY_MS", low=0),
        default=_env("LATENCY_MS", "0"),
    )
    p.add_argument(
        "--jitter-ms",
        type=num(float, "milliseconds >= 0", env="JITTER_MS", low=0),
        default=_env("JITTER_MS", "0"),
    )
    p.add_argument(
        "--failure-rate",
        type=num(float, "a number", env="FAILURE_RATE"),
        default=_env("FAILURE_RATE", "0"),
    )
    p.add_argument("--failure-status", default=_env("FAILURE_STATUS", "500"))
    p.add_argument("--retry-after-s", default=_env("RETRY_AFTER_S", ""))
    p.add_argument(
        "--hang-rate",
        type=num(float, "a number", env="HANG_RATE"),
        default=_env("HANG_RATE", "0"),
    )
    p.add_argument(
        "--hang-ms",
        type=num(float, "milliseconds >= 0", env="HANG_MS", low=0),
        default=_env("HANG_MS", "30000"),
    )
    p.add_argument(
        "--status",
        type=num(int, "an HTTP status code", env="STATUS"),
        default=_env("STATUS", "204"),
    )
    p.add_argument("--seed", type=num(int, "an integer", env="SEED"), default=_env("SEED", "0"))
    p.add_argument("--log-level", default=_env("LOG_LEVEL", "warning"))
    return p


def behaviour_from_args(args: argparse.Namespace) -> Behaviour:
    return Behaviour().merged(
        {
            "latency_ms": args.latency_ms,
            "jitter_ms": args.jitter_ms,
            "failure_rate": args.failure_rate,
            "failure_status": args.failure_status,
            "retry_after_s": args.retry_after_s,
            "hang_rate": args.hang_rate,
            "hang_ms": args.hang_ms,
            "status": args.status,
        }
    )


def main(
    argv: list[str] | None = None, *, run: Callable[[uvicorn.Config], None] | None = None
) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        behaviour = behaviour_from_args(args)
    except BehaviourError as exc:
        parser.error(str(exc))
    app = MockReceiver(behaviour, seed=args.seed)
    config = uvicorn_config(app, args.host, args.port, log_level=args.log_level)
    (run or (lambda c: uvicorn.Server(c).run()))(config)
    return 0


if __name__ == "__main__":
    sys.exit(main())
