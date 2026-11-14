"""The load/chaos mock receiver without a network: behaviour parsing, planning, accounting,
the ASGI routes (through httpx's ASGI transport) and the CLI."""

from __future__ import annotations

import json
import random
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import uvicorn

from hookrelay import mock_receiver
from hookrelay.mock_receiver import (
    Behaviour,
    BehaviourError,
    MockReceiver,
    Recorder,
    envelope_lag_ms,
    percentile,
    plan_response,
)


def test_behaviour_defaults_and_validation() -> None:
    assert Behaviour().status == 204
    for bad in (
        {"latency_ms": -1},
        {"failure_rate": 1.5},
        {"hang_rate": -0.1},
        {"status": 500},
        {"retry_after_s": -1},
        {"failure_status": "99"},
        {"failure_status": "abc"},
        {"failure_status": 42},
        {"latency_ms": "nan"},
        {"latency_ms": "slow"},
        {"colour": "blue"},
    ):
        with pytest.raises(BehaviourError):
            Behaviour().merged(bad)


def test_behaviour_merges_query_strings_and_json_values() -> None:
    base = Behaviour(latency_ms=5)
    assert base.merged({}) is base
    merged = base.merged(
        {"failure_rate": "0.25", "failure_status": "503, 429", "retry_after_s": "2", "status": 200}
    )
    assert merged.latency_ms == 5
    assert (merged.failure_rate, merged.failure_status, merged.retry_after_s) == (
        0.25,
        (503, 429),
        2,
    )
    assert merged.merged({"retry_after_s": ""}).retry_after_s is None
    assert base.merged({"failure_status": [500, 502]}).failure_status == (500, 502)
    assert merged.as_dict()["failure_status"] == [503, 429]


def test_plan_response_draws_failures_hangs_and_jitter_from_the_seeded_rng() -> None:
    rng = random.Random(1)
    assert plan_response(Behaviour(), rng) == mock_receiver.Plan(0.0, 204, hang=False)
    jittered = plan_response(Behaviour(latency_ms=10, jitter_ms=10), rng)
    assert 0.01 <= jittered.delay_s <= 0.02
    failing = Behaviour(failure_rate=1, failure_status=(429,), retry_after_s=3)
    plan = plan_response(failing, rng)
    assert (plan.status, plan.headers) == (429, ((b"retry-after", b"3"),))
    no_hint = plan_response(Behaviour(failure_rate=1, failure_status=(500,), retry_after_s=3), rng)
    assert (no_hint.status, no_hint.headers) == (500, ())
    hang = plan_response(Behaviour(hang_rate=1, hang_ms=1500), rng)
    assert (hang.hang, hang.delay_s) == (True, 1.5)
    # Same seed, same sequence: a chaos run is reproducible.
    flaky = Behaviour(failure_rate=0.5)
    rng_a, rng_b = random.Random(7), random.Random(7)
    first = [plan_response(flaky, rng_a).status for _ in range(20)]
    again = [plan_response(flaky, rng_b).status for _ in range(20)]
    assert first == again
    assert set(first) == {204, 500}


def test_percentile_is_nearest_rank() -> None:
    assert percentile([], 50) is None
    values = [float(v) for v in range(1, 101)]
    assert (percentile(values, 50), percentile(values, 95), percentile(values, 100)) == (
        50.0,
        95.0,
        100.0,
    )
    assert percentile([3.0], 99) == 3.0


def test_recorder_counts_duplicates_acks_and_samples_lag() -> None:
    recorder = Recorder(max_samples=10)
    key = ("/a", "msg_1")
    recorder.arrival(key, signed=True, lag_ms=5.0)
    recorder.answered(key, 500)
    recorder.arrival(key, signed=True, lag_ms=7.0)
    recorder.answered(key, 204)
    recorder.arrival(("/a", ""), signed=False, lag_ms=None)
    for value in range(100):  # reservoir keeps at most max_samples
        recorder.arrival(("/b", f"m{value}"), signed=True, lag_ms=float(value))
    stats = recorder.stats()
    assert stats["received"] == 103
    assert stats["unique"] == 102
    assert stats["duplicates"] == 1
    assert (stats["acked"], stats["acked_duplicates"], stats["max_acked_per_message"]) == (1, 0, 1)
    assert stats["by_status"] == {"204": 1, "500": 1}
    assert (stats["missing_webhook_id"], stats["unsigned"]) == (1, 1)
    assert stats["lag_ms"]["samples"] == 102
    assert len(recorder.lag_ms) == 10
    assert recorder.count("msg_1") == 2
    assert recorder.count("msg_1", path="/a", acked=True) == 1
    assert recorder.count("msg_1", path="/b") == 0
    recorder.reset()
    assert recorder.stats()["received"] == 0
    assert recorder.stats()["window_s"] is None


def test_envelope_lag() -> None:
    occurred = datetime.now(UTC) - timedelta(seconds=2)
    body = json.dumps({"timestamp": occurred.isoformat()}).encode()
    lag = envelope_lag_ms(body, occurred.timestamp() + 2)
    assert lag is not None
    assert abs(lag - 2000) < 1
    assert envelope_lag_ms(b"not json", 0) is None
    assert envelope_lag_ms(b"[]", 0) is None
    assert envelope_lag_ms(b'{"timestamp": "2026-11-08T10:00:00"}', 0) is None  # naive
    assert envelope_lag_ms(json.dumps({"timestamp": occurred.isoformat()}).encode(), 0) == 0.0


async def _client(app: MockReceiver) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mock")


async def test_asgi_webhook_accounting_and_control_routes() -> None:
    app = MockReceiver(Behaviour(latency_ms=1), seed=3)
    async with await _client(app) as client:
        occurred = datetime.now(UTC).isoformat()
        body = json.dumps({"type": "t", "timestamp": occurred, "data": {}})
        headers = {"webhook-id": "msg_1", "webhook-signature": "v1,x"}
        for _ in range(2):
            ok = await client.post("/hooks/a", content=body, headers=headers)
            assert (ok.status_code, ok.content) == (204, b"")
        ok200 = await client.post("/hooks/a?status=200", content=body, headers=headers)
        assert (ok200.status_code, ok200.text) == (200, "ok")
        stats = (await client.get("/stats")).json()
        assert (stats["received"], stats["unique"], stats["acked_duplicates"]) == (3, 1, 2)
        assert stats["lag_ms"]["samples"] == 3
        assert stats["unsigned"] == 0

        bad = await client.post("/hooks/a?failure_rate=2")
        assert bad.status_code == 400
        configured = await client.put(
            "/config",
            json={"default": {"latency_ms": 0}, "paths": {"/down": {"failure_rate": 1}}},
        )
        assert configured.json()["paths"]["/down"]["failure_rate"] == 1.0
        assert (await client.get("/config")).json()["default"]["latency_ms"] == 0.0
        assert (await client.post("/down")).status_code == 500
        assert (await client.post("/up")).status_code == 204
        assert (await client.put("/config", json={"paths": [1]})).status_code == 422
        assert (await client.put("/config", content=b"{")).status_code == 422
        assert (await client.get("/hooks/a")).status_code == 405
        assert (await client.get("/health")).json() == {"status": "ok"}
        assert (await client.post("/reset")).status_code == 200
        assert (await client.get("/stats")).json()["received"] == 0


async def test_asgi_client_disconnect_is_not_an_ack() -> None:
    app = MockReceiver(Behaviour(hang_rate=1, hang_ms=10_000))
    sent: list[dict[str, Any]] = []
    messages: Iterator[dict[str, Any]] = iter(
        [
            {"type": "http.request", "body": b"{}", "more_body": False},
            {"type": "http.disconnect"},
        ]
    )

    async def receive() -> dict[str, Any]:
        return next(messages)

    async def send(message: Any) -> None:
        sent.append(message)

    scope = {"type": "http", "method": "POST", "path": "/h", "query_string": b"", "headers": []}
    await app(scope, receive, send)
    assert sent == []
    stats = app.recorder.stats()
    assert (stats["received"], stats["acked"], stats["hung"], stats["disconnected"]) == (1, 0, 1, 1)

    # A client that hangs up while the body is still streaming is not even received.
    early = iter([{"type": "http.disconnect"}])

    async def receive_early() -> dict[str, Any]:
        return next(early)

    await app(scope, receive_early, send)
    assert app.recorder.stats()["disconnected"] == 2


async def test_asgi_lifespan_and_large_bodies_are_capped() -> None:
    app = MockReceiver()
    lifespan = iter([{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}])
    sent: list[Any] = []

    async def receive() -> Any:
        return next(lifespan)

    async def send(message: Any) -> None:
        sent.append(message["type"])

    await app({"type": "lifespan"}, receive, send)
    assert sent == ["lifespan.startup.complete", "lifespan.shutdown.complete"]

    chunk = b"x" * (mock_receiver.MAX_BODY_BYTES // 2 + 1)
    chunks = iter(
        [
            {"type": "http.request", "body": chunk, "more_body": True},
            {"type": "http.request", "body": chunk, "more_body": True},
            {"type": "http.request", "body": b"", "more_body": False},
        ]
    )

    async def receive_body() -> Any:
        return next(chunks)

    body, disconnected = await mock_receiver._read_body(receive_body)
    assert (len(body), disconnected) == (len(chunk), False)


def test_cli_builds_the_server_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOCK_RECEIVER_LATENCY_MS", "12")
    captured: list[uvicorn.Config] = []
    argv = ["--port", "9123", "--failure-rate", "0.1", "--failure-status", "503,429"]
    assert mock_receiver.main([*argv, "--retry-after-s", "1"], run=captured.append) == 0
    [config] = captured
    assert config.port == 9123
    app = config.app
    assert isinstance(app, MockReceiver)
    assert app.default.latency_ms == 12
    assert app.default.failure_status == (503, 429)
    assert app.default.retry_after_s == 1
    with pytest.raises(SystemExit):
        mock_receiver.main(["--failure-rate", "3"], run=captured.append)


def test_service_link_port_env_is_a_clear_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """CI run #141: a Service named mock-receiver injects MOCK_RECEIVER_PORT=tcp://ip:port."""
    monkeypatch.setenv("MOCK_RECEIVER_PORT", "tcp://10.96.23.64:9000")
    captured: list[uvicorn.Config] = []
    with pytest.raises(SystemExit) as exc:
        mock_receiver.main([], run=captured.append)
    assert exc.value.code == 2  # argparse usage error, not a traceback
    err = capsys.readouterr().err
    assert "argument --port" in err
    assert "MOCK_RECEIVER_PORT" in err
    assert "enableServiceLinks: false" in err
    assert captured == []
    # An explicit flag wins and the bad env var is never parsed.
    assert mock_receiver.main(["--port", "9000"], run=captured.append) == 0
    assert captured[0].port == 9000


@pytest.mark.parametrize(
    ("env", "value", "message"),
    [
        ("PORT", "70000", "a TCP port"),
        ("PORT", "nine", "a TCP port"),
        ("LATENCY_MS", "-5", "milliseconds >= 0"),
        ("SEED", "1.5", "an integer"),
        ("STATUS", "ok", "an HTTP status code"),
        ("FAILURE_RATE", "lots", "a number"),
    ],
)
def test_bad_env_values_name_the_variable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    env: str,
    value: str,
    message: str,
) -> None:
    monkeypatch.setenv(f"MOCK_RECEIVER_{env}", value)
    with pytest.raises(SystemExit):
        mock_receiver.main([], run=lambda _c: None)
    err = capsys.readouterr().err
    assert message in err
    assert f"MOCK_RECEIVER_{env}" in err
    assert "service link" not in err


def test_env_defaults_are_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOCK_RECEIVER_PORT", "9555")
    monkeypatch.setenv("MOCK_RECEIVER_SEED", "7")
    monkeypatch.setenv("MOCK_RECEIVER_JITTER_MS", "2.5")
    args = mock_receiver.build_parser().parse_args([])
    assert (args.port, args.seed, args.jitter_ms) == (9555, 7, 2.5)


def test_running_receiver_urls() -> None:
    running = mock_receiver.RunningReceiver(app=MockReceiver(), host="127.0.0.1", port=9)
    assert running.url() == "http://127.0.0.1:9/hook"
    assert running.url("/x", latency_ms=5, failure_rate=0.5) == (
        "http://127.0.0.1:9/x?latency_ms=5&failure_rate=0.5"
    )
