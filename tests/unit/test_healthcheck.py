from __future__ import annotations

import http.server
import threading
from collections.abc import Iterator

import pytest

from hookrelay import healthcheck


class _Handler(http.server.BaseHTTPRequestHandler):
    status = 200

    def do_GET(self) -> None:
        self.send_response(self.status if self.path == "/health" else 404)
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass


@pytest.fixture
def local_server() -> Iterator[http.server.HTTPServer]:
    server = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_main_ok_when_health_is_200(local_server, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", str(local_server.server_address[1]))
    assert healthcheck.main() == 0


def test_main_fails_when_nothing_listens(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT", "1")
    assert healthcheck.main() == 1


def test_non_200_is_unhealthy(local_server) -> None:
    port = local_server.server_address[1]
    assert healthcheck.probe(f"http://127.0.0.1:{port}/health")
    assert not healthcheck.probe(f"http://127.0.0.1:{port}/other")


def test_refuses_remote_urls() -> None:
    with pytest.raises(ValueError, match="local"):
        healthcheck.probe("http://example.com/health")


# -- --heartbeat relay|worker (Kubernetes exec probes) -------------------------------------


def test_heartbeat_defaults_match_settings() -> None:
    """The stdlib probe must agree with Settings without importing it (probe cost)."""
    from hookrelay.core.config import Settings  # noqa: PLC0415 - only this test needs it

    fields = Settings.model_fields
    for role, (path, max_age) in healthcheck.HEARTBEAT_DEFAULTS.items():
        assert fields[f"{role}_heartbeat_file"].default == path
        assert fields[f"{role}_heartbeat_max_age_s"].default == max_age


@pytest.mark.parametrize("role", ["relay", "worker"])
def test_heartbeat_fresh_stale_and_missing(
    role: str, tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hb = tmp_path / f"{role}.hb"
    monkeypatch.setenv(f"{role.upper()}_HEARTBEAT_FILE", str(hb))
    monkeypatch.setenv(f"{role.upper()}_HEARTBEAT_MAX_AGE_S", "30")
    assert healthcheck.main(["--heartbeat", role]) == 1  # not written yet
    hb.touch()
    assert healthcheck.main(["--heartbeat", role]) == 0
    mtime = hb.stat().st_mtime
    assert healthcheck.check_heartbeat(role, now=mtime + 29)
    assert not healthcheck.check_heartbeat(role, now=mtime + 31)


def test_heartbeat_env_is_case_insensitive_and_falls_back(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hb = tmp_path / "w.hb"
    hb.touch()
    monkeypatch.delenv("WORKER_HEARTBEAT_FILE", raising=False)
    monkeypatch.delenv("WORKER_HEARTBEAT_MAX_AGE_S", raising=False)
    monkeypatch.setenv("worker_heartbeat_file", str(hb))
    monkeypatch.setenv("worker_heartbeat_max_age_s", "not-a-number")
    path, max_age = healthcheck.heartbeat_settings("worker")
    assert path == str(hb)
    assert max_age == healthcheck.HEARTBEAT_DEFAULTS["worker"][1]
    monkeypatch.delenv("worker_heartbeat_file")
    monkeypatch.setenv("WORKER_HEARTBEAT_FILE", "  ")
    default_path = healthcheck.HEARTBEAT_DEFAULTS["worker"][0]
    assert healthcheck.heartbeat_settings("worker")[0] == default_path


@pytest.mark.parametrize(
    "argv", [["--heartbeat"], ["--heartbeat", "api"], ["--bogus"], ["--heartbeat", "relay", "x"]]
)
def test_bad_arguments_exit_2(argv: list[str], capsys: pytest.CaptureFixture[str]) -> None:
    assert healthcheck.main(argv) == 2
    assert "usage" in capsys.readouterr().err


def test_module_entrypoint_passes_argv(tmp_path) -> None:
    import subprocess  # noqa: PLC0415
    import sys  # noqa: PLC0415

    hb = tmp_path / "relay.hb"
    hb.touch()
    env = {"RELAY_HEARTBEAT_FILE": str(hb), "PATH": "/usr/bin:/bin"}
    ok = subprocess.run(
        [sys.executable, "-m", "hookrelay.healthcheck", "--heartbeat", "relay"],
        env=env,
        check=False,
        timeout=30,
    )
    assert ok.returncode == 0
    hb.unlink()
    stale = subprocess.run(
        [sys.executable, "-m", "hookrelay.healthcheck", "--heartbeat", "relay"],
        env=env,
        check=False,
        timeout=30,
    )
    assert stale.returncode == 1


def test_probe_module_imports_only_the_stdlib() -> None:
    """Exec probes run every few seconds: importing the app here would cost ~0.7 s CPU each."""
    import subprocess  # noqa: PLC0415
    import sys  # noqa: PLC0415

    code = (
        "import sys, hookrelay.healthcheck as h; "
        "heavy = [m for m in ('sqlalchemy', 'pydantic', 'httpx', 'fastapi', 'opentelemetry', "
        "'hookrelay.core.config') if m in sys.modules]; print(','.join(heavy))"
    )
    out = subprocess.run(  # noqa: S603 - fixed interpreter and code
        [sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=30
    )
    assert out.stdout.strip() == ""
