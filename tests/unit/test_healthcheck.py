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
