"""``hookrelay`` console script: uvicorn wiring the Helm chart's grace-period math relies on."""

from __future__ import annotations

from typing import Any

import pytest

from hookrelay import __main__ as entrypoint
from hookrelay.core.config import Settings
from tests.conftest import make_settings


def _run_with(monkeypatch: pytest.MonkeyPatch, settings: Settings) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_run(app: str, **kwargs: Any) -> None:
        captured["app"] = app
        captured.update(kwargs)

    monkeypatch.setattr("hookrelay.__main__.get_settings", lambda: settings)
    monkeypatch.setattr("uvicorn.run", fake_run)
    entrypoint.main()
    return captured


def test_default_graceful_shutdown_is_20s(monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = _run_with(monkeypatch, make_settings())
    assert kwargs["app"] == "hookrelay.main:create_app"
    assert kwargs["factory"] is True
    assert kwargs["timeout_graceful_shutdown"] == 20
    assert kwargs["access_log"] is False


def test_graceful_shutdown_comes_from_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = _run_with(monkeypatch, make_settings(api_graceful_shutdown_s=45, port=8123))
    assert kwargs["timeout_graceful_shutdown"] == 45
    assert kwargs["port"] == 8123


@pytest.mark.parametrize("bad", [0, 601])
def test_graceful_shutdown_bounds(bad: int) -> None:
    with pytest.raises(ValueError, match="api_graceful_shutdown_s"):
        make_settings(api_graceful_shutdown_s=bad)
