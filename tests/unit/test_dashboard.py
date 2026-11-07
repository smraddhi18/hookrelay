"""Dashboard policy validation (no DB)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from hookrelay.core.errors import UnprocessableEntityError
from hookrelay.services.dashboard import DashboardPolicy
from tests.conftest import make_settings


def test_dashboard_window_defaults_and_cap() -> None:
    policy = DashboardPolicy.from_settings(
        make_settings(dashboard_default_window_s=3600, dashboard_max_window_s=7200)
    )
    assert policy.window(None) == 3600
    assert policy.window(1800) == 1800
    with pytest.raises(UnprocessableEntityError) as exc:
        policy.window(10_000)
    assert exc.value.code == "window_too_large"


def test_dashboard_settings_cross_check() -> None:
    with pytest.raises(ValidationError):
        make_settings(dashboard_default_window_s=10_000, dashboard_max_window_s=3600)
