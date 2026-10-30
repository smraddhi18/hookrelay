from __future__ import annotations

import pytest

from hookrelay.services.health import CheckStatus, check_postgres, readiness
from tests.conftest import FakeDatabase


async def test_check_postgres_up() -> None:
    result = await check_postgres(FakeDatabase(), timeout_s=1)
    assert result.status is CheckStatus.UP
    assert result.error is None
    assert result.latency_ms >= 0


@pytest.mark.parametrize(
    ("db", "error"),
    [
        (FakeDatabase(ping_error=ConnectionRefusedError()), "ConnectionRefusedError"),
        (FakeDatabase(ping_error=RuntimeError("x")), "RuntimeError"),
        (FakeDatabase(ping_delay_s=1), "timed out after 0.01s"),
    ],
)
async def test_check_postgres_down(db: FakeDatabase, error: str) -> None:
    result = await check_postgres(db, timeout_s=0.01)
    assert result.status is CheckStatus.DOWN
    assert result.error == error


async def test_readiness_combines_checks_and_draining() -> None:
    assert (await readiness(FakeDatabase(), timeout_s=1, draining=False)).ready
    assert not (await readiness(FakeDatabase(), timeout_s=1, draining=True)).ready
    down = FakeDatabase(ping_error=OSError())
    assert not (await readiness(down, timeout_s=1, draining=False)).ready
