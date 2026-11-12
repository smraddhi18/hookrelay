"""Milestone 9: running behind PgBouncer in transaction pooling mode.

Transaction pooling hands a server connection to a client for one transaction only, so the
application must never depend on session state: no session-level advisory locks, LISTEN,
session ``SET``, temporary tables, ``WITH HOLD`` cursors or client-named prepared statements
reused across transactions. These tests pin the driver settings that make asyncpg safe there
and statically audit the code base for session features.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from hookrelay.core.config import Settings
from hookrelay.db.engine import build_engine, connect_args, prepared_statement_name
from tests.conftest import make_settings

SRC = Path(__file__).resolve().parents[2] / "src" / "hookrelay"

# Session-scoped features that break (or leak between clients) under transaction pooling.
SESSION_FEATURES: dict[str, re.Pattern[str]] = {
    "session advisory lock": re.compile(r"pg_(try_)?advisory_lock(_shared)?\s*\(", re.I),
    "LISTEN/UNLISTEN": re.compile(r"\b(UN)?LISTEN\s+[\w\"*]", re.I),
    "session SET": re.compile(r"""["']\s*SET\s+(?!LOCAL\b)(SESSION\s+)?\w+\s*(=|TO\b)""", re.I),
    "set_config(..., false)": re.compile(r"set_config\([^)]*,\s*false\s*\)", re.I),
    "SQL PREPARE": re.compile(r"""["']\s*PREPARE\s+\w+""", re.I),
    "temporary table": re.compile(r"CREATE\s+(LOCAL\s+)?TEMP(ORARY)?\s+TABLE", re.I),
    "WITH HOLD cursor": re.compile(r"\bWITH\s+HOLD\b", re.I),
}


def _sources() -> dict[Path, str]:
    return {p: p.read_text(encoding="utf-8") for p in sorted(SRC.rglob("*.py"))}


def test_application_code_uses_no_session_level_postgres_features() -> None:
    offenders = [
        f"{path.relative_to(SRC)}:{source[: m.start()].count(chr(10)) + 1}: {feature}"
        for path, source in _sources().items()
        for feature, pattern in SESSION_FEATURES.items()
        for m in pattern.finditer(source)
    ]
    assert offenders == [], "session state does not survive transaction pooling: " + str(offenders)


def test_the_audit_recognises_what_it_forbids_and_allows_transaction_scoped_features() -> None:
    forbidden = {
        "session advisory lock": "SELECT pg_advisory_lock(1)",
        "LISTEN/UNLISTEN": "LISTEN deliveries",
        "session SET": "text('SET statement_timeout = 5')",
        "set_config(..., false)": "SELECT set_config('x.y', '1', false)",
        "SQL PREPARE": "execute('PREPARE q AS SELECT 1')",
        "temporary table": "CREATE TEMP TABLE t (id int)",
        "WITH HOLD cursor": "DECLARE c CURSOR WITH HOLD FOR SELECT 1",
    }
    for feature, sample in forbidden.items():
        assert SESSION_FEATURES[feature].search(sample), feature
    allowed = [
        "SELECT pg_advisory_xact_lock(hashtextextended(:k, 0))",  # replays' rate limiter
        "text(f\"SET LOCAL lock_timeout = '5ms'\")",  # idempotency claim
        '"""\n    UPDATE deliveries\n    SET status = :status',  # an UPDATE's SET clause
        "SELECT set_config('x.y', '1', true)",
    ]
    for sample in allowed:
        assert not any(p.search(sample) for p in SESSION_FEATURES.values()), sample
    # And the transaction-scoped ones really are in use (the audit is not vacuous).
    joined = "\n".join(_sources().values())
    assert "pg_advisory_xact_lock" in joined
    assert "SET LOCAL" in joined


def test_direct_connections_carry_server_side_timeouts() -> None:
    settings = make_settings(db_statement_timeout_ms=7_000, db_connect_timeout_s=3)
    args = connect_args(settings)
    assert args["timeout"] == 3
    assert args["command_timeout"] == 12
    assert args["server_settings"] == {
        "application_name": "hookrelay",
        "statement_timeout": "7000",
        "idle_in_transaction_session_timeout": "60000",
    }
    assert "statement_cache_size" not in args
    assert "prepared_statement_name_func" not in args


def test_pgbouncer_mode_disables_statement_caches_and_untracked_startup_parameters() -> None:
    settings = make_settings(db_pgbouncer=True, db_statement_timeout_ms=7_000)
    args = connect_args(settings)
    # PgBouncer refuses startup parameters it does not track ("unsupported startup
    # parameter: statement_timeout"); application_name is tracked.
    assert args["server_settings"] == {"application_name": "hookrelay"}
    assert args["command_timeout"] == 12  # the client-side bound stays
    assert args["statement_cache_size"] == 0
    assert args["prepared_statement_cache_size"] == 0
    assert args["prepared_statement_name_func"] is prepared_statement_name


def test_prepared_statement_names_are_unique_and_never_asyncpgs_sequence() -> None:
    names = {prepared_statement_name() for _ in range(1_000)}
    assert len(names) == 1_000
    assert all(re.fullmatch(r"__asyncpg_[0-9a-f]{32}__", n) for n in names)
    assert not any(n.startswith("__asyncpg_stmt_") for n in names)


async def test_build_engine_keeps_a_client_side_pool_behind_pgbouncer() -> None:
    engine = build_engine(make_settings(db_pgbouncer=True, db_pool_size=3, db_max_overflow=1))
    try:
        assert engine.pool.size() == 3  # type: ignore[attr-defined]
    finally:
        await engine.dispose()


@pytest.fixture
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)


@pytest.mark.usefixtures("_clean_env")
def test_migrations_never_go_through_the_pooler() -> None:
    direct = make_settings(database_url="postgresql://u:p@db:5432/hr")
    assert direct.migration_dsn == "postgresql+asyncpg://u:p@db:5432/hr"

    pooled = make_settings(database_url="postgresql://u:p@pgbouncer:6432/hr", db_pgbouncer=True)
    with pytest.raises(ValueError, match="DATABASE_DIRECT_URL"):
        _ = pooled.migration_dsn

    both = make_settings(
        database_url="postgresql://u:p@pgbouncer:6432/hr",
        db_pgbouncer=True,
        database_direct_url="postgres://u:p@db:5432/hr",
    )
    assert both.migration_dsn == "postgresql+asyncpg://u:p@db:5432/hr"
    assert both.database_dsn.endswith("@pgbouncer:6432/hr")
    assert "u:p" not in repr(both.database_direct_url)  # a secret, never logged


@pytest.mark.usefixtures("_clean_env")
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_a_blank_direct_url_means_unset(blank: str | None) -> None:
    assert make_settings(database_direct_url=blank).database_direct_url is None


@pytest.mark.usefixtures("_clean_env")
def test_a_non_postgres_direct_url_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_settings(database_direct_url="mysql://u:p@h/d")
