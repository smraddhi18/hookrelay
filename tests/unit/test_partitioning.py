"""Milestone 9c: partition naming, bound parsing, maintenance planning and settings (no DB)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from hookrelay import partitions as partitions_cli
from hookrelay.db.partitioning import (
    DELIVERY_ATTEMPTS,
    RangeBound,
    day_bounds,
    include_name,
    parse_range_bound,
    utc_day,
)
from hookrelay.observability.metrics import SERVICE_RELAY, build_metrics, render_metrics
from hookrelay.services.partitions import (
    Partition,
    PartitionPolicy,
    PartitionReport,
    advisory_key,
    connect_kwargs,
    plan_maintenance,
    quote_ident,
    ts_literal,
)
from tests.conftest import make_settings

NOW = datetime(2026, 11, 14, 4, 30, tzinfo=UTC)


def _daily(day: date, *, pending: bool = False) -> Partition:
    lower, upper = day_bounds(day)
    return Partition(DELIVERY_ATTEMPTS.partition_name(day), RangeBound(lower, upper), pending)


DEFAULT = Partition(DELIVERY_ATTEMPTS.default_partition, None)


def test_names_and_days() -> None:
    assert DELIVERY_ATTEMPTS.partition_name(date(2026, 1, 2)) == "delivery_attempts_p20260102"
    assert DELIVERY_ATTEMPTS.day_of("delivery_attempts_p20260102") == date(2026, 1, 2)
    for other in (
        "delivery_attempts",
        "delivery_attempts_p2026010",
        "delivery_attempts_p20261399",  # not a date
        "delivery_attempts_p20260102_old",
        "xdelivery_attempts_p20260102",
    ):
        assert DELIVERY_ATTEMPTS.day_of(other) is None, other
    assert DELIVERY_ATTEMPTS.is_partition("delivery_attempts_default")
    assert DELIVERY_ATTEMPTS.is_partition("delivery_attempts_p20260102")
    assert not DELIVERY_ATTEMPTS.is_partition("delivery_attempts")


def test_day_bounds_are_utc_midnights() -> None:
    lower, upper = day_bounds(date(2026, 11, 14))
    assert lower == datetime(2026, 11, 14, tzinfo=UTC)
    assert upper - lower == timedelta(days=1)
    ist = timezone(timedelta(hours=5, minutes=30))
    # 01:00 IST on the 15th is still the 14th in UTC.
    assert utc_day(datetime(2026, 11, 15, 1, 0, tzinfo=ist)) == date(2026, 11, 14)


def test_parse_range_bound() -> None:
    bound = parse_range_bound(
        "FOR VALUES FROM ('2026-11-14 00:00:00+00') TO ('2026-11-15 05:30:00+05:30')"
    )
    assert bound == RangeBound(
        datetime(2026, 11, 14, tzinfo=UTC), datetime(2026, 11, 15, tzinfo=UTC)
    )
    assert parse_range_bound("DEFAULT") is None
    assert parse_range_bound("FOR VALUES FROM (MINVALUE) TO ('2026-11-15 00:00:00+00')") is None
    with pytest.raises(ValueError, match="no time zone"):
        parse_range_bound("FOR VALUES FROM ('2026-11-14 00:00:00') TO ('2026-11-15 00:00:00')")
    assert bound is not None
    assert bound.overlaps(*day_bounds(date(2026, 11, 14)))
    assert not bound.overlaps(*day_bounds(date(2026, 11, 15)))


def test_alembic_filter_hides_partitions_only() -> None:
    assert include_name("delivery_attempts", "table", {})
    assert include_name("deliveries", "table", {})
    assert not include_name("delivery_attempts_default", "table", {})
    assert not include_name("delivery_attempts_p20261114", "table", {})
    assert include_name("delivery_attempts_p20261114_pkey", "index", {})
    assert include_name(None, "schema", {})


def _policy(**overrides: object) -> PartitionPolicy:
    base: dict[str, object] = {"premake_days": 3, "retention_days": 30}
    base.update(overrides)
    return PartitionPolicy(**base)  # type: ignore[arg-type]


def test_plan_premakes_today_onwards_and_skips_existing() -> None:
    today = NOW.date()
    plan = plan_maintenance(
        table=DELIVERY_ATTEMPTS,
        partitions=[_daily(today), _daily(today + timedelta(days=2)), DEFAULT],
        now=NOW,
        policy=_policy(),
        default_days=[],
        orphans=[],
    )
    assert plan.create == [today + timedelta(days=1), today + timedelta(days=3)]
    assert plan.retire == []
    assert plan.cutoff == datetime(2026, 10, 15, tzinfo=UTC)


def test_plan_moves_default_rows_only_inside_the_retention_window() -> None:
    today = NOW.date()
    plan = plan_maintenance(
        table=DELIVERY_ATTEMPTS,
        partitions=[_daily(today + timedelta(days=k)) for k in range(4)],
        now=NOW,
        policy=_policy(),
        default_days=[today - timedelta(days=31), today - timedelta(days=2), today + timedelta(9)],
        orphans=[],
    )
    assert plan.create == [today - timedelta(days=2), today + timedelta(days=9)]


def test_plan_retires_whole_days_past_the_cutoff() -> None:
    today = NOW.date()
    old, edge, kept = (today - timedelta(days=d) for d in (31, 30, 29))
    pending = _daily(today - timedelta(days=40), pending=True)
    plan = plan_maintenance(
        table=DELIVERY_ATTEMPTS,
        partitions=[_daily(old), _daily(edge), _daily(kept), pending, DEFAULT],
        now=NOW,
        policy=_policy(premake_days=0, retention_days=30),
        default_days=[],
        orphans=[
            DELIVERY_ATTEMPTS.partition_name(today - timedelta(days=50)),
            DELIVERY_ATTEMPTS.partition_name(today),  # never dropped: inside the window
            "delivery_attempts_px",
        ],
    )
    # old = today - 31 ends at the cutoff (midnight of today - 30): every row in it is more
    # than 30 days old. edge = today - 30 still holds rows younger than 30 days: kept.
    assert [p.name for p in plan.retire] == [DELIVERY_ATTEMPTS.partition_name(old)]
    assert plan.cutoff == day_bounds(edge)[0]
    assert plan.finalize == [pending]
    assert plan.orphans == [DELIVERY_ATTEMPTS.partition_name(today - timedelta(days=50))]
    assert plan.create == [today]


def test_policy_from_settings_and_validation() -> None:
    settings = make_settings(
        partition_premake_days=5, partition_lock_timeout_ms=750, retention_batch_size=9
    )
    policy = PartitionPolicy.from_settings(settings, dry_run=True)
    assert policy.premake_days == 5
    assert policy.retention_days == settings.event_retention_days == 30
    assert policy.lock_timeout_ms == 750
    assert policy.sweep_batch_size == 9
    assert policy.dry_run
    assert make_settings(partition_retention_days=45).partition_retention_days_effective == 45
    with pytest.raises(ValidationError, match="PARTITION_RETENTION_DAYS must be at least"):
        make_settings(event_retention_days=30, partition_retention_days=7)


def test_session_settings_and_helpers() -> None:
    kwargs = connect_kwargs(_policy(lock_timeout_ms=1234), application_name="hr-partitions")
    assert kwargs["server_settings"]["lock_timeout"] == "1234"
    assert kwargs["server_settings"]["TimeZone"] == "UTC"
    assert advisory_key(DELIVERY_ATTEMPTS) == "hookrelay.partitions:delivery_attempts"
    assert quote_ident('we"ird') == '"we""ird"'
    assert ts_literal(NOW) == "'2026-11-14T04:30:00+00:00'::timestamptz"
    dsn = partitions_cli.asyncpg_dsn("postgresql+asyncpg://u:p@h:5432/db")
    assert dsn == "postgresql://u:p@h:5432/db"


def test_report_as_log() -> None:
    report = PartitionReport(table="delivery_attempts", created=["a"])
    log = report.as_log()
    assert log["created"] == ["a"]
    assert log["premade_until"] is None
    report.premade_until = report.cutoff = NOW
    assert report.as_log()["cutoff"] == "2026-11-14T04:30:00+00:00"


def test_partition_gauges_render() -> None:
    metrics = build_metrics(SERVICE_RELAY)
    metrics.set_partition_state(
        table="delivery_attempts", partitions=8, premade_seconds=3600.5, default_rows=2
    )
    body = render_metrics(metrics)[0].decode()
    assert 'hookrelay_partitions{service="relay",table="delivery_attempts"} 8.0' in body
    assert (
        'hookrelay_partition_premade_seconds{service="relay",table="delivery_attempts"} 3600.5'
        in body
    )
    assert 'hookrelay_partition_default_rows{service="relay",table="delivery_attempts"} 2.0' in body


def test_cli_rejects_unknown_flags(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit):
        partitions_cli.main(["--nope"])
    assert "--dry-run" in capsys.readouterr().err
