"""Native range partitioning: naming, bounds and the Alembic autogenerate filter (Milestone 9c).

``delivery_attempts`` is ``PARTITION BY RANGE (created_at)`` with one partition per UTC day
(``delivery_attempts_p20261114`` holds ``[2026-11-14 00:00Z, 2026-11-15 00:00Z)``) plus a
``DEFAULT`` partition (``delivery_attempts_default``) that catches any row no daily partition
covers, so an insert never fails because partition maintenance fell behind. Migration 0011
creates the layout; :mod:`hookrelay.services.partitions` keeps partitions premade ahead and
retires old ones.

Partitions are not in ``Base.metadata`` (they are runtime objects, not schema), so the
autogenerate filter :func:`include_name` hides them from ``alembic check``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Final


@dataclass(frozen=True, slots=True)
class PartitionedTable:
    """A table range-partitioned by a timestamptz column into UTC days."""

    name: str
    key: str

    @property
    def default_partition(self) -> str:
        return f"{self.name}_default"

    def partition_name(self, day: date) -> str:
        return f"{self.name}_p{day:%Y%m%d}"

    def day_of(self, partition: str) -> date | None:
        """The day a daily partition name stands for (``None`` for any other name)."""
        match = re.fullmatch(re.escape(self.name) + r"_p(\d{8})", partition)
        if match is None:
            return None
        try:
            return datetime.strptime(match.group(1), "%Y%m%d").replace(tzinfo=UTC).date()
        except ValueError:
            return None

    def is_partition(self, table: str) -> bool:
        return table == self.default_partition or self.day_of(table) is not None


DELIVERY_ATTEMPTS: Final = PartitionedTable("delivery_attempts", "created_at")
PARTITIONED_TABLES: Final = (DELIVERY_ATTEMPTS,)


def day_bounds(day: date) -> tuple[datetime, datetime]:
    """``[day 00:00 UTC, next day 00:00 UTC)``."""
    lower = datetime.combine(day, time.min, tzinfo=UTC)
    return lower, lower + timedelta(days=1)


def utc_day(moment: datetime) -> date:
    return moment.astimezone(UTC).date()


_BOUND_RE: Final = re.compile(r"FOR VALUES FROM \('([^']+)'\) TO \('([^']+)'\)")


@dataclass(frozen=True, slots=True)
class RangeBound:
    lower: datetime
    upper: datetime

    def overlaps(self, lower: datetime, upper: datetime) -> bool:
        return self.lower < upper and lower < self.upper


def parse_range_bound(expr: str) -> RangeBound | None:
    """Parse ``pg_get_expr(relpartbound, oid)`` of a timestamptz range partition.

    ``None`` for ``DEFAULT`` (and for MINVALUE / MAXVALUE bounds, which this code never
    creates). Postgres renders the bounds in the session time zone with an explicit offset,
    so the parsed values are exact whatever ``TimeZone`` is.
    """
    match = _BOUND_RE.fullmatch(expr.strip())
    if match is None:
        return None
    return RangeBound(_parse_ts(match.group(1)), _parse_ts(match.group(2)))


def _parse_ts(raw: str) -> datetime:
    value = datetime.fromisoformat(raw)
    if value.tzinfo is None:  # timestamptz always renders an offset; be strict anyway
        msg = f"partition bound {raw!r} has no time zone"
        raise ValueError(msg)
    return value


def include_name(name: str | None, type_: str, _parent_names: object) -> bool:
    """Alembic ``include_name`` hook: skip partitions (and their per-partition indexes)."""
    if type_ == "table" and name is not None:
        return not any(table.is_partition(name) for table in PARTITIONED_TABLES)
    return True
