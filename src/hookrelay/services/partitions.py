"""Partition maintenance for ``delivery_attempts`` (Milestone 9c): premake, retire, sweep.

One run (``hookrelay-partitions``, an hourly CronJob) does, in order:

1. **finalize** partitions an interrupted ``DETACH ... CONCURRENTLY`` left "detach pending";
2. **premake** a daily partition for today .. today + ``premake_days`` (UTC), and for every
   day that has rows in the ``DEFAULT`` partition (a missed run let inserts land there): the
   new partition is created standalone (``LIKE`` the parent, with a CHECK matching its bound
   so ``ATTACH`` needs no validation scan), those rows are moved into it, and it is attached,
   all in one transaction. ``ATTACH`` takes ``SHARE UPDATE EXCLUSIVE`` on the parent, so
   inserts and reads keep going (``CREATE TABLE ... PARTITION OF`` would take ``ACCESS
   EXCLUSIVE``);
3. **retire** every partition whose whole day is older than ``retention_days``: ``DETACH``
   then ``DROP``. Postgres refuses ``DETACH ... CONCURRENTLY`` while a ``DEFAULT`` partition
   exists, so with one (the default layout) the detach is the plain form, which needs a brief
   ``ACCESS EXCLUSIVE`` on the parent; without a default partition it is ``CONCURRENTLY``
   (never blocks readers or writers). Tables a crashed run detached but did not drop are
   dropped too (only names this module generates, only past the cutoff);
4. **sweep** rows older than the cutoff out of the ``DEFAULT`` partition in bounded batches.

Every DDL statement runs under ``lock_timeout`` (``PARTITION_LOCK_TIMEOUT_MS``): it either gets
its lock quickly or gives up, instead of queueing every attempt insert behind it, and is retried
with backoff ``PARTITION_DDL_RETRIES`` times; a statement that still cannot get its lock is
reported (``deferred``) and the run exits incomplete. Runs are single-flight through a
session-level advisory lock, so the connection must go to Postgres itself (``migration_dsn``),
never through PgBouncer's transaction pooling.

Why the retention cannot lose data an event still needs: an attempt is never older than its
event, so a partition whose upper bound is older than the event retention only holds attempts
of events past ``retain_until`` (the settings refuse a partition retention below the event
retention).
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, TypeVar

import asyncpg
import structlog

from hookrelay.core.config import Settings
from hookrelay.db.partitioning import (
    DELIVERY_ATTEMPTS,
    PartitionedTable,
    RangeBound,
    day_bounds,
    parse_range_bound,
    utc_day,
)

logger = structlog.stdlib.get_logger("hookrelay.partitions")

T = TypeVar("T")

# Bounded probes: the DEFAULT partition is normally empty; never count a huge one exactly.
DEFAULT_ROWS_CAP = 10_000
_MAX_DEFAULT_DAYS = 1_000
_STATEMENT_TIMEOUT_MS = 600_000  # DETACH CONCURRENTLY waits for older transactions to finish

_IS_PARTITIONED_SQL = "SELECT relkind = 'p' FROM pg_class WHERE oid = to_regclass($1)"
_LIST_SQL = """
    SELECT c.relname AS name, pg_get_expr(c.relpartbound, c.oid) AS bound,
           i.inhdetachpending AS pending
    FROM pg_inherits AS i JOIN pg_class AS c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass($1)
    ORDER BY c.relname
"""
# Standalone tables with a partition's name: detached by a run that died before DROP.
_ORPHANS_SQL = """
    SELECT c.relname AS name
    FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema() AND c.relkind = 'r' AND NOT c.relispartition
      AND c.relname LIKE $1 || '\\_p%'
    ORDER BY c.relname
"""
_COLUMNS_SQL = """
    SELECT attname FROM pg_attribute
    WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped
    ORDER BY attnum
"""


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ts_literal(moment: datetime) -> str:
    """A timestamptz SQL literal (DDL takes no bind parameters; values are ours, not input)."""
    return f"'{moment.isoformat()}'::timestamptz"


class NotPartitionedError(RuntimeError):
    """The table is not partitioned (migration 0011 has not run)."""


@dataclass(frozen=True, slots=True)
class Partition:
    name: str
    bound: RangeBound | None  # None = the DEFAULT partition
    pending_detach: bool = False

    @property
    def is_default(self) -> bool:
        return self.bound is None


@dataclass(frozen=True, slots=True)
class PartitionPolicy:
    premake_days: int
    retention_days: int
    lock_timeout_ms: int = 2_000
    ddl_retries: int = 5
    retry_base_s: float = 0.25
    retry_max_s: float = 5.0
    sweep_batch_size: int = 500
    sweep_max_batches: int = 200
    dry_run: bool = False

    @classmethod
    def from_settings(cls, settings: Settings, *, dry_run: bool = False) -> PartitionPolicy:
        return cls(
            premake_days=settings.partition_premake_days,
            retention_days=settings.partition_retention_days_effective,
            lock_timeout_ms=settings.partition_lock_timeout_ms,
            ddl_retries=settings.partition_ddl_retries,
            sweep_batch_size=settings.retention_batch_size,
            sweep_max_batches=settings.retention_max_batches,
            dry_run=dry_run,
        )


@dataclass(slots=True)
class PartitionReport:
    table: str
    dry_run: bool = False
    created: list[str] = field(default_factory=list)
    moved_from_default: int = 0
    finalized: list[str] = field(default_factory=list)
    detached: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)
    default_rows_deleted: int = 0
    default_rows: int = 0  # left in DEFAULT after the run, capped at DEFAULT_ROWS_CAP
    partitions: int = 0  # daily partitions attached after the run
    premade_until: datetime | None = None
    cutoff: datetime | None = None
    detach_mode: str | None = None  # "concurrently" | "lock" (a DEFAULT partition exists)
    skipped_locked: bool = False
    complete: bool = True
    elapsed_ms: int = 0

    def as_log(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "dry_run": self.dry_run,
            "created": self.created,
            "moved_from_default": self.moved_from_default,
            "finalized": self.finalized,
            "detached": self.detached,
            "dropped": self.dropped,
            "deferred": self.deferred,
            "default_rows_deleted": self.default_rows_deleted,
            "default_rows": self.default_rows,
            "partitions": self.partitions,
            "premade_until": None if self.premade_until is None else self.premade_until.isoformat(),
            "cutoff": None if self.cutoff is None else self.cutoff.isoformat(),
            "detach_mode": self.detach_mode,
            "skipped_locked": self.skipped_locked,
            "complete": self.complete,
            "elapsed_ms": self.elapsed_ms,
        }


def advisory_key(table: PartitionedTable) -> str:
    return f"hookrelay.partitions:{table.name}"


def connect_kwargs(policy: PartitionPolicy, *, application_name: str) -> dict[str, Any]:
    """Server settings for the maintenance session: UTC (bounds render in UTC), bounded lock
    waits for every DDL statement, and a statement timeout long enough for ``CONCURRENTLY``."""
    return {
        "server_settings": {
            "application_name": application_name,
            "TimeZone": "UTC",
            "lock_timeout": str(policy.lock_timeout_ms),
            "statement_timeout": str(_STATEMENT_TIMEOUT_MS),
        }
    }


async def list_partitions(conn: asyncpg.Connection, table: PartitionedTable) -> list[Partition]:
    if not await conn.fetchval(_IS_PARTITIONED_SQL, table.name):
        msg = f"{table.name} is not a partitioned table: run `alembic upgrade head` first"
        raise NotPartitionedError(msg)
    rows = await conn.fetch(_LIST_SQL, table.name)
    return [
        Partition(name=r["name"], bound=parse_range_bound(r["bound"]), pending_detach=r["pending"])
        for r in rows
    ]


@dataclass(frozen=True, slots=True)
class Plan:
    """What a run would do, computed from the catalog and the database clock."""

    now: datetime
    cutoff: datetime
    create: list[date]
    retire: list[Partition]
    finalize: list[Partition]
    orphans: list[str]


def plan_maintenance(
    *,
    table: PartitionedTable,
    partitions: list[Partition],
    now: datetime,
    policy: PartitionPolicy,
    default_days: list[date],
    orphans: list[str],
) -> Plan:
    """Pure planning (unit-tested): which days to create, which partitions to retire."""
    today = utc_day(now)
    cutoff = day_bounds(today - timedelta(days=policy.retention_days))[0]
    attached = [p for p in partitions if p.bound is not None and not p.pending_detach]
    wanted = {today + timedelta(days=k) for k in range(policy.premake_days + 1)}
    wanted.update(d for d in default_days if day_bounds(d)[1] > cutoff)
    create = sorted(
        day
        for day in wanted
        if not any(p.bound and p.bound.overlaps(*day_bounds(day)) for p in attached)
    )
    retire = [p for p in attached if p.bound is not None and p.bound.upper <= cutoff]
    finalize = [p for p in partitions if p.pending_detach]
    stale_orphans = []
    for name in orphans:
        day = table.day_of(name)
        if day is not None and day_bounds(day)[1] <= cutoff:
            stale_orphans.append(name)
    return Plan(now, cutoff, create, retire, finalize, stale_orphans)


class PartitionMaintainer:
    """One maintenance run over one partitioned table on one dedicated connection."""

    def __init__(
        self,
        conn: asyncpg.Connection,
        policy: PartitionPolicy,
        *,
        table: PartitionedTable = DELIVERY_ATTEMPTS,
        should_stop: Callable[[], bool] = lambda: False,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.conn = conn
        self.policy = policy
        self.table = table
        self._should_stop = should_stop
        self._sleep = sleep

    async def run(self) -> PartitionReport:
        started = time.monotonic()
        report = PartitionReport(table=self.table.name, dry_run=self.policy.dry_run)
        key = advisory_key(self.table)
        if not await self.conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key
        ):
            report.skipped_locked = True
            logger.info("partitions.skipped_locked", table=self.table.name)
            return report
        try:
            await self._run(report)
        finally:
            await self.conn.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", key)
            report.elapsed_ms = int((time.monotonic() - started) * 1000)
        return report

    async def _run(self, report: PartitionReport) -> None:
        partitions = await list_partitions(self.conn, self.table)
        default = next((p for p in partitions if p.is_default), None)
        now: datetime = await self.conn.fetchval("SELECT now()")
        cutoff_probe = day_bounds(utc_day(now) - timedelta(days=self.policy.retention_days))[0]
        default_days: list[date] = []
        if default is not None:
            default_days = [
                r["day"]
                for r in await self.conn.fetch(
                    f"SELECT DISTINCT ({self.table.key} AT TIME ZONE 'UTC')::date AS day "
                    f"FROM ONLY {quote_ident(default.name)} WHERE {self.table.key} >= $1 "
                    f"ORDER BY 1 LIMIT {_MAX_DEFAULT_DAYS}",
                    cutoff_probe,
                )
            ]
        orphans = [r["name"] for r in await self.conn.fetch(_ORPHANS_SQL, self.table.name)]
        plan = plan_maintenance(
            table=self.table,
            partitions=partitions,
            now=now,
            policy=self.policy,
            default_days=default_days,
            orphans=orphans,
        )
        report.cutoff = plan.cutoff
        if self.policy.dry_run:
            report.created = [self.table.partition_name(d) for d in plan.create]
            report.finalized = [p.name for p in plan.finalize]
            report.detached = [p.name for p in plan.retire]
            report.dropped = [p.name for p in plan.retire] + plan.orphans
        else:
            await self._finalize(plan.finalize, report)
            await self._premake(plan.create, default, report)
            await self._retire(
                plan.retire, plan.orphans, has_default=default is not None, report=report
            )
            if default is not None:
                await self._sweep_default(default, plan.cutoff, report)
        await self._summarise(report, default)

    # --- steps --------------------------------------------------------------------------

    async def _finalize(self, pending: list[Partition], report: PartitionReport) -> None:
        for p in pending:
            if self._stopped(report):
                return
            ok, _ = await self._with_retries(
                f"finalize {p.name}",
                functools.partial(
                    self.conn.execute,
                    f"ALTER TABLE {quote_ident(self.table.name)} "
                    f"DETACH PARTITION {quote_ident(p.name)} FINALIZE",
                ),
                report,
                p.name,
            )
            if ok:
                report.finalized.append(p.name)
                await self._drop(p.name, report)

    async def _premake(
        self, days: list[date], default: Partition | None, report: PartitionReport
    ) -> None:
        columns = ", ".join(
            quote_ident(r["attname"]) for r in await self.conn.fetch(_COLUMNS_SQL, self.table.name)
        )
        for day in days:
            if self._stopped(report):
                return
            name = self.table.partition_name(day)
            ok, moved = await self._with_retries(
                f"create {name}",
                functools.partial(self._create_partition, day, default, columns),
                report,
                name,
            )
            if ok and moved is not None:
                report.created.append(name)
                report.moved_from_default += moved
                logger.info(
                    "partitions.created", table=self.table.name, partition=name, moved=moved
                )

    async def _create_partition(self, day: date, default: Partition | None, columns: str) -> int:
        lower, upper = day_bounds(day)
        name = self.table.partition_name(day)
        parent, part, key = quote_ident(self.table.name), quote_ident(name), self.table.key
        check = quote_ident(f"{name}_bound")
        bound = f"FROM ({ts_literal(lower)}) TO ({ts_literal(upper)})"
        moved = 0
        async with self.conn.transaction():
            await self.conn.execute(
                f"CREATE TABLE {part} (LIKE {parent} INCLUDING DEFAULTS INCLUDING CONSTRAINTS "
                f"INCLUDING STORAGE)"
            )
            # Lets ATTACH skip scanning the new table to prove the bound.
            await self.conn.execute(
                f"ALTER TABLE {part} ADD CONSTRAINT {check} "
                f"CHECK ({key} >= {ts_literal(lower)} AND {key} < {ts_literal(upper)})"
            )
            if default is not None:
                # ATTACH fails while DEFAULT holds rows of the new range: move them first.
                moved = int(
                    await self.conn.fetchval(
                        f"WITH moved AS (DELETE FROM {quote_ident(default.name)} "
                        f"WHERE {key} >= $1 AND {key} < $2 RETURNING {columns}), "
                        f"ins AS (INSERT INTO {part} ({columns}) SELECT {columns} FROM moved "
                        f"RETURNING 1) SELECT count(*) FROM ins",
                        lower,
                        upper,
                    )
                )
            await self.conn.execute(
                f"ALTER TABLE {parent} ATTACH PARTITION {part} FOR VALUES {bound}"
            )
            await self.conn.execute(f"ALTER TABLE {part} DROP CONSTRAINT {check}")
        return moved

    async def _retire(
        self,
        partitions: list[Partition],
        orphans: list[str],
        *,
        has_default: bool,
        report: PartitionReport,
    ) -> None:
        parent = quote_ident(self.table.name)
        # Postgres: "cannot detach partitions concurrently when a default partition exists".
        mode = "" if has_default else " CONCURRENTLY"
        if partitions:
            report.detach_mode = "lock" if has_default else "concurrently"
        for p in partitions:
            if self._stopped(report):
                return
            ok, _ = await self._with_retries(
                f"detach {p.name}",
                functools.partial(self._detach, parent, p.name, mode),
                report,
                p.name,
            )
            if ok:
                report.detached.append(p.name)
                logger.info(
                    "partitions.detached",
                    table=self.table.name,
                    partition=p.name,
                    concurrently=bool(mode),
                )
                await self._drop(p.name, report)
        for name in orphans:
            if self._stopped(report):
                return
            await self._drop(name, report)

    async def _detach(self, parent: str, name: str, mode: str) -> None:
        # A concurrent detach interrupted in its second transaction (lock timeout, cancel)
        # leaves the partition "detach pending"; only FINALIZE completes it.
        if mode and await self._is_pending(name):
            await self.conn.execute(
                f"ALTER TABLE {parent} DETACH PARTITION {quote_ident(name)} FINALIZE"
            )
            return
        await self.conn.execute(f"ALTER TABLE {parent} DETACH PARTITION {quote_ident(name)}{mode}")

    async def _is_pending(self, name: str) -> bool:
        return bool(
            await self.conn.fetchval(
                "SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = to_regclass($1)", name
            )
        )

    async def _drop(self, name: str, report: PartitionReport) -> None:
        ok, _ = await self._with_retries(
            f"drop {name}",
            functools.partial(self.conn.execute, f"DROP TABLE IF EXISTS {quote_ident(name)}"),
            report,
            name,
        )
        if ok:
            report.dropped.append(name)
            logger.info("partitions.dropped", table=self.table.name, partition=name)

    async def _sweep_default(
        self, default: Partition, cutoff: datetime, report: PartitionReport
    ) -> None:
        sql = (
            f"DELETE FROM {quote_ident(default.name)} WHERE ctid = ANY(ARRAY("
            f"SELECT ctid FROM {quote_ident(default.name)} WHERE {self.table.key} < $1 "
            f"LIMIT $2))"
        )
        size = self.policy.sweep_batch_size
        for _ in range(self.policy.sweep_max_batches):
            if self._stopped(report):
                return
            ok, status = await self._with_retries(
                "sweep default",
                functools.partial(self.conn.execute, sql, cutoff, size),
                report,
                default.name,
            )
            if not ok or status is None:
                return
            deleted = int(status.split()[-1])
            report.default_rows_deleted += deleted
            if deleted < size:
                return
        report.complete = False

    async def _summarise(self, report: PartitionReport, default: Partition | None) -> None:
        after = await list_partitions(self.conn, self.table)
        daily = [p for p in after if p.bound is not None and not p.pending_detach]
        report.partitions = len(daily)
        report.premade_until = max((p.bound.upper for p in daily if p.bound), default=None)
        if default is not None:
            report.default_rows = int(
                await self.conn.fetchval(
                    f"SELECT count(*) FROM (SELECT 1 FROM ONLY {quote_ident(default.name)} "
                    f"LIMIT {DEFAULT_ROWS_CAP}) AS s"
                )
            )
            if report.default_rows:
                logger.warning(
                    "partitions.default_rows",
                    table=self.table.name,
                    rows=report.default_rows,
                    hint="partition maintenance fell behind; this run moved what it could",
                )

    # --- helpers ------------------------------------------------------------------------

    def _stopped(self, report: PartitionReport) -> bool:
        if self._should_stop():
            report.complete = False
            return True
        return False

    async def _with_retries(
        self,
        what: str,
        step: Callable[[], Awaitable[T]],
        report: PartitionReport,
        name: str,
    ) -> tuple[bool, T | None]:
        """Run one DDL step; retry lock timeouts with capped exponential backoff.

        ``(True, result)`` on success; ``(False, None)`` once the retries are spent (the step
        is reported as deferred and the run as incomplete)."""
        delay = self.policy.retry_base_s
        for attempt in range(self.policy.ddl_retries + 1):
            try:
                return True, await step()
            except asyncpg.exceptions.LockNotAvailableError:
                logger.warning(
                    "partitions.lock_timeout",
                    table=self.table.name,
                    step=what,
                    attempt=attempt + 1,
                    lock_timeout_ms=self.policy.lock_timeout_ms,
                )
                if attempt == self.policy.ddl_retries:
                    break
                await self._sleep(delay)
                delay = min(delay * 2, self.policy.retry_max_s)
        report.deferred.append(name)
        report.complete = False
        return False, None
