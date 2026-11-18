"""Partition maintenance for ``events``, ``deliveries`` and ``delivery_attempts``: premake,
retire, sweep (Milestones 9c / 9d).

One run (``hookrelay-partitions``, an hourly CronJob) works through the three tables of the
event graph (:data:`hookrelay.db.partitioning.PARTITIONED_TABLES`), each under its own advisory
lock, and does, in order:

1. **finalize** partitions an interrupted ``DETACH ... CONCURRENTLY`` left "detach pending";
2. **premake** a daily partition for today .. today + ``premake_days`` (UTC), parents first
   (``events``, ``deliveries``, ``delivery_attempts``). For ``delivery_attempts``, which no
   foreign key references, every day that has rows in the ``DEFAULT`` partition (a missed run
   let inserts land there) gets a partition too: it is created standalone (``LIKE`` the parent,
   with a CHECK matching its bound so ``ATTACH`` needs no validation scan), those rows are
   moved into it, and it is attached, all in one transaction. For ``events`` and
   ``deliveries`` the move is impossible: it is a ``DELETE`` + ``INSERT`` and the ``DELETE``
   would cascade into the referencing rows. A day that has rows in their ``DEFAULT`` is
   reported as *blocked* (``blocked_days``, a warning, an incomplete run) instead, and those rows
   expire through the ``DEFAULT`` sweep (step 4) or the row-by-row backstop purge, unless the
   run has ``repair_default`` (Milestone 9e): then, before planning and only while it holds all
   three advisory locks, it re-homes those days cascade-safely
   (:mod:`hookrelay.services.partition_repair`). ``ATTACH``
   takes ``SHARE UPDATE EXCLUSIVE`` on the parent, so inserts and reads keep going
   (``CREATE TABLE ... PARTITION OF`` would take ``ACCESS EXCLUSIVE``);
3. **retire** every partition whose whole day is older than ``retention_days``, children first
   (``delivery_attempts``, ``deliveries``, ``events``): ``DETACH`` then ``DROP``. All three
   tables of a day expire together because the partition key is the *event's* creation time, and
   Postgres refuses to detach a referenced ``events`` / ``deliveries`` partition while a
   referencing row is left (an ``ForeignKeyViolationError``, reported as ``blocked``: the child
   drop of that day was deferred). Postgres refuses ``DETACH ... CONCURRENTLY`` while a
   ``DEFAULT`` partition exists, so with one (the default layout) the detach is the plain form,
   which needs a brief ``ACCESS EXCLUSIVE`` on the parent; without a default partition it is
   ``CONCURRENTLY`` (never blocks readers or writers). Tables a crashed run detached but did
   not drop are dropped too (only names this module generates, only past the cutoff);
4. **sweep** rows older than the cutoff out of the ``DEFAULT`` partition in bounded batches.

Every DDL statement runs under ``lock_timeout`` (``PARTITION_LOCK_TIMEOUT_MS``): it either gets
its lock quickly or gives up, instead of queueing every insert behind it, and is retried with
backoff ``PARTITION_DDL_RETRIES`` times; a statement that still cannot get its lock is reported
(``deferred``) and the run exits incomplete. Runs are single-flight through session-level
advisory locks, so the connection must go to Postgres itself (``migration_dsn``), never through
PgBouncer's transaction pooling.

The relay runs the same code with ``premake_only`` as a second safety net against a dead
CronJob (:func:`ensure_premade`): it never retires or sweeps.

Why the retention cannot lose data an event still needs: every row of a partition belongs to an
event created that day, so a partition whose upper bound is older than the event retention
only holds expired events (the settings refuse a partition retention below the event retention).
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Any, TypeVar

import asyncpg
import structlog

from hookrelay.core.config import Settings
from hookrelay.db.partitioning import (
    DELIVERY_ATTEMPTS,
    PARTITIONED_TABLES,
    PartitionedTable,
    RangeBound,
    day_bounds,
    parse_range_bound,
    utc_day,
)
from hookrelay.services.partition_repair import REPAIRED, WOULD_REPAIR, repair_defaults

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
    premake_only: bool = False  # the relay's safety net: never retire, never sweep
    # Milestone 9e: re-home rows stranded in the events / deliveries DEFAULT partitions
    # (hookrelay.services.partition_repair), at most this many days and rows per day per run.
    repair_default: bool = False
    repair_max_days: int = 7
    repair_max_rows: int = 50_000

    @classmethod
    def from_settings(
        cls, settings: Settings, *, dry_run: bool = False, repair_default: bool | None = None
    ) -> PartitionPolicy:
        return cls(
            premake_days=settings.partition_premake_days,
            retention_days=settings.partition_retention_days_effective,
            lock_timeout_ms=settings.partition_lock_timeout_ms,
            ddl_retries=settings.partition_ddl_retries,
            sweep_batch_size=settings.retention_batch_size,
            sweep_max_batches=settings.retention_max_batches,
            dry_run=dry_run,
            repair_default=(
                settings.partition_repair_default if repair_default is None else repair_default
            ),
            repair_max_days=settings.partition_repair_max_days,
            repair_max_rows=settings.partition_repair_max_rows,
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
    blocked_days: list[str] = field(default_factory=list)  # DEFAULT rows prevent the partition
    blocked: list[str] = field(default_factory=list)  # detach refused: referencing rows left
    # Milestone 9e repair of stranded DEFAULT rows (only with repair_default):
    repaired_days: list[str] = field(default_factory=list)  # days this table was the root of
    repaired_rows: int = 0  # rows of this table re-homed (as root or as part of a component)
    repair_planned: list[str] = field(default_factory=list)  # dry run: days it would repair
    repair_skipped: list[str] = field(default_factory=list)  # "<day> <status>": left in DEFAULT
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
            "blocked_days": self.blocked_days,
            "blocked": self.blocked,
            "repaired_days": self.repaired_days,
            "repaired_rows": self.repaired_rows,
            "repair_planned": self.repair_planned,
            "repair_skipped": self.repair_skipped,
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
    blocked: list[date] = field(default_factory=list)


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
    blocked: list[date] = []
    if table.movable:
        wanted.update(d for d in default_days if day_bounds(d)[1] > cutoff)
    else:
        # Rows of that day sit in DEFAULT and cannot be moved (see the module docstring):
        # creating the partition would fail, so it is reported instead.
        blocked = sorted(wanted & set(default_days))
        wanted -= set(blocked)
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
    return Plan(now, cutoff, create, retire, finalize, stale_orphans, blocked)


class PartitionMaintainer:
    """One maintenance run over one partitioned table on one dedicated connection.

    :meth:`run` does everything for this table. :func:`maintain` drives several tables through
    the phases (:meth:`premake_phase` parents first, :meth:`retire_phase` children first) so the
    cross-table order the foreign keys need holds."""

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
        self.report = PartitionReport(table=table.name, dry_run=policy.dry_run)
        self._started = time.monotonic()
        self._plan: Plan | None = None
        self._default: Partition | None = None

    async def run(self) -> PartitionReport:
        if not await self.acquire():
            return self.report
        try:
            await self.prepare()
            await self.premake_phase()
            await self.retire_phase()
            await self.summarise()
        finally:
            await self.release()
        return self.report

    # --- phases -------------------------------------------------------------------------

    async def acquire(self) -> bool:
        """Take this table's advisory lock (single-flight); ``False`` if another run has it."""
        if await self.conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", advisory_key(self.table)
        ):
            return True
        self.report.skipped_locked = True
        logger.info("partitions.skipped_locked", table=self.table.name)
        return False

    async def release(self) -> None:
        await self.conn.execute(
            "SELECT pg_advisory_unlock(hashtextextended($1, 0))", advisory_key(self.table)
        )
        self.report.elapsed_ms = int((time.monotonic() - self._started) * 1000)

    async def prepare(self) -> None:
        report = self.report
        partitions = await list_partitions(self.conn, self.table)
        self._default = default = next((p for p in partitions if p.is_default), None)
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
        self._plan = plan = plan_maintenance(
            table=self.table,
            partitions=partitions,
            now=now,
            policy=self.policy,
            default_days=default_days,
            orphans=orphans,
        )
        report.cutoff = plan.cutoff
        if plan.blocked:
            report.blocked_days = [d.isoformat() for d in plan.blocked]
            report.complete = False
            logger.warning(
                "partitions.blocked_by_default_rows",
                table=self.table.name,
                days=report.blocked_days,
                hint=(
                    "rows of these days sit in the DEFAULT partition and cannot be moved "
                    "(foreign keys reference this table); see docs/partitioning.md"
                ),
            )
        if self.policy.dry_run:
            report.created = [self.table.partition_name(d) for d in plan.create]
            report.finalized = [p.name for p in plan.finalize]
            if not self.policy.premake_only:
                report.detached = [p.name for p in plan.retire]
                report.dropped = [p.name for p in plan.retire] + plan.orphans

    async def premake_phase(self) -> None:
        plan = self._plan
        if plan is None or self.policy.dry_run:
            return
        if not self.policy.premake_only:
            await self._finalize(plan.finalize, self.report)
        await self._premake(plan.create, self._default, self.report)

    async def retire_phase(self) -> None:
        plan = self._plan
        if plan is None or self.policy.dry_run or self.policy.premake_only:
            return
        await self._retire(
            plan.retire,
            plan.orphans,
            has_default=self._default is not None,
            report=self.report,
        )
        if self._default is not None:
            await self._sweep_default(self._default, plan.cutoff, self.report)

    async def summarise(self) -> None:
        await self._summarise(self.report, self._default)

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
                functools.partial(
                    self._create_partition, day, default if self.table.movable else None, columns
                ),
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
            except asyncpg.exceptions.ForeignKeyViolationError as exc:
                # Detaching a referenced events / deliveries partition while a referencing row
                # is left: that day's child drop was deferred. Not retryable inside this run.
                logger.warning(
                    "partitions.detach_blocked",
                    table=self.table.name,
                    step=what,
                    detail=exc.detail,
                )
                report.blocked.append(name)
                report.complete = False
                return False, None
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


async def maintain(
    conn: asyncpg.Connection,
    policy: PartitionPolicy,
    *,
    tables: Sequence[PartitionedTable] = PARTITIONED_TABLES,
    should_stop: Callable[[], bool] = lambda: False,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> list[PartitionReport]:
    """Maintain ``tables`` (parents first in ``tables``) in the order their foreign keys need.

    Premake runs parents first, retirement children first, so one day of the event graph is
    dropped child-to-parent and Postgres' own check (no row still references a detached
    partition) never fires unless a child drop was deferred. Tables whose advisory lock another
    run holds are skipped (their report says so). Raises :class:`NotPartitionedError` before
    changing anything if a table is not partitioned."""
    maintainers = [
        PartitionMaintainer(conn, policy, table=t, should_stop=should_stop, sleep=sleep)
        for t in tables
    ]
    held: list[PartitionMaintainer] = []
    # Milestone 9e: the repair needs all three table locks. Only the holder of the repair gate
    # waits for the ones another run holds (every other acquirer only tries, so the waiting
    # cannot deadlock); without the gate two overlapping runs could each hold part of the set
    # and neither repair.
    repair = policy.repair_default and not policy.premake_only
    gate = repair and await _try_advisory_lock(conn, REPAIR_GATE_KEY)
    try:
        for m in maintainers:
            if await m.acquire() or (gate and await _acquire_patiently(m, policy, sleep)):
                held.append(m)  # noqa: PERF401 - awaits in the condition
        if repair:
            await _repair_phase(
                conn,
                policy,
                maintainers=maintainers,
                held=held if gate else [],  # no gate: another run is repairing
                should_stop=should_stop,
                sleep=sleep,
            )
        for m in held:
            await m.prepare()
        for m in held:
            await m.premake_phase()
        for m in reversed(held):
            await m.retire_phase()
        for m in held:
            await m.summarise()
    finally:
        for m in reversed(held):
            await m.release()
        if gate:
            await conn.execute(
                "SELECT pg_advisory_unlock(hashtextextended($1, 0))", REPAIR_GATE_KEY
            )
    return [m.report for m in maintainers]


REPAIR_GATE_KEY = "hookrelay.partitions:repair"


async def _try_advisory_lock(conn: asyncpg.Connection, key: str) -> bool:
    return bool(await conn.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1, 0))", key))


async def _acquire_patiently(
    m: PartitionMaintainer,
    policy: PartitionPolicy,
    sleep: Callable[[float], Awaitable[None]],
) -> bool:
    """Retry a table lock another run holds, with the DDL backoff (the repair gate holder
    only): other runs finish or skip, so the lock frees up."""
    delay = max(policy.retry_base_s, 0.05)
    for _ in range(policy.ddl_retries):
        await sleep(delay)
        delay = min(delay * 2, policy.retry_max_s)
        if await _try_advisory_lock(m.conn, advisory_key(m.table)):
            m.report.skipped_locked = False
            logger.info("partitions.lock_acquired_for_repair", table=m.table.name)
            return True
    return False


async def _repair_phase(
    conn: asyncpg.Connection,
    policy: PartitionPolicy,
    *,
    maintainers: list[PartitionMaintainer],
    held: list[PartitionMaintainer],
    should_stop: Callable[[], bool],
    sleep: Callable[[float], Awaitable[None]],
) -> None:
    """Re-home stranded DEFAULT rows before planning (so the plan sees the new partitions).

    Needs every table's advisory lock: a component spans all three tables. If another runner
    holds one, it is that runner's job (or the next run's); nothing is reported as repaired."""
    reports = {m.table.name: m.report for m in maintainers}
    if {m.table.name for m in held} != {t.name for t in PARTITIONED_TABLES}:
        logger.info("partitions.repair_skipped_locked")
        return
    for result in await repair_defaults(conn, policy, should_stop=should_stop, sleep=sleep):
        root = reports[result.table]
        day = result.day.isoformat()
        if result.status == WOULD_REPAIR:
            root.repair_planned.append(day)
        elif result.status == REPAIRED:
            root.repaired_days.append(day)
            for table, n in result.rows.items():
                if table in reports:
                    reports[table].repaired_rows += n
        else:
            root.repair_skipped.append(f"{day} {result.status}")
            root.complete = False


async def ensure_premade(
    conn: asyncpg.Connection, policy: PartitionPolicy, *, min_days_ahead: int
) -> list[PartitionReport]:
    """The relay's safety net: make sure every table has partitions through today +
    ``min_days_ahead`` (UTC), premaking under the same advisory locks the CronJob uses.

    Never retires or sweeps, so a healthy CronJob and the relay cannot disagree: whoever holds
    the lock does the (idempotent) premake, the other skips."""
    quick = replace(policy, premake_days=min_days_ahead, premake_only=True)
    return await maintain(conn, quick)
