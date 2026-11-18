"""Cascade-safe repair of rows stranded in the ``events`` / ``deliveries`` DEFAULT partitions
(Milestone 9e).

Rows land in a DEFAULT partition only when their day's partition was missing (the maintenance
CronJob *and* the relay safety net were down past the premake horizon, or a clock mistake).
Postgres then refuses to create that day's partition (``ATTACH`` finds matching rows in the
DEFAULT), and for ``delivery_attempts`` (a leaf) the maintenance job simply moves the rows. For
``events`` and ``deliveries`` a move is a ``DELETE`` + ``INSERT`` and the ``DELETE`` fires
``ON DELETE CASCADE`` into the rows that reference them. Referential actions cannot be
deferred, and turning FK triggers off (``session_replication_role = replica``) needs a
superuser, so this module does not try to dodge the cascade: it lets it happen *inside the
same transaction* after taking a copy of everything it will remove, and puts it all back.

One stranded day of one root table (``events``, else ``deliveries`` when only the deliveries of
that day are stranded) is repaired in **one transaction**:

1. ``LOCK`` every table of the component (found in step 2's catalog walk: ``events``,
   ``deliveries``, ``event_outbox``, ``delivery_attempts``) in ``SHARE ROW EXCLUSIVE``,
   parents first, before anything else. Readers keep going; writers to the event graph wait
   until commit. ``ATTACH`` needs these locks anyway (it takes ``SHARE ROW EXCLUSIVE`` on the
   tables on the other side of every FK), and taking them late would deadlock with a writer
   that holds a table lock while waiting for one of this transaction's row locks. Taken first,
   the transaction only waits (bounded by ``lock_timeout``) while it holds nothing.
2. Walk the foreign keys that reference the root, from the catalog (``pg_constraint``), and copy
   the day's connected component into ``ON COMMIT DROP`` temp tables, parents first, with
   ``FOR UPDATE`` on every source row (rows in already-existing day partitions and in plain
   tables such as ``event_outbox`` are row-locked, so nothing can modify, delete or newly
   reference them before commit). A referencing FK that is not ``ON DELETE CASCADE`` and has
   matching rows makes the day ``unsupported`` (the delete would fail or null them); a table
   reached twice makes the whole repair ``unsupported``. The component must fit
   ``PARTITION_REPAIR_MAX_ROWS`` or the day is skipped (``too_large``): a day can only move as
   a unit, because ``ATTACH`` needs the DEFAULT free of the whole range.
3. ``DELETE`` the root's rows of that day from its DEFAULT; the cascade removes the component.
   The deleted count must equal the copied count, else the transaction is rolled back.
4. Stage the missing day partitions: each is created standalone (``LIKE`` the parent, plus a
   CHECK matching its bound) and filled straight from its copy. A standalone table has no
   triggers and no FKs yet, so this is a plain bulk insert (the copies already hold the
   derived columns), not one trigger + FK check per row.
5. Parents first, ``ATTACH`` each staged partition (Postgres validates its FKs against the
   parents in one set-based check, and the CHECK lets it skip the bound scan) or, for a table
   whose day partition already existed and for plain tables such as ``event_outbox``,
   re-insert the copy through the parent table (routing, triggers, FK checks;
   ``OVERRIDING SYSTEM VALUE`` keeps identity values). Every inserted count must equal its
   copied count. ``ATTACH`` takes ``ACCESS EXCLUSIVE`` on the DEFAULT partition until commit,
   so this last step is kept short: on the build box it was 0.6 s of a 2.0 s repair of a
   50,000-row day (``loadtest/partition_repair_bench.py``).

Any error rolls the whole day back: either the component moved completely or nothing changed.
Lock timeouts, deadlocks and a raced count are retried with capped backoff, then the day is
reported ``deferred``. The caller (:func:`hookrelay.services.partitions.maintain`) runs this
only while it holds the advisory locks of all three tables, so it is single-flight with the
CronJob, the relay's premake and other repairs.

A worker holding a lease on a delivery of the component makes the repair wait for its row lock
(bounded by ``lock_timeout``). A worker whose completion arrives after the commit finds its
row gone from the old partition and treats it as a lost lease (fenced writes), so the delivery
is retried after the lease expires: at-least-once, never lost.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import asyncpg
import structlog

from hookrelay.db.partitioning import (
    PARTITIONED_TABLES,
    PartitionedTable,
    day_bounds,
    utc_day,
)

if TYPE_CHECKING:
    from hookrelay.services.partitions import PartitionPolicy

logger = structlog.stdlib.get_logger("hookrelay.partitions")

# Statuses of one day's repair.
REPAIRED = "repaired"
WOULD_REPAIR = "would_repair"  # dry run
TOO_LARGE = "too_large"
UNSUPPORTED = "unsupported"
DEFERRED = "deferred"  # lock timeout / deadlock / raced, retries spent
FAILED = "failed"

_BY_NAME = {t.name: t for t in PARTITIONED_TABLES}
_FK_SQL = """
    SELECT c.conname AS name, c.conrelid::regclass::text AS child,
           c.confrelid::regclass::text AS parent, c.confdeltype::text AS on_delete,
           ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY AS k(n, i)
                 JOIN pg_attribute AS a ON a.attrelid = c.conrelid AND a.attnum = k.n
                 ORDER BY k.i)::text[] AS child_cols,
           ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY AS k(n, i)
                 JOIN pg_attribute AS a ON a.attrelid = c.confrelid AND a.attnum = k.n
                 ORDER BY k.i)::text[] AS parent_cols
    FROM pg_constraint AS c
    WHERE c.contype = 'f' AND c.conparentid = 0 AND c.confrelid = to_regclass($1)
    ORDER BY c.conname
"""
_INSERT_COLUMNS_SQL = """
    SELECT attname FROM pg_attribute
    WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped AND attgenerated = ''
    ORDER BY attnum
"""
_DEFAULT_SQL = """
    SELECT c.relname FROM pg_inherits AS i JOIN pg_class AS c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass($1) AND c.relpartbound IS NOT NULL
      AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
"""


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True, slots=True)
class ForeignKey:
    name: str
    child: str
    parent: str
    child_cols: tuple[str, ...]
    parent_cols: tuple[str, ...]
    on_delete: str  # pg_constraint.confdeltype: c cascade, a no action, r restrict, n / d set

    @property
    def cascades(self) -> bool:
        return self.on_delete == "c"


@dataclass(frozen=True, slots=True)
class Node:
    """One table of a component: ``via`` is the FK it was reached by (``None`` for the root)."""

    table: str
    via: ForeignKey | None
    temp: str


@dataclass(frozen=True, slots=True)
class ComponentPlan:
    nodes: tuple[Node, ...]  # parents first
    guards: tuple[tuple[Node, ForeignKey], ...]  # non-cascading FKs: must have no matching rows


class RepairRefusedError(Exception):
    def __init__(self, status: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


class _RacedError(Exception):
    """A count did not match its copy: someone got in between; roll back and retry."""


@dataclass(slots=True)
class DayRepair:
    table: str  # the root of the component (events or deliveries)
    day: date
    status: str
    rows: dict[str, int] = field(default_factory=dict)  # rows re-homed per table
    detail: str | None = None
    elapsed_ms: int = 0
    phase_ms: dict[str, int] = field(default_factory=dict)  # lock / copy / delete / create / insert

    @property
    def total_rows(self) -> int:
        return sum(self.rows.values())

    def as_log(self) -> dict[str, Any]:
        return {
            "table": self.table,
            "day": self.day.isoformat(),
            "status": self.status,
            "rows": self.rows,
            "total_rows": self.total_rows,
            "detail": self.detail,
            "elapsed_ms": self.elapsed_ms,
            "phase_ms": self.phase_ms,
        }


async def foreign_keys_to(conn: asyncpg.Connection, table: str) -> list[ForeignKey]:
    """The table-level foreign keys referencing ``table`` (per-partition clones excluded)."""
    return [
        ForeignKey(
            name=r["name"],
            child=r["child"],
            parent=r["parent"],
            child_cols=tuple(r["child_cols"]),
            parent_cols=tuple(r["parent_cols"]),
            on_delete=r["on_delete"],
        )
        for r in await conn.fetch(_FK_SQL, table)
    ]


async def component_plan(conn: asyncpg.Connection, root: str) -> ComponentPlan:
    """Breadth-first over the FKs that reference ``root``: every table its delete would cascade
    into, parents first. Raises :class:`RepairRefusedError` for a graph this code cannot copy
    faithfully (a table reachable along two paths)."""
    nodes = [Node(root, None, "_hr_repair_0")]
    guards: list[tuple[Node, ForeignKey]] = []
    seen = {root}
    i = 0
    while i < len(nodes):
        parent = nodes[i]
        for fk in await foreign_keys_to(conn, parent.table):
            if not fk.cascades:
                guards.append((parent, fk))
                continue
            if fk.child in seen:
                msg = f"{fk.child} is reachable from {root} along two paths ({fk.name})"
                raise RepairRefusedError(UNSUPPORTED, msg)
            seen.add(fk.child)
            nodes.append(Node(fk.child, fk, f"_hr_repair_{len(nodes)}"))
        i += 1
    return ComponentPlan(tuple(nodes), tuple(guards))


async def default_partition(conn: asyncpg.Connection, table: str) -> str | None:
    return await conn.fetchval(_DEFAULT_SQL, table)  # type: ignore[no-any-return]


async def stranded_days(
    conn: asyncpg.Connection, table: PartitionedTable, cutoff: datetime, *, limit: int
) -> list[date]:
    """Days (not older than ``cutoff``) with rows in ``table``'s DEFAULT partition."""
    default = await default_partition(conn, table.name)
    if default is None:
        return []
    rows = await conn.fetch(
        f"SELECT DISTINCT ({table.key} AT TIME ZONE 'UTC')::date AS day FROM ONLY {_q(default)} "
        f"WHERE {table.key} >= $1 ORDER BY 1 LIMIT {int(limit)}",
        cutoff,
    )
    return [r["day"] for r in rows]


def _match(fk: ForeignKey, child: str = "c", parent: str = "p") -> str:
    return " AND ".join(
        f"{child}.{_q(cc)} = {parent}.{_q(pc)}"
        for cc, pc in zip(fk.child_cols, fk.parent_cols, strict=True)
    )


def _ts(moment: datetime) -> str:
    """A timestamptz literal for DDL (no bind parameters there; the value is ours)."""
    return f"'{moment.isoformat()}'::timestamptz"


def _rowcount(status: str) -> int:
    return int(status.rsplit(maxsplit=1)[-1])


class _DayRepairer:
    """Repairs one day of one root table on ``conn`` (see the module docstring)."""

    def __init__(
        self,
        conn: asyncpg.Connection,
        root: PartitionedTable,
        day: date,
        plan: ComponentPlan,
        *,
        max_rows: int,
        dry_run: bool,
    ) -> None:
        self.conn = conn
        self.root = root
        self.day = day
        self.plan = plan
        self.max_rows = max_rows
        self.dry_run = dry_run
        self.lower, self.upper = day_bounds(day)
        self.phase_ms: dict[str, int] = {}
        self._mark = time.monotonic()

    def _phase(self, name: str) -> None:
        now = time.monotonic()
        self.phase_ms[name] = int((now - self._mark) * 1000)
        self._mark = now

    async def run(self) -> dict[str, int]:
        tx = self.conn.transaction()
        await tx.start()
        try:
            rows = await self._repair()
        except BaseException:
            await tx.rollback()
            raise
        if self.dry_run:
            await tx.rollback()  # the copies were only counted
        else:
            await tx.commit()
        return rows

    async def _repair(self) -> dict[str, int]:
        defaults = await self._lock_defaults()
        root_default = defaults.get(self.root.name)
        if root_default is None:
            return {}
        self._phase("lock")
        where = f"{_q(self.root.key)} >= $1 AND {_q(self.root.key)} < $2"
        rows = await self._snapshot(root_default, where)
        self._phase("copy")
        if not rows or self.dry_run:
            return rows
        bounds = (self.lower, self.upper)
        deleted = _rowcount(
            await self.conn.execute(f"DELETE FROM ONLY {_q(root_default)} WHERE {where}", *bounds)
        )
        if deleted != rows[self.root.name]:
            raise _RacedError
        self._phase("delete")
        staged = await self._stage_partitions(rows)
        self._phase("stage")
        # Parents first: an attached partition's FKs are validated against its (already
        # attached or re-inserted) parents; rows of existing partitions and plain tables go
        # back through the parent table (routing, triggers, FK checks).
        for node in self.plan.nodes:
            table = _BY_NAME.get(node.table)
            if table is not None and table.name in staged:
                await self._attach(table)
            else:
                await self._reinsert(node, rows[node.table])
        self._phase("attach")
        return rows

    async def _lock_defaults(self) -> dict[str, str]:
        """Lock every table of the component against writes (not reads), parents first, before
        touching anything, and return the DEFAULT partition of each partitioned one.

        The later steps need these locks anyway: ``ATTACH`` of a referenced partition takes
        ``SHARE ROW EXCLUSIVE`` on the referencing tables and of a referencing partition on the
        referenced one. Taking them only there would deadlock with a writer that holds a table
        lock while it waits for one of this transaction's row or partition locks. Taken up front
        (bounded by ``lock_timeout``) the transaction waits only while it holds nothing, and
        writers to the event graph pause for the repair (bounded by the row budget)."""
        defaults: dict[str, str] = {}
        for node in self.plan.nodes:
            if not self.dry_run:
                await self.conn.execute(f"LOCK TABLE {_q(node.table)} IN SHARE ROW EXCLUSIVE MODE")
            if node.table in _BY_NAME:
                default = await default_partition(self.conn, node.table)
                if default is not None:
                    defaults[node.table] = default
        return defaults

    async def _snapshot(self, root_default: str, where: str) -> dict[str, int]:
        """Copy (and row-lock) the day's component into temp tables; rows per table, parents
        first. Empty when the root has no rows of that day."""
        lock = "" if self.dry_run else " FOR UPDATE"
        bounds = (self.lower, self.upper)
        probe = await self.conn.fetchval(
            f"SELECT count(*) FROM (SELECT 1 FROM ONLY {_q(root_default)} WHERE {where} "
            f"LIMIT {self.max_rows + 1}) AS s",
            *bounds,
        )
        if probe > self.max_rows:
            msg = f"more than {self.max_rows} {self.root.name} rows on {self.day}"
            raise RepairRefusedError(TOO_LARGE, msg)
        root_node = self.plan.nodes[0]
        status = await self.conn.execute(
            f"CREATE TEMP TABLE {_q(root_node.temp)} ON COMMIT DROP AS "
            f"SELECT * FROM ONLY {_q(root_default)} WHERE {where}{lock}",
            *bounds,
        )
        rows = {root_node.table: _rowcount(status)}
        if rows[root_node.table] == 0:
            return {}
        # Temp tables have no statistics: without them the child copies plan nested loops.
        await self.conn.execute(f"ANALYZE {_q(root_node.temp)}")
        temps = {root_node.table: root_node.temp}
        for node in self.plan.nodes[1:]:
            parent = node.via.parent if node.via is not None else root_node.table
            rows[node.table] = await self._copy_child(node, temps[parent], lock)
            temps[node.table] = node.temp
            await self.conn.execute(f"ANALYZE {_q(node.temp)}")
            if sum(rows.values()) > self.max_rows:
                msg = f"the component of {self.day} has more than {self.max_rows} rows"
                raise RepairRefusedError(TOO_LARGE, msg)
        for parent_node, fk in self.plan.guards:
            if await self.conn.fetchval(
                f"SELECT EXISTS (SELECT 1 FROM {_q(fk.child)} AS c WHERE EXISTS "
                f"(SELECT 1 FROM {_q(temps[parent_node.table])} AS p WHERE {_match(fk)}))"
            ):
                msg = f"{fk.name} (ON DELETE {fk.on_delete!r}) has referencing rows"
                raise RepairRefusedError(UNSUPPORTED, msg)
        return rows

    async def _copy_child(self, node: Node, parent_temp: str, lock: str) -> int:
        fk = node.via
        if fk is None:  # pragma: no cover - only the root has no FK
            msg = "the root is not copied as a child"
            raise RuntimeError(msg)
        sql = (
            f"CREATE TEMP TABLE {_q(node.temp)} ON COMMIT DROP AS SELECT c.* FROM "
            f"{_q(node.table)} AS c WHERE EXISTS (SELECT 1 FROM {_q(parent_temp)} AS p "
            f"WHERE {_match(fk)})"
        )
        args: list[Any] = []
        child = _BY_NAME.get(node.table)
        if child is not None and child.key in fk.child_cols:
            # Same event day by the composite FK: lets the planner prune to that day.
            sql += f" AND c.{_q(child.key)} >= $1 AND c.{_q(child.key)} < $2"
            args = [self.lower, self.upper]
        if lock:
            sql += " FOR UPDATE OF c"
        return _rowcount(await self.conn.execute(sql, *args))

    async def _stage_partitions(self, rows: dict[str, int]) -> set[str]:
        """For every partitioned table of the component without a partition for the day: create
        it standalone (``LIKE`` the parent, plus a CHECK matching the bound so ``ATTACH`` skips
        the scan) and fill it straight from the copy. A standalone table has no triggers and no
        FKs yet, so this is a plain bulk insert; the copies already hold the derived columns.
        Returns the staged table names (attached later, parents first)."""
        # Imported here: partitions imports this module.
        from hookrelay.services.partitions import list_partitions  # noqa: PLC0415

        staged: set[str] = set()
        for node in self.plan.nodes:
            table = _BY_NAME.get(node.table)
            if table is None:
                continue
            parts = await list_partitions(self.conn, table)
            if any(p.bound is not None and p.bound.overlaps(self.lower, self.upper) for p in parts):
                continue
            name = table.partition_name(self.day)
            part, key = _q(name), _q(table.key)
            await self.conn.execute(
                f"CREATE TABLE {part} (LIKE {_q(table.name)} INCLUDING DEFAULTS "
                f"INCLUDING CONSTRAINTS INCLUDING STORAGE)"
            )
            await self.conn.execute(
                f"ALTER TABLE {part} ADD CONSTRAINT {_q(name + '_bound')} "
                f"CHECK ({key} >= {_ts(self.lower)} AND {key} < {_ts(self.upper)})"
            )
            cols = await self._insert_columns(table.name)
            inserted = _rowcount(
                await self.conn.execute(
                    f"INSERT INTO {part} ({cols}) SELECT {cols} FROM {_q(node.temp)}"
                )
            )
            if inserted != rows[node.table]:
                raise _RacedError
            staged.add(table.name)
        return staged

    async def _attach(self, table: PartitionedTable) -> None:
        name = table.partition_name(self.day)
        await self.conn.execute(
            f"ALTER TABLE {_q(table.name)} ATTACH PARTITION {_q(name)} "
            f"FOR VALUES FROM ({_ts(self.lower)}) TO ({_ts(self.upper)})"
        )
        await self.conn.execute(f"ALTER TABLE {_q(name)} DROP CONSTRAINT {_q(name + '_bound')}")
        logger.info("partitions.repair_created", table=table.name, partition=name)

    async def _insert_columns(self, table: str) -> str:
        return ", ".join(
            _q(r["attname"]) for r in await self.conn.fetch(_INSERT_COLUMNS_SQL, table)
        )

    async def _reinsert(self, node: Node, expected: int) -> None:
        cols = await self._insert_columns(node.table)
        inserted = _rowcount(
            await self.conn.execute(
                f"INSERT INTO {_q(node.table)} ({cols}) OVERRIDING SYSTEM VALUE "
                f"SELECT {cols} FROM {_q(node.temp)}"
            )
        )
        if inserted != expected:
            raise _RacedError


_RETRYABLE = (
    asyncpg.exceptions.LockNotAvailableError,
    asyncpg.exceptions.DeadlockDetectedError,
    asyncpg.exceptions.CheckViolationError,  # ATTACH found a row that raced into DEFAULT
    asyncpg.exceptions.SerializationError,
    _RacedError,
)


async def repair_day(
    conn: asyncpg.Connection,
    root: PartitionedTable,
    day: date,
    policy: PartitionPolicy,
    *,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> DayRepair:
    """Repair one stranded day of ``root`` (one transaction per attempt), retrying lock
    timeouts, deadlocks and races; never raises for a database error, it reports it."""
    started = time.monotonic()
    result = DayRepair(table=root.name, day=day, status=DEFERRED)
    delay = policy.retry_base_s
    try:
        plan = await component_plan(conn, root.name)
        for attempt in range(policy.ddl_retries + 1):
            try:
                repairer = _DayRepairer(
                    conn, root, day, plan, max_rows=policy.repair_max_rows, dry_run=policy.dry_run
                )
                rows = await repairer.run()
            except _RETRYABLE as exc:
                result.detail = type(exc).__name__
                logger.warning(
                    "partitions.repair_retry",
                    table=root.name,
                    day=day.isoformat(),
                    attempt=attempt + 1,
                    error=type(exc).__name__,
                )
                if attempt < policy.ddl_retries:
                    await sleep(delay)
                    delay = min(delay * 2, policy.retry_max_s)
                continue
            result.rows = rows
            result.phase_ms = repairer.phase_ms
            result.status = WOULD_REPAIR if policy.dry_run else REPAIRED
            result.detail = None
            break
    except RepairRefusedError as exc:
        result.status, result.detail = exc.status, exc.detail
    except asyncpg.PostgresError as exc:
        result.status, result.detail = FAILED, f"{type(exc).__name__}: {exc}"
    result.elapsed_ms = int((time.monotonic() - started) * 1000)
    if result.status in (REPAIRED, WOULD_REPAIR):
        logger.info("partitions.repaired", **result.as_log())
    else:
        logger.warning("partitions.repair_skipped", **result.as_log())
    return result


async def repair_defaults(
    conn: asyncpg.Connection,
    policy: PartitionPolicy,
    *,
    should_stop: Callable[[], bool] = lambda: False,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> list[DayRepair]:
    """Repair up to ``policy.repair_max_days`` stranded days, ``events`` first (its component
    includes that day's deliveries), then days where only ``deliveries`` are stranded. Days older
    than the retention cutoff are left to the DEFAULT sweep. The caller holds the advisory locks
    of every partitioned table."""
    now: datetime = await conn.fetchval("SELECT now()")
    cutoff = day_bounds(utc_day(now) - timedelta(days=policy.retention_days))[0]
    results: list[DayRepair] = []
    budget = policy.repair_max_days
    # Parents first (events, then deliveries): an events day's component includes its deliveries.
    roots = [t for t in PARTITIONED_TABLES if not t.movable]
    for root in roots:
        # Re-read per root: repairing an events day also re-homes that day's deliveries.
        for day in await stranded_days(conn, root, cutoff, limit=budget + 1):
            if budget <= 0 or should_stop():
                results.append(
                    DayRepair(table=root.name, day=day, status=DEFERRED, detail="day budget")
                )
                return results
            results.append(await repair_day(conn, root, day, policy, sleep=sleep))
            budget -= 1
    return results
