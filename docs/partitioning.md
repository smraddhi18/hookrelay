# Partitioning the event graph

Milestones 9c and 9d. Three tables grow with traffic: `events` (one row per published event),
`deliveries` (one per event and subscribed endpoint) and `delivery_attempts` (one per HTTP
attempt). Until 9c their expiry was row by row: the retention job's `DELETE` on `events`
cascaded through `deliveries` into every attempt. Since migration `0012` all three are natively
range-partitioned by **the event's UTC day**, so one day of the graph is one set of partitions
and expiry is `DETACH` + `DROP` of those partitions, children first. Space returns to the OS at
once instead of becoming dead tuples for autovacuum.

## The design in one picture

```
events                  PARTITION BY RANGE (created_at)
deliveries              PARTITION BY RANGE (event_created_at)    event_created_at = the event's created_at
delivery_attempts       PARTITION BY RANGE (event_created_at)    event_created_at = the delivery's

events_p20261115        ┐
deliveries_p20261115    ├─ day D of the graph: events of D, their deliveries, their attempts
delivery_attempts_p20261115 ┘   (an attempt made on D + 3 for an event of D still lives here)
… PARTITION_PREMAKE_DAYS (7) ahead, plus a DEFAULT per table that should stay empty
```

Why the *event's* day for all three, and not each table's own `created_at`:

- A day is a connected component of the FK graph (`events` ← `deliveries` ← `attempts`), so
  dropping the day's three partitions leaves nothing dangling and nothing to cascade.
- With 9c's key (the attempt's own `created_at`) an event created on day D expires during day
  D + N but its attempts' partitions of D .. D + N only become droppable later, so the cascade
  still deleted most attempt rows first. Keyed by the event's day, the same run drops them all.
- Retention is global (`retain_until = created_at + EVENT_RETENTION_DAYS`), so "the event's day
  is past the cutoff" is exactly "every row of that day is expired".

### Keys and constraints (the trade-off)

Postgres needs every primary key and unique constraint of a partitioned table to contain the
partition key, and an FK that targets one needs the key in the referenced columns:

| table | 0011 | 0012 |
|---|---|---|
| `events` | `PRIMARY KEY (id)`, unique `(tenant_id, id)`, unique `(tenant_id, external_id)` | `PRIMARY KEY (id, created_at)`, unique `(tenant_id, id, created_at)`, **external ids moved** (below) |
| `deliveries` | `PRIMARY KEY (id)`, unique `(event_id, endpoint_id)` | `PRIMARY KEY (id, event_created_at)`, unique `(event_id, endpoint_id, event_created_at)`, unique `(tenant_id, id, event_created_at)` |
| `delivery_attempts` | `PRIMARY KEY (id, created_at)`, unique `(delivery_id, attempt_number, created_at)` | `PRIMARY KEY (id, event_created_at)`, unique `(delivery_id, attempt_number, event_created_at)` |
| `event_outbox` | FK `(tenant_id, event_id)` | plain table, new `event_created_at`, FK `(tenant_id, event_id, event_created_at)` |

- Ids are still random UUIDs from `gen_random_uuid()`: unique in practice, no longer proven
  across days by the database. Nothing references them without the key.
- The per-event invariants stay *exact*, not weakened: "one delivery per (event, endpoint)" and
  "attempt number unique per delivery" are scoped to one event, and an event, its deliveries and
  its attempts share one day by construction, so the wider keys cover precisely the same rows
  (`test_attempt_numbers_are_unique_per_delivery` shows a retry made days later is still
  rejected). 9c's weaker "unique per partition only" caveat is gone.
- **`external_id` uniqueness moved to `event_external_ids`**, a small plain table
  (`PRIMARY KEY (tenant_id, external_id)`, `event_id`, `event_created_at`, `expires_at` = the
  event's `retain_until`). It is claimed in the ingest transaction with
  `INSERT … ON CONFLICT DO UPDATE … WHERE expires_at <= now()`: a live claim makes the publish a
  `409 event_exists` (same body as before), an expired one is taken over, exactly as the old
  unique index let a purged event's id be reused, but now it holds even while the old event row
  is still waiting for its partition drop. The relay's housekeeping deletes expired claims.
- **`idempotency_keys` lost its FK to `events`.** It would need the key too, and it only
  cascaded cleanup. A record is self-contained and expires by its own `expires_at` (the sweep
  already existed). The replayed response is stored, so it never needed the event row.
- **Writers pass the key.** A `BEFORE ROW` trigger cannot fill a partition key (the row is routed
  first: "moving row to another partition during a BEFORE FOR EACH ROW trigger is not
  supported"), so `deliveries` and `delivery_attempts` inserts carry `event_created_at`, which
  the lease carries from the delivery row to every fenced write (`Lease`, `LeasedDelivery`). A
  wrong value is rejected by the composite FK (`test_the_partition_key_cannot_be_wrong_or_missing`),
  and the existing `BEFORE INSERT` triggers that derive `event_type` / `endpoint_id` now look the
  parent up by primary key, so they hit one partition. `event_outbox` is not partitioned, so its
  trigger can still fill `event_created_at` when left NULL.
- Unique-violation errors name the partition's index (`deliveries_p20261115_event_id_endpoint_…`),
  not `uq_deliveries_event_endpoint`. The application never maps those names to responses.
- `alembic check`: Postgres clones an FK that targets a partitioned table onto the referencing
  table once per referenced partition, which reflection reports as extra FKs. `include_object`
  in `hookrelay.db.partitioning` hides those clones and `include_name` hides the partitions.

## Reads: what prunes and what does not

| Query | Pruning |
|---|---|
| Hot paths: lease, complete, renew, release, reaper, fan-out, mark-processed, per-event delivery list | keyed by `(id, event_created_at)` or `event_created_at = …`: one partition |
| `GET /v1/events/{id}` and `GET /v1/deliveries/{id}` (id only) | one index probe per partition (~39 with 30 days of retention + 7 premade + DEFAULT) |
| Delivery log search / keyset pagination (`created_at` windows) | no pruning on deliveries' own `created_at` (the key is the event day); the `(tenant_id, status, created_at, id)` etc. indexes are per partition and the planner uses a Merge Append: no Sort node, still an index scan |
| Endpoint health / dashboard windows on `delivery_attempts.created_at` | **lost**: attempts are keyed by the event day and a replay creates attempts under an old day, so a `created_at` window cannot bound `event_created_at`; one index probe per partition |
| Day windows on the key (`event_created_at >= …`) | plan-time pruning for literals, run-time pruning (`Subplans Removed`) for `now() - interval` |

### Measured cost of those trade-offs

`loadtest/partition_bench.py`, run on the build box (Postgres 16.16, one machine, shared with
everything else, defaults for the rest; medians of 25 `EXPLAIN ANALYZE` runs, no extrapolation).
Data: 31 UTC days × 15,000 events, one delivery each, two attempts each (465k events, 465k
deliveries, 930k attempts), 39 partitions per table, 50 endpoints. The 0011-style rows below are
the same attempts partitioned by their own `created_at`; "plain" is the same deliveries in an
unpartitioned table with the same index.

| Query | 0012 layout | comparison | |
|---|---|---|---|
| endpoint health, last 1 h | plan 1.76 ms, exec 0.26 ms | 0011-style: plan 0.48 ms, exec 0.11 ms | +1.3 ms plan, +0.15 ms exec |
| endpoint health, last 24 h | plan 1.76 ms, exec 0.91 ms | 0011-style: plan 0.52 ms, exec 0.75 ms | +1.2 ms plan, +0.16 ms exec |
| lease candidate pick, 50 endpoints × 20 | plan 2.63 ms, exec 4.21 ms | plain table: plan 0.03 ms, exec 0.32 ms | **+3.9 ms exec**, about 13× |
| delivery by `(id, event_created_at)` | plan 0.07 ms, exec 0.005 ms | | one partition |
| delivery by `id` only | plan 1.95 ms, exec 0.14 ms | | probes 39 partitions |

The window reads cost about a millisecond more to plan and a fraction of a millisecond more to
run: the attempt-window pruning loss is real but small at this size. The **lease pick is the
cost to watch**: every lease cycle probes the partial `endpoint_due` index of each partition for
each endpoint with capacity (about 2,000 empty probes here, 4.2 ms against 0.3 ms). It is one
statement per lease cycle, not per delivery, and old partitions are nearly always empty, but it
is a fixed tax per cycle that grows with `EVENT_RETENTION_DAYS`. Ways to shrink it when it
matters, none done yet: bound the pick by the oldest day that can still hold due work (unsafe
today, because a replay re-queues old deliveries), or keep due work in a small plain table.

### Retention cost

Same data, retiring the oldest day (14,999 events, 14,999 deliveries, 29,998 attempts), child
first, one connection:

| table | `DETACH` | `DROP` |
|---|---|---|
| `delivery_attempts` | 10.9 ms | 9.7 ms |
| `deliveries` | 278.5 ms | 9.0 ms |
| `events` | 113.6 ms | 3.5 ms |

For comparison the row-by-row `DELETE` of one day of events (cascading into the same deliveries
and attempts, the 0011 retention path) took 1,704 ms here, **before** autovacuum has to reclaim
those tuples. At 300 events per day the same three detaches took 8.9 / 15.9 / 9.4 ms, so
the two parent detaches scale with the data: **`DETACH` of a partition that other partitions
reference scans the referencing side to prove no row still points at it, while it holds
`ACCESS EXCLUSIVE` on the parent** (inserts and leases on that table queue behind it).
`lock_timeout` only bounds the wait to *get* the lock, not the time the check then runs. At
this size that is a third of a second once a day per table; at a hundred times the volume it
would be tens of seconds, so schedule the job off-peak and re-measure at your volume (the
script takes `N_PER_DAY`). Without a DEFAULT partition the job detaches `CONCURRENTLY`
(verified for all three tables, see below), which does not hold `ACCESS EXCLUSIVE` for the
check; the cost of that mode at volume has not been measured here.

## Retention: the order, and why Postgres enforces it

`PARTITION_RETENTION_DAYS` (0 = follow `EVENT_RETENTION_DAYS`, validated at least that) defines
the cutoff, UTC midnight of today minus N. A day is droppable when its partition's upper bound
is at or before it. `hookrelay-partitions` retires **children first**, premakes **parents
first**:

1. `delivery_attempts` day D: `DETACH`, `DROP`.
2. `deliveries` day D: `DETACH` (Postgres checks that no attempt still references it), `DROP`.
3. `events` day D: same, against deliveries and the outbox.

Postgres itself refuses the wrong order: detaching a referenced partition fails with "removing
partition violates foreign key constraint" while referencing rows exist, and it works once the
child is gone (verified on 16; `test_a_parent_partition_is_never_dropped_while_children_reference_it`).
So if a child drop is deferred (lock timeout) the parent is reported `blocked`, keeps every row,
and the next run takes the whole day. Nothing is ever orphaned or half-deleted.

The row-level event purge (`hookrelay-retention`, relay housekeeping) is now a **backstop**: it
deletes events only `RETENTION_EVENT_PURGE_GRACE_S` (default 2 days) after their
`retain_until`, by `(id, created_at)`, so a healthy partition job always gets there first and
the cascade finds nothing. `0` purges as soon as an event expires (no partition job).

## Maintenance job: `hookrelay-partitions`

`python -m hookrelay.partitions [--dry-run]` runs as an hourly CronJob in Helm (`partitions.*`
values, `concurrencyPolicy: Forbid`, `7 * * * *`) and as the `partitions` compose service
(`--profile ops`). It maintains all three tables in the order above; every step is idempotent,
so a crashed, repeated or overlapping run is harmless.

1. **Single runner per table.** `pg_try_advisory_lock(hashtextextended('hookrelay.partitions:<table>', 0))`
   for each table; a table whose lock is held is skipped (`partitions.skipped_locked`), the rest
   proceed. Session level, so a direct connection (`DATABASE_DIRECT_URL`), never PgBouncer in
   transaction mode.
2. **Finalize** partitions left "detach pending" by an interrupted `DETACH … CONCURRENTLY`.
3. **Premake** today .. today + `PARTITION_PREMAKE_DAYS` per table with `CREATE TABLE … (LIKE
   parent INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)` + a bound `CHECK` +
   `ATTACH` (`SHARE UPDATE EXCLUSIVE` on the parent, unlike `PARTITION OF`'s `ACCESS
   EXCLUSIVE`; the CHECK lets `ATTACH` skip the scan), then the CHECK is dropped.
4. **Rows stranded in DEFAULT.** `delivery_attempts` is a leaf table: it creates the day's
   partition and moves the rows (DELETE + INSERT in one transaction). `events` and `deliveries`
   are **not** moved: the move is a DELETE, and `ON DELETE CASCADE` would delete the rows that
   reference them. The day is reported as `blocked_days` (`partitions.blocked_by_default_rows`
   warning, exit code 3, `hookrelay_partition_default_rows`) and needs an operator (see
   below). The relay's premake keeps this from happening in the first place.
5. **Retire** children first (above), then drop orphaned tables a crashed run detached.
6. **Sweep DEFAULT** of rows older than the cutoff, child first, in `ctid` batches (an event's
   deliveries cascade, which is intended: it is past retention).
7. **Summarise** one `partitions.finished` line per table, plus the gauges below.

`DETACH … CONCURRENTLY` is used only for tables without a DEFAULT partition (Postgres refuses
it otherwise); it works for referenced partitions too, so an operator who removes the DEFAULTs
(detach first: a DEFAULT cannot be dropped directly while the FKs reference it) gets a fully
concurrent, child-first day drop (`test_without_default_partitions_a_whole_day_drops_concurrently`).
With the DEFAULTs in place (the shipped layout) it is a plain `DETACH` under `lock_timeout`,
retried with capped backoff, deferred to the next run (exit 3) if still blocked.

### The relay is a second safety net

Every `RELAY_PARTITION_CHECK_INTERVAL_S` (300; 0 turns it off) the relay opens a direct
connection and makes sure every table has partitions through today + `RELAY_PARTITION_MIN_DAYS_AHEAD`
(2), under the **same advisory locks**, so whichever of relay and CronJob holds a lock does the
(idempotent) premake and the other skips. It **never retires or sweeps** (`premake_only`), so a
healthy CronJob and the relay can not disagree. Anything it creates logs `relay.partitions_premade`
(a warning: the CronJob is behind or dead) and increments
`hookrelay_partition_safety_premakes_total{table}`: alert on it being non-zero, it is the
signal that the job is not running while the relay is hiding the symptom. A failure of the check
is a counted loop error, never a stopped relay.

### If rows are stranded in `events_default` / `deliveries_default`

It happens only if partitions were missing when a row arrived (the CronJob **and** the relay
down for longer than the premake horizon, or a clock / time-zone mistake). The job refuses to
move them, because the move is a DELETE whose `ON DELETE CASCADE` would delete the rows that
reference them, and reports the day in `blocked_days` (tested:
`test_days_stranded_in_default_are_blocked_not_moved_for_parent_tables`). Rows older than the
cutoff are simply swept. Repairing a live day is a manual, **untested** operation in this
milestone: it needs the day's three partitions created while the rows are re-homed with FK
triggers off (`SET session_replication_role = replica` as a superuser), with the relay and
workers stopped. A cascade-safe automatic repair needs that superuser setting or a different FK
design (deferrable `NO ACTION` plus an explicit purge); it is recorded as remaining work
rather than shipped half-tested. The relay's premake exists to keep this from happening.

### Settings

| Variable | Default | Meaning |
|---|---|---|
| `PARTITION_PREMAKE_DAYS` | 7 | days of partitions kept ready ahead of today (1-90) |
| `PARTITION_RETENTION_DAYS` | 0 | 0 = follow `EVENT_RETENTION_DAYS`; otherwise must be at least that |
| `PARTITION_LOCK_TIMEOUT_MS` | 2000 | `lock_timeout` for each DDL statement |
| `PARTITION_DDL_RETRIES` | 5 | retries per step on lock timeout before deferring it |
| `RETENTION_EVENT_PURGE_GRACE_S` | 172800 | the row-level event purge waits this long past `retain_until` (0-30 d; 0 = purge on expiry) |
| `RELAY_PARTITION_CHECK_INTERVAL_S` | 300 | relay premake safety net cadence (0 = off) |
| `RELAY_PARTITION_MIN_DAYS_AHEAD` | 2 | how far ahead the relay premakes (1-30) |

Exit codes: `0` done (or another runner holds a lock), `3` incomplete (something deferred or
blocked, or the sweep cap was reached), `1` a table is not partitioned (database below `0012`).

### Metrics and alerts

| Metric (label `table`) | Alert when |
|---|---|
| `hookrelay_partitions` | n/a (capacity view) |
| `hookrelay_partition_premade_seconds` | `< 2 * 86400`: premake has been failing for days |
| `hookrelay_partition_default_rows` (capped at 10,000) | `> 0` for an hour: rows are landing in DEFAULT |
| `hookrelay_partition_safety_premakes_total` (counter, relay) | any increase: the CronJob is behind and the relay is covering for it |

Alert on CronJob failures too (`kube_job_status_failed`), which covers exit code 3.

## Migrating an existing database: `0012`

`0012` is the simple in-migration copy, like `0011`. In one transaction (`ACCESS EXCLUSIVE` on
the old tables, so writes wait for the copy): rename the three tables, drop their keys, indexes
and the FKs that point at them, create the partitioned tables (`LIKE` the old ones plus
`event_created_at`), one partition per UTC day that has events plus today .. today + 7 plus
DEFAULT for each, copy the rows (deliveries and attempts joined to their parent for the key),
then add keys, indexes, FKs and triggers; backfill `event_outbox.event_created_at`; create and
backfill `event_external_ids`; drop the idempotency FK and the old tables.

The downgrade restores the 0011 layout, keeps every row, deletes idempotency records whose event
no longer exists (there was no FK to stop that happening), and fails loudly if two live events
ended up sharing an `external_id` (possible after a reclaimed claim) rather than lose one.
`test_m9d_partitioned_event_graph_roundtrip_keeps_every_row` seeds events on three days (one 40
days old) with external ids, an idempotency key, outbox rows, deliveries and attempts (one made
two days after its event), upgrades, checks every row's partition, deletes an event to leave a
dangling key, downgrades, compares, re-upgrades, and checks an empty schema diff each time.

For a database too big to copy inside a window, the online paths are unchanged from 9c and
still documented, not automated:

1. **Attach the old table as a legacy partition**: `NOT VALID` CHECK + `VALIDATE` on the old
   table, then `ATTACH` as `FROM (MINVALUE) TO (<cutover>)`; unique indexes rebuilt with the key
   first (`CREATE UNIQUE INDEX CONCURRENTLY`). It ages out as one `DROP`.
2. **Dual-write plus backfill**: a trigger mirrors inserts, a batched `INSERT … SELECT … ON
   CONFLICT DO NOTHING` backfills history, verify counts per day, then rename in one short
   transaction.

## What is left

- A cascade-safe automatic repair of stranded `events` / `deliveries` DEFAULT rows (see above).
- Making the parent `DETACH` cheap at high volume (see the measured scaling) and shrinking the
  lease-pick tax with a retention-independent bound.
- Tooling for the online migration paths. They are documented, not automated.
