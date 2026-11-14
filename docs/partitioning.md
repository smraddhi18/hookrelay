# Partitioning `delivery_attempts`

Milestone 9c. `delivery_attempts` gets one row per HTTP attempt, so it is the fastest-growing
table and the only one whose expiry used to be row by row: when an event expired, the
retention job's `DELETE` on `events` cascaded through `deliveries` into every attempt. Since
migration `0011` the table is natively range-partitioned by `created_at`, one partition per UTC
day. Expired days are detached and dropped as whole tables, so their space goes back to the OS
at once instead of becoming dead tuples for autovacuum. Time-windowed reads also prune to the
days they need. Read [the limitation](#limitation-the-event-cascade-still-runs-first) before
assuming this removes all delete work: it does not yet.

`deliveries` and `events` are still unpartitioned. They are mutable (leases, retries, status) and
their keys are referenced by other tables, so they need a separate design. See
[What is left](#what-is-left).

## Layout

```
delivery_attempts                  PARTITION BY RANGE (created_at)
├── delivery_attempts_p20261114    FOR VALUES FROM ('2026-11-14 00:00+00') TO ('2026-11-15 00:00+00')
├── delivery_attempts_p20261115    … one per UTC day, PARTITION_PREMAKE_DAYS (7) ahead
├── …
└── delivery_attempts_default      DEFAULT: should stay empty (alerted on)
```

- Names are `<table>_pYYYYMMDD` (`hookrelay.db.partitioning`). The bounds are UTC midnights,
  whatever the server's `TimeZone` setting is, and the maintenance job connects with
  `TimeZone=UTC`.
- The DEFAULT partition is a safety net: if the job stops, inserts still succeed instead of
  failing with "no partition of relation found for row". The job moves those rows into proper
  partitions when it catches up (see below), and `hookrelay_partition_default_rows` exposes them.
- Alembic only sees the parent: `include_name` in `alembic/env.py` skips partitions, so
  `alembic check` stays clean. The model declares `postgresql_partition_by`.

## Keys and constraints (the trade-off)

Postgres requires every primary key and unique constraint on a partitioned table to include the
partition key:

| 0010 (unpartitioned) | 0011 (partitioned) |
|---|---|
| `PRIMARY KEY (id)` | `PRIMARY KEY (id, created_at)` |
| `UNIQUE (delivery_id, attempt_number)` | `UNIQUE (delivery_id, attempt_number, created_at)` |

- `id` is still a random UUID from `gen_random_uuid()`. In practice it is unique; the database
  just no longer proves that across days. Nothing references `delivery_attempts.id` by foreign
  key.
- The attempt-number constraint is now a backstop *within one partition*. Attempt numbers are
  assigned under the delivery's lease fence: `attempt_count` is incremented when the lease is
  taken, and the attempt row is written only by the transition that ends that lease (completion
  or the reaper), fenced by `lease_token`. A worker whose lease was reclaimed writes nothing, so
  it cannot land a second attempt with the same number on any day.
  `test_attempt_numbers_are_unique_per_partition_only` pins down the weaker guarantee (rejected
  within a day, accepted across days), so nobody relies on the old one by accident.
- Unique-violation errors name the partition's index (for example
  `delivery_attempts_p20261114_delivery_id_attempt_number_crea_key`), not
  `uq_delivery_attempts_number`. The application never maps that constraint name to a response.
- Outgoing FKs (to `deliveries`, `endpoints` and `tenants`, including the same-tenant composite
  FKs) are declared on the parent. Postgres clones them onto every partition, as it does indexes
  and the endpoint trigger. `ON DELETE CASCADE` from `deliveries` still works.

## Reads

| Query | Pruning |
|---|---|
| Endpoint health, dashboard and attempt listings bounded by `created_at` | plan-time pruning for literal bounds; run-time pruning (`Subplans Removed`) for `now() - interval` |
| Attempts of one delivery (`WHERE delivery_id = …`) | none: one index probe per partition |
| Cascade from an expiring delivery | one index probe per partition |

The per-delivery probe costs about one index lookup per partition. With 30 days of retention plus
7 premade days that is around 38 small probes, which is acceptable for an API read of one
delivery. If it ever matters, the fix is to carry the delivery's `created_at` into those queries,
since an attempt is never older than its delivery. The integration tests check both behaviours
with `EXPLAIN`.

## Maintenance job: `hookrelay-partitions`

`python -m hookrelay.partitions [--dry-run]` runs as an hourly CronJob in Helm (`partitions.*`
values, `concurrencyPolicy: Forbid`, schedule `7 * * * *`) and as the `partitions` service in
compose (`--profile ops`). Every step is idempotent, so a crashed run, a re-run or an overlapping
run is harmless.

1. **Single runner.** `pg_try_advisory_lock(hashtextextended('hookrelay.partitions:delivery_attempts', 0))`.
   If another run holds the lock, this run logs `partitions.skipped_locked` and exits 0. The lock
   is session level, so the job connects to `DATABASE_DIRECT_URL` (`Settings.migration_dsn`), never
   through PgBouncer in transaction mode. The pooled-code audit in `tests/unit/test_db_pooling.py`
   allow-lists exactly this module and asserts that it uses the direct DSN.
2. **Finalize** partitions left "detach pending" by an interrupted `DETACH ... CONCURRENTLY`.
3. **Premake** today through today + `PARTITION_PREMAKE_DAYS`, plus every day that has rows in
   DEFAULT and is not yet past the cutoff. Each partition is created with `CREATE TABLE ... (LIKE parent
   INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)` and a CHECK matching its bound.
   Rows of that day are moved out of DEFAULT, then the table is `ATTACH`ed and the CHECK dropped,
   all in one transaction. `ATTACH` needs only `SHARE UPDATE EXCLUSIVE` on the parent, unlike
   `CREATE TABLE ... PARTITION OF`, which takes `ACCESS EXCLUSIVE`, so inserts keep flowing. The
   CHECK lets Postgres skip scanning the new table. When a DEFAULT exists, `ATTACH` locks and
   re-checks it, so a racing insert into DEFAULT makes the transaction fail and roll back rather
   than misplace a row. The next run retries.
4. **Retire** every partition whose upper bound is at or before the cutoff (UTC midnight of
   today minus the retention days): `DETACH`, then `DROP TABLE`. Leftover tables named like
   partitions (detached but not dropped by a crashed run) are dropped too.
5. **Sweep DEFAULT** of rows older than the cutoff, in `ctid` batches of `RETENTION_BATCH_SIZE`
   (at most `RETENTION_MAX_BATCHES` per run).
6. **Summarise**: one `partitions.finished` log line with the counts, plus the gauges below.

### Retention is safe by construction

An attempt is never older than its event. So a partition whose upper bound is at or before
`today - N days` only holds attempts of events older than N days, and those events have already
expired at `EVENT_RETENTION_DAYS = N`. That is why `PARTITION_RETENTION_DAYS` (0 means "same as
`EVENT_RETENTION_DAYS`") is validated to be at least `EVENT_RETENTION_DAYS`: the drop never takes
history that the API can still show. If `EVENT_RETENTION_DAYS` is lowered, partitions expire one
day after the matching events, because only whole days are dropped.

### Limitation: the event cascade still runs first

The FK from `delivery_attempts` to `deliveries` is `ON DELETE CASCADE`, and the relay's
housekeeping (or `hookrelay-retention`) deletes expired events at their `retain_until`. Those events cascade into
their deliveries and attempts. An event created on day D expires during day D + N, but the
partition for day D can only be dropped from D + N + 1 00:00 UTC, so for most rows the cascade
gets there first. The partition drop then removes a table that is mostly dead tuples. That is
still cheaper than vacuuming them, but the row-by-row delete has not gone away. Each cascaded
delivery delete also probes every partition's `delivery_id` index.

There are two ways to make the drop do all of the work. Neither is done yet; both are on the
roadmap:

- partition attempts by the *delivery's* (or event's) creation day instead of the attempt's
  own, so the attempts expire in the same run as their events, and the cascade finds nothing
  left once the job runs ahead of the relay's purge;
- allow `PARTITION_RETENTION_DAYS < EVENT_RETENTION_DAYS` as a product choice (attempt history
  kept for a shorter time than events), so partitions are dropped before their events expire.

### Locks, CONCURRENTLY and the DEFAULT partition

- Postgres refuses `DETACH PARTITION ... CONCURRENTLY` while a DEFAULT partition exists ("cannot
  detach partitions concurrently when a default partition exists"). This table keeps a DEFAULT,
  so the job uses a plain `DETACH`, which takes a brief `ACCESS EXCLUSIVE` lock on the parent.
  The detach itself is metadata only and takes milliseconds, but it waits behind long-running
  transactions on the table. To keep it from queueing every new insert behind it, the job sets
  `lock_timeout = PARTITION_LOCK_TIMEOUT_MS` (2 s). On `55P03` it retries with capped exponential
  backoff (`PARTITION_DDL_RETRIES`, default 5). If that is still not enough, the step is
  *deferred*: logged, the next step continues, and the process exits `3` so the Job shows up as
  failed. The next hourly run picks the step up again.
- Tables partitioned without a DEFAULT (other tables later, or an operator who drops it) get
  `DETACH ... CONCURRENTLY`. `test_without_a_default_partition_detach_is_concurrent` covers that
  path, and `..._is_finalized` covers recovery from an interrupted concurrent detach.
- `DROP TABLE` of a detached partition briefly locks the tables its FKs reference (`deliveries`,
  `endpoints`, `tenants`) to remove the FK triggers. The same lock timeout and retry policy
  applies.

### Settings

| Variable | Default | Meaning |
|---|---|---|
| `PARTITION_PREMAKE_DAYS` | 7 | days of partitions kept ready ahead of today (1-90) |
| `PARTITION_RETENTION_DAYS` | 0 | 0 = follow `EVENT_RETENTION_DAYS`; otherwise must be at least that |
| `PARTITION_LOCK_TIMEOUT_MS` | 2000 | `lock_timeout` for each DDL statement |
| `PARTITION_DDL_RETRIES` | 5 | retries per step on lock timeout before deferring it |

Exit codes: `0` done (or another runner holds the lock), `3` incomplete (something deferred or
the sweep cap was reached), `1` the table is not partitioned (database below `0011`).

### Metrics and alerts

Refreshed by the relay's gauge loop with the other queue gauges (`table` label):

| Metric | Alert when |
|---|---|
| `hookrelay_partitions` | n/a (capacity view) |
| `hookrelay_partition_premade_seconds` (upper bound of the newest partition minus now) | `< 2 * 86400`: the job has been failing for days and DEFAULT will start filling |
| `hookrelay_partition_default_rows` (capped at 10,000) | `> 0` for an hour: rows are landing in DEFAULT |

Alert on CronJob failures too (`kube_job_status_failed`), which covers the deferred-step exit code.

## Migrating an existing table

`0011` does the simple thing: inside the migration transaction it renames the old table, creates
the partitioned parent and a partition for each day that has rows, copies the rows before any
index exists (one bulk index build per partition), adds keys, indexes, FKs and the trigger, and
drops the old table. Writes to `delivery_attempts` wait for the whole copy. That is fine at the
sizes this project runs at and is fully reversible: the downgrade copies back into an
unpartitioned table with the `0010` keys, and it fails loudly instead of dropping rows if the
weaker constraint ever let a duplicate attempt number through.
`test_m9c_partitioning_roundtrip_keeps_every_attempt` seeds three days of attempts at `0010`,
upgrades, checks each row's partition, downgrades, compares every row, and re-upgrades with an
empty schema diff.

For a table too big to copy inside a maintenance window, use one of two online paths:

1. **Attach the old table as a legacy partition** (the cheapest). Create the partitioned
   parent, add a `NOT VALID` CHECK `created_at < <cutover>` on the old table, `VALIDATE` it (no
   write lock), then `ATTACH` it as `FOR VALUES FROM (MINVALUE) TO (<cutover>)`, which skips the
   scan because the CHECK proves the bound. New days go into new daily partitions. The legacy
   partition ages out as one big `DROP` once it is past retention. Unique indexes still have to
   be rebuilt with the partition key (`CREATE UNIQUE INDEX CONCURRENTLY` on the old table first).
2. **Dual-write plus backfill.** A trigger on the old table mirrors inserts into the new
   partitioned table. A batched backfill (`INSERT ... SELECT ... WHERE id > $last ORDER BY id
   LIMIT n ... ON CONFLICT DO NOTHING`) copies history while the app keeps writing. Then verify
   counts and checksums per day, and in one short transaction rename the tables and drop the
   trigger. Rolling back before the rename is just dropping the new table.

## What is left

- Make the drop do all of the retention work (see
  [the limitation](#limitation-the-event-cascade-still-runs-first)).
- Partition `deliveries` and `events`. `deliveries` is mutable and leased, so partition pruning
  must not make the lease query scan every partition. `events` is referenced by `deliveries` and
  `outbox`, and FKs that point *at* a partitioned table need the partition key in the referenced
  key. Both need their own design and migration.
- Let a long-running process (the relay) premake partitions as well, so a dead CronJob cannot
  fill DEFAULT. Today that case is covered by the DEFAULT safety net plus the alerts above.
- Tooling for the online migration paths above. They are documented, not automated.
