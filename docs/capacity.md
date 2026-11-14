# hookrelay capacity plan (Milestones 8 and 9)

Every number in this document was measured with the k6 harness in [`loadtest/`](../loadtest/README.md)
against the real processes (uvicorn API, fan-out relay, delivery workers, `hookrelay-mock-receiver`)
on one machine, or is derived from measured numbers by the arithmetic shown next to it. Raw k6
summaries, receiver `/stats`, process logs and `ps` CPU samples were kept per run; the run label is
given in brackets, e.g. `[final-drain-w1-c128-l50]`.

## Read this first: the hardware caveat

| | Benchmark box |
| --- | --- |
| CPU / RAM | 8 vCPU (virtualised "Intel Xeon Processor"), 16 GB RAM, no swap |
| Co-tenants | **Everything on the same box**: Postgres, Redis, k6, the mock receiver, 1-4 API processes, the relay, 1-4 workers, plus unrelated desktop/browser workloads of the shared machine |
| Postgres | 16.15, `shared_buffers=512MB`, `max_connections=300`, `fsync=on`, `synchronous_commit=on`, `work_mem=4MB` |
| Redis | 8.0.2 locally (CI runs `redis:7-alpine`); wake-ups, subscription cache and Redis rate limits on |
| k6 | v2.3.0, constant-arrival-rate executor |
| Dates | 2026-11-08/09 (matrix), 2026-11-10 (Postgres restart runs), 2026-11-11/12 (Milestone 9 batching / PgBouncer runs, section 11) |

So: absolute numbers are a **floor for one shared 8-vCPU machine**, not a production figure. k6 and
Postgres compete with hookrelay for the same cores, which is why adding a 4th worker process stopped
helping (below). The *shape* of the results (what saturates first, how throughput scales with
concurrency and endpoints, how many DB connections are needed) is what this plan relies on. Re-run
the same scripts on separated nodes before committing to hardware (Milestone 9).

Load profile used everywhere unless stated: ~0.5 KB JSON payloads (477 B on average), every event
subscribed by `ENDPOINTS` endpoints on the mock receiver, 10% of ingest requests with an
`Idempotency-Key` (half of those replays) in `ingest.js`.

## 1. Ingest (`POST /v1/events`)

`ingest.js`, 30 s per step, latency in ms [`final-ingest-*`]:

| API processes | Offered req/s | Achieved req/s | p50 | p90 | p95 | p99 | Errors | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 150 | 149.4 | 6.0 | 8.5 | 10.8 | 33.2 | 0 | |
| 1 | 200 | 199.4 | 8.0 | 15.6 | 32.6 | 107.6 | 0 | knee |
| 1 | 250 | 228.6 | 653.9 | 2,229.9 | 2,891.6 | 4,281.2 | 0.18% | saturated, 350 iterations dropped |
| 4 | 600 | 597.4 | 7.3 | 22.6 | 28.7 | 77.0 | 0 | 28 dropped (k6 VU allocation) |
| 4 | 800 | 795.5 | 11.5 | 50.0 | 58.9 | 110.6 | 0 | 45 dropped |
| 4 | 1,000 | 902.8 | 295.6 | 790.2 | 982.6 | 1,429.6 | 0 | saturated, 2,538 dropped |

- **One API process sustains ~200 req/s** with p99 ≈ 108 ms; past ~230 req/s latency explodes
  (queueing in a single Python event loop). **Four processes sustain ~800 req/s** with p99 ≈ 111 ms,
  i.e. ingest scales close to linearly with processes on this box, up to ~900 req/s total.
- Ingest is a single short transaction (event + outbox row + idempotency row), so Postgres is not
  the limiter at these rates: the API process's CPU is. Plan **1 API process (≈ 1 vCPU) per
  ~150 req/s** of peak ingest to keep p99 near 30-35 ms, ~200 req/s if a p99 near 100 ms is fine.
- An earlier pass of the same matrix on pre-final code [`ingest-*`] gave the same knee
  (p95 12.4 / 35.7 ms at 150 / 200 req/s; saturated at 250 and 300 req/s with 1 process).

## 2. Delivery throughput per worker (backlog drain)

Method: ingest 6,000 events to 2 endpoints (12,000 deliveries) with workers stopped, then start the
workers and measure acknowledged deliveries per second at the receiver between its first and last
arrival. `l0` = receiver answers immediately, `l50` = 50 ms receiver latency. Every run below: **0
lost, 0 acknowledged twice**.

| Workers × concurrency | Receiver | Deliveries/s | Drain (s) | Run |
| --- | --- | --- | --- | --- |
| 1 × 32 | 0 ms | 349.7 | 41.6 | `final-drain-w1-c32-l0` |
| 1 × 32 | 50 ms | 265.3 / 273.9 | 52.7 / 51.2 | `final-drain-w1-c32-l50-rep2/3` |
| 1 × 64 | 50 ms | 368.4 | 40.1 | `final-drain-w1-c64-l50` |
| 1 × 128 | 50 ms | 430.3 | 35.6 | `final-drain-w1-c128-l50` |
| 2 × 64 | 50 ms | 397.4 | 37.6 | `final-drain-w2-c64-l50` |
| 4 × 32 | 0 ms | 378.1 | 39.1 | `final-drain-w4-c32-l0` |

**A worker process is CPU-bound, not I/O-bound.** By Little's law 32 slots against a 50 ms receiver
could carry 640 deliveries/s and 128 slots 2,560/s; measured: 265-274/s and 430/s. The `ps` samples
show the single worker at 85-88% of one core for the whole drain [`final-drain-w1-c32-l0`,
`final-drain-w1-c128-l50`]. That is ≈ 0.88 core / 430 deliveries/s ≈ **2 ms of worker CPU per
delivery** (lease share, HMAC signing, the HTTP exchange, the completion transaction) at the best
setting.

**Planning number: one worker process (≈ 1 vCPU) delivers ~265-430 deliveries/s** depending on
`WORKER_CONCURRENCY` (32 → 128) when receivers answer in ~50 ms. Raise concurrency for slow
receivers (it is cheap: slots are coroutines), but expect the curve to flatten at about one core.
(These are M8 numbers with one completion transaction per delivery; Milestone 9's group commit
roughly doubled the 128-slot figure on the same box, see section 11.)

One run was an outlier and is excluded from the table: the first `final-drain-w1-c32-l50` drained
at 99.2/s; 22 attempts against the 50 ms receiver timed out after ~15.9 s (a stalled process on the
shared box), their leases were reclaimed and retried, and 8 messages were acknowledged twice. That
is within the at-least-once contract (and is exactly the duplicate the reaper exists to create), but
it is a stall, not a capacity figure; its two repeats are in the table.

## 3. Scaling out: workers need endpoint parallelism

The lease planner locks one `endpoint_states` row per endpoint with `SKIP LOCKED` so concurrent
workers plan for disjoint endpoints (that is what makes `max_in_flight`, rate limits and the
breaker exact across the fleet). With only 2 endpoints, extra workers mostly skip each other's
locked rows: 2 × 64 (397/s) and 4 × 32 (378/s) do not beat 1 × 128 (430/s) in section 2.

With 16 endpoints (1,500 events × 16 = 24,016 deliveries, 50 ms receiver, 0 lost / 0 duplicates in
every run) [`final-drain-e16-*`]:

| Workers × concurrency (128 slots total) | Deliveries/s | Drain (s) |
| --- | --- | --- |
| 1 × 128 | 425.2 | 64.3 |
| 2 × 64 | 547.2 | 51.3 |
| 4 × 32 | 511.3 / 513.2 | 54.2 / 54.2 |

- 2 workers give **1.29×** one worker with the same total slots. The 4-worker runs show each worker at
  30-36% CPU (sampled) while Postgres, k6, the receiver and the API share the remaining cores: on
  this box the machine, not the design, is the ceiling beyond 2 workers. The best figure seen on the
  box was 590.4/s (4 × 32, 16 endpoints, immediate receiver, pre-final code [`drain-w4-c32-l0-e16-sniffio`]).
- Rule: **scale workers horizontally only when the active-endpoint count is well above the worker
  count**; a fleet dominated by one hot endpoint is bounded by that endpoint's `max_in_flight` and
  a single planner row, by design (failure isolation over raw speed for one tenant).

## 4. Steady state with realistic receivers

`e2e.js`, 60 s, 2 API processes, 2 workers × 32, 4 endpoints, receiver latency 20 ms + 0-30 ms jitter,
2% `500`/`503` (retried with backoff). Lag = event timestamp → receiver acknowledgement
[`final-steady-*`]:

| Events/s | Deliveries/s offered | Achieved deliveries/s | Lag p50 | Lag p95 | Lag p99 | Lost / acked twice |
| --- | --- | --- | --- | --- | --- | --- |
| 50 | 200 | 199.8 | 85.6 ms | 140.1 ms | 445.0 ms | 0 / 0 |
| 75 | 300 | 297.7 | 98.1 ms | 180.0 ms | 477.3 ms | 0 / 0 |
| 150 | 600 | 376.4 | 13.0 s | 34.7 s | 35.8 s | 0 / 0 (falls behind, drained 38.3 s after ingest stopped) |

- With 2 workers × 32 on this box the pipeline **keeps sub-200 ms p95 lag up to 300 deliveries/s**
  and saturates at ~375/s, consistent with sections 2-3. Past saturation nothing is lost: the
  backlog sits in Postgres and drains at the saturated rate.
- The 18,004-delivery run had 373 extra receiver hits, all retries of the injected 2% failures
  (received 18,377, acknowledged exactly 18,004).
- A 300 events/s (1,200 deliveries/s offered) step was attempted; k6 was SIGKILLed (exit 137) on the
  shared box and produced no summary, so it is not reported.

## 5. Receiver ceiling (is the mock the bottleneck?)

k6 straight at `hookrelay-mock-receiver` with 50 ms latency, 15 s per step [`recv-ceiling-*`]:
600, 1,000 and 1,495 req/s served with p50 50.2-50.3 ms, p99 51.3-52.3 ms and no errors. The
receiver handles at least 2.5× the highest hookrelay delivery rate measured, so it never limited the
results above.

## 6. Database connection pool sizing

Workers hold a connection only for the lease and the completion transactions, never during the HTTP
call, so the pool does not have to match `WORKER_CONCURRENCY`:

- 1 × 128 slots reached 430/s on the **default pool (`DB_POOL_SIZE=10`, `DB_MAX_OVERFLOW=5`)**.
- Larger pools did not buy throughput: 40 connections for 128 slots gave 111.2/s vs 120.7/s with
  the default [`drain-w1-c128-l50`, `-pool40`]; 20 connections for 32 slots gave 289.7/s vs 256.8/s
  [`drain-w1-c32-l0`, `-pool20`] (both pre-final code, within run-to-run noise of this box).
- Peak Postgres backends sampled for the whole stack (2 API processes + relay + workers): 29-31 with
  one worker, 37 with two, 61 with four.

Sizing rule: **total connections = Σ processes × (`DB_POOL_SIZE` + `DB_MAX_OVERFLOW`)**, i.e. 15 per
API, relay or worker process with the defaults. Keep the defaults up to 128 slots per worker. At
~20+ processes put PgBouncer (transaction pooling) in front of Postgres rather than raising
`max_connections`; asyncpg's prepared-statement cache must then be disabled or made
PgBouncer-compatible (Milestone 9).

## 7. Where the time went (and the fixes that made it into M8)

Profiling the worker during drains drove three changes, each measured on the box with pre-final
code [`drain-*`]:

| Change | Before → after | Kept? |
| --- | --- | --- |
| Shard the httpcore connection pool (≤ 16 connections per shard, least-busy shard per request). httpcore scans every pooled connection on each request and release, so with one 128-connection pool that scan dominated the worker's profile | 1 × 128, 50 ms: **120.7 → 472.0/s** (3.9×) | yes (`GuardedTransport`) |
| Declare `sniffio` explicitly. Without it httpcore's backend probe is a failed import walking `sys.path` on every pool operation, visible as a hot spot in the worker profile | 1 × 32, 0 ms: 256.8 → 326.3/s | yes (`pyproject.toml`) |
| Refill linger: a saturated worker waits up to `WORKER_REFILL_LINGER_MS` (20 ms) for a quarter of its slots before leasing again, instead of one lease transaction per finished delivery | 289.7 → 289.5/s (1 × 32), 111.2 → 118.5/s (1 × 128): within noise | yes, default 20 ms: it bounds lease-transaction rate at saturation; 0 disables |

## 8. Chaos results (correctness under faults)

Automated in CI (`tests/integration/test_chaos.py`, real Postgres, in-process mock receiver):
worker `SIGKILL` mid-delivery (8 requests in flight → redelivered once each, 0 acknowledged twice),
a connection storm that terminates every backend of the test database 8 times during traffic, a
backend killed between a receiver's 2xx and the completion write (completion retried, no
redelivery; with retries disabled exactly one redelivery), slow vs healthy receivers, flaky /
rate-limiting / hanging receivers converging to exactly-once acknowledgement, and a `SIGTERM` drain
during a database outage leaving leases to the reaper.

Real Postgres restart under load (`pg_ctl restart -m fast` ~25-30 s into `e2e.js` at 50 events/s ×
4 endpoints, 2 workers × 32, receivers 20-50 ms with 2% 5xx; reproducible with
`CHAOS_CMD`/`CHAOS_AFTER_S` in `loadtest/run-local.sh`), 2026-11-10:

| Code | Ingest failures | API 500s | Deliveries stranded in `delivering` | Lost / acked twice | Lag p95 / p99 |
| --- | --- | --- | --- | --- | --- |
| before fix [`chaos-pg-restart`] | 13 of 3,001 (0.43%) | 5 | 24 (completion write failed with `57P03`) | 0 / 0 at judgement time | 161.1 / 627.2 ms |
| after fix [`chaos-pg-restart-fixed`] | 8 of 3,001 (0.27%), all `503` + `Retry-After` | 0 | 0 (2 completion retries) | 0 / 0 | 152.5 / 552.2 ms |
| after fix, via `CHAOS_CMD` [`chaos-pg-restart-hook`] | 13 of 3,001 (0.43%), all `503` | 0 | 0 | 0 / 0 | 188.9 / 649.3 ms |
| final code (relay change too) [`chaos-pg-restart-final`] | 8 of 3,000 (0.27%), all `503` | 0 | 0 (28 completion retries) | 0 / 0 | 147.8 / 572.4 ms |

The first run found a real gap: while Postgres shuts down and starts up it refuses new connections
with SQLSTATE `57P03` (`cannot_connect_now`), which SQLAlchemy surfaces as a generic `DBAPIError`
that is not marked as a disconnect. The API answered `500` instead of a retryable `503`, and the
worker's completion retry gave up, leaving 24 already-acknowledged deliveries leased until their
60 s lease expired, at which point the reaper would have sent them again (at-least-once duplicates
that the completion retry exists to prevent). M8 now classifies connection-level SQLSTATEs (`08xxx`,
`57P01-57P03`, `53300`) as transient, and the relay treats them (and SQLAlchemy's
`PendingRollbackError` after a connection died mid-transaction) as "retry the batch on a fresh
connection" instead of running poison-row isolation, which the `-hook` run showed it attempting
once. Delivery throughput stayed at the offered ~200/s in all restart runs (200.7-201.2/s).

## 9. Storage, partitioning and retention

Measured on-disk size per row, heap + indexes, after the 24,016-delivery run
[`final-drain-e16-*` database]:

| Table | Rows | Total bytes | Bytes / row |
| --- | --- | --- | --- |
| `events` (477 B payload) | 1,501 | 2,138,112 | ≈ 1,424 |
| `deliveries` | 24,016 | 27,615,232 | ≈ 1,150 (of which indexes ≈ 895) |
| `delivery_attempts` | 24,016 | 23,797,760 | ≈ 991 |
| `event_outbox` | 1,501 | 491,520 | ≈ 327 (purged an hour after processing) |

Derived example: at the 300 deliveries/s steady state above (75 events/s × 4 endpoints, ~1.02
attempts per delivery) one day writes about 6.5 M events × 1.42 KB ≈ 9.2 GB, 25.9 M deliveries ×
1.15 KB ≈ 29.8 GB and 26.5 M attempts × 0.99 KB ≈ 26.2 GB: **≈ 65 GB/day** before payload retention.
These are small, freshly written tables (index sizes include update churn), so treat the figure as
an order of magnitude.

Strategy (implemented pieces in **bold**, the rest is Milestone 9):

1. **Payload retention** (`EVENT_PAYLOAD_RETENTION_DAYS`, M5) drops payloads and stored response
   bodies of settled events in bounded batches; TOASTed payloads are most of an event's size once
   payloads are larger than this benchmark's 0.5 KB.
2. **Row expiry by `retain_until`** (M2) keeps the hot set bounded today, but as batched `DELETE`s it
   costs WAL and vacuum work proportional to the rows removed.
3. **Native range partitioning of `delivery_attempts`** by `created_at` (M9c, migration `0011`,
   [partitioning.md](partitioning.md)): daily partitions premade by `hookrelay-partitions`, and
   expired days are detached and dropped as tables (no vacuum debt for what is left in them).
   Until the partition key follows the delivery's creation day, the event purge still cascades
   most attempt rows away first, so the WAL and delete cost of item 2 is not gone yet for attempts.
   `deliveries` and `events` are still to do. For `deliveries`, the partial "due" indexes must keep
   the lease query O(due rows) inside each partition.

## 10. Scaling limits and next bottlenecks

| Component | Measured on this box | Scale by | Limit / next step |
| --- | --- | --- | --- |
| API (ingest) | ~200 req/s per process at p99 ≈ 108 ms; ~800 req/s with 4 | processes / replicas (stateless) | Postgres write rate and connection count; PgBouncer past ~20 processes |
| Relay (fan-out) | 3.5-4% of a core while feeding 430-547 deliveries/s | replicas (`SKIP LOCKED` batches) | not near a limit in any run |
| Workers | ~265-430 deliveries/s per process (CPU-bound); 547/s with 2 workers on 16 endpoints | processes, *if* active endpoints ≫ workers | per-endpoint planner row; very hot single endpoints are capped by `max_in_flight` by design |
| Postgres | not saturated in isolation (shared CPU was) | vertical first; partition hot tables; read replica for dashboards / search | completions are group-committed since M9 (~13x fewer commits per delivery, section 11); `delivery_attempts` partitioned by day since M9c, `deliveries` / `events` next |
| Redis | accelerator only (wake-ups, cache, token buckets) | single primary is ample | losing it costs latency, not correctness |

Capacity formula for a target delivery rate `D` (deliveries/s) and ingest rate `I` (events/s), with
the box numbers as a conservative floor:

- workers ≈ `D / 265` processes at `WORKER_CONCURRENCY=32`, or `D / 430` at 128, plus one for
  headroom, provided the active-endpoint count is well above the worker count;
- API processes ≈ `I / 150` (p99 ≈ 35 ms) to `I / 200` (p99 ≈ 100 ms);
- relays: 1 (2 for availability);
- Postgres connections ≈ 15 × (API + relay + worker processes) with the default pool.

Example: 1,000 deliveries/s from 250 events/s × 4 endpoints → 3-4 workers at concurrency 128,
2 API processes, 2 relays, ≈ 105-120 Postgres connections. On separate nodes the per-process figures
should only improve, which Milestone 9 will verify.

## 11. Milestone 9: batched completions + PgBouncer

Same method and box as section 2 (6,000 events × 2 endpoints = ~12,000 deliveries ingested with the
worker stopped, then **1 worker × 128 slots** against a 50 ms receiver, 2 API processes, Redis on).
`WORKER_COMPLETE_BATCH_MAX=1` is the pre-M9 one-transaction-per-attempt path; `32` is the new
default (linger 5 ms). "Commits" is the `xact_commit` delta of the load database from 3 s after the
worker started to the end of the drain (so ingest is excluded; it includes lease, renewal and
reaper transactions), "WAL" the `pg_current_wal_lsn()` delta over the same window, "worker CPU" the
mean `ps` %CPU of the worker process. Every run: **0 lost, 0 acknowledged twice**, and one attempt
row per delivery.

| Batch max | Path to Postgres | Deliveries/s | Drain (s) | Commits | WAL (MB) | Worker CPU | Run |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | direct | 329.2 / 329.7 | 43.7 / 44.2 | 13,115 / 12,995 | 44.9 / 45.0 | 96.6% / 95.7% | `m9a-drain-w1-c128-l50-b1-r1/r2` (11-11) |
| 32 | direct | 693.5 / 716.2 | 24.6 / 24.1 | 944 / 1,022 | 42.1 / 41.6 | 87.3% / 87.3% | `m9a-drain-w1-c128-l50-b32-r1/r2` (11-11) |
| 1 | direct | 388.5 | 38.2 | 12,907 | 44.4 | 97.1% | `m9a-1112-drain-w1-c128-l50-b1` |
| 32 | direct | 759.6 | 23.1 | 1,005 | 41.0 | 86.5% | `m9a-1112-drain-w1-c128-l50-b32` |
| 1 | PgBouncer (transaction, 8 server conns) | 372.2 | 39.6 | 12,496 | 44.4 | 96.0% | `m9a-1112-drain-w1-c128-l50-b1-pgb` |
| 32 | PgBouncer (transaction, 8 server conns) | 618.5 | 26.6 | 924 | 41.3 | 76.2% | `m9a-1112-drain-w1-c128-l50-b32-pgb` |

The 11-11 runs used the M9 code before the final `(id, lease_token)` fencing hardening (same SQL
shape and statement count); the 11-12 runs are the final code. Absolute numbers moved between the two
days (the baseline was 329 vs 388 deliveries/s on the same shared box), so compare within a day.

What this shows:

- **Group commit roughly doubles what one worker drains**: 329 → 694-716 deliveries/s (11-11, 2
  reps each) and 388.5 → 759.6 (11-12), i.e. ×1.96-2.18. The worker was CPU-bound in both modes
  (86-97% of a core), so the gain is mostly CPU *per delivery*: ≈ 0.97 core / 388.5/s ≈ 2.5 ms
  with batch 1 vs ≈ 0.865 / 759.6 ≈ 1.1 ms with batch 32. One completion transaction per delivery
  (session checkout, `now()`, `UPDATE`, ORM insert + flush, breaker `UPDATE`, `COMMIT`, through
  SQLAlchemy + asyncpg) was more than half of the worker's per-delivery CPU.
- **Commits per delivery drop ~13x**: ≈ 1.08 (12,907 / 12,000) → ≈ 0.084 (1,005 / 12,002). The
  remaining commits are mostly leases (a 128-slot worker leases in groups) plus completion batches.
- **WAL barely changes** (44.4 → 41.0 MB for ~12,000 deliveries): the row images dominate, not
  commit records. Batching saves fsyncs and round trips, not bytes; partitioning and payload
  retention remain the storage levers (section 9).
- **The drain is shorter, end-to-end lag is not the metric here**: p50 lag in a backlog drain is
  dominated by the 35 s the backlog waited for the worker to start (28.4 s vs 35.6 s p50 on 11-12).
  At low load the batcher adds no wait (it flushes as soon as every in-flight attempt is waiting);
  under load the added wait is bounded by `WORKER_COMPLETE_BATCH_LINGER_MS` (5 ms).
- **PgBouncer costs something on one box**: 372.2 vs 388.5/s with batch 1 (−4%) and 618.5 vs 759.6/s
  with batch 32 (−19%), single runs. Each statement is prepared afresh (no statement cache, unique
  names), the pooler is one more process on the same 8 vCPUs, and its pool was 8 server connections.
  The batched path still beats the unbatched direct path by ×1.6 behind the pooler. PgBouncer is
  there for the connection budget below, not for speed; measure on separate nodes before tuning it
  (still on the roadmap).

### Connection budget

Without a pooler Postgres sees `Σ processes × (DB_POOL_SIZE + DB_MAX_OVERFLOW)` = 15 per API,
relay or worker process with the defaults (section 6): the 1,000 deliveries/s example in section 10 needs
~105-120 server connections and grows linearly with replicas. With PgBouncer in transaction mode
(compose default, `deploy/pgbouncer/pgbouncer.ini`):

- clients: every process keeps its SQLAlchemy pool (cheap client connections to PgBouncer, up to
  `max_client_conn = 1000`);
- servers: `default_pool_size = 20` per (database, user), `reserve_pool_size = 5` after 2 s of
  queueing, hard cap `max_db_connections = 40`, so Postgres' `max_connections` only needs 40 +
  migrations + admin / monitoring sessions, whatever the replica count;
- a client waits at most `query_wait_timeout = 15 s` for a server connection, then gets an error
  the worker treats as transient (fenced retry) and the API as `503`.

Each transaction holds a server connection only while it runs (no connection is held during HTTP
calls, section 6), and group commit cuts completion transactions ~13x, so the server-side pool is
sized by transaction *rate × duration*, not by slot count. On this box the drain above ran on 8
server connections (the local pooler's `default_pool_size`). The compose values above are a
configured starting point, not a measured optimum: size `default_pool_size` against the Postgres
host and re-measure on separate nodes.

## Reproducing

```bash
export DATABASE_URL=postgresql+asyncpg://hookrelay:hookrelay@127.0.0.1:5432/hookrelay_load  # dedicated DB
export REDIS_URL=redis://127.0.0.1:6379/0
# Ingest knee
API_WORKERS=1 bash loadtest/run-local.sh ingest.js -e RATE=200 -e DURATION=30s
# Backlog drain (delivery throughput per worker)
API_WORKERS=2 WORKERS=1 WORKER_CONCURRENCY=128 START_WORKERS_AFTER_S=35 \
  bash loadtest/run-local.sh e2e.js -e RATE=200 -e DURATION=30s -e ENDPOINTS=2 \
  -e RECEIVER_QUERY=latency_ms=50 -e E2E_P95_MS=600000 -e DRAIN_TIMEOUT_S=400
# Milestone 9: the same drain with batch 1 vs 32 (add DB_PGBOUNCER=true, DATABASE_DIRECT_URL=<direct>
# and a DATABASE_URL pointing at PgBouncer to measure through the pooler)
WORKER_COMPLETE_BATCH_MAX=1 API_WORKERS=2 WORKERS=1 WORKER_CONCURRENCY=128 START_WORKERS_AFTER_S=35 \
  bash loadtest/run-local.sh e2e.js -e RATE=200 -e DURATION=30s -e ENDPOINTS=2 \
  -e RECEIVER_QUERY=latency_ms=50 -e E2E_P95_MS=600000 -e DRAIN_TIMEOUT_S=400
# Real Postgres restart 25 s in (the data directory must be yours to restart)
CHAOS_AFTER_S=25 CHAOS_CMD='pg_ctl -D /path/to/pgdata restart -m fast -w' \
  API_WORKERS=2 WORKERS=2 bash loadtest/run-local.sh e2e.js -e RATE=50 -e DURATION=60s -e ENDPOINTS=4 \
  -e 'RECEIVER_QUERY=latency_ms=20&jitter_ms=30&failure_rate=0.02&failure_status=500,503' \
  -e E2E_P95_MS=600000 -e DRAIN_TIMEOUT_S=180
```
