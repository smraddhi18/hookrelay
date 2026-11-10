# Load tests (k6) and the mock receiver

Everything here drives the **real** processes: the API (uvicorn), the fan-out relay, delivery
workers and `hookrelay-mock-receiver`, against Postgres 16 (and optionally Redis 7). Measured
numbers and the capacity model built from them live in [docs/capacity.md](../docs/capacity.md).

| File | What it does |
| --- | --- |
| `run-local.sh` | Migrates `DATABASE_URL`, starts receiver + API + relays + workers as local processes, runs one k6 script, stops everything, writes logs, the k6 summary and the receiver's `/stats` to `OUT_DIR`. |
| `ingest.js` | Constant-arrival-rate `POST /v1/events` (10% with `Idempotency-Key`, half of them replays). Thresholds: < 1% errors, p95 < 250 ms, p99 < 750 ms. |
| `e2e.js` | Publishes at a constant rate to `ENDPOINTS` endpoints on the mock receiver, then waits until the receiver has acknowledged every delivery. Fails on **any lost delivery** or **any delivery acknowledged twice** (`MAX_DUPLICATES`, default 0), and on e2e lag p95 above `E2E_P95_MS`. |
| `lib.js` | Provisioning (a fresh tenant per run via the operator API), payload builder, receiver helpers. |

## Quick start

```bash
# A dedicated, throwaway database (the script migrates it). Redis is optional.
export DATABASE_URL=postgresql+asyncpg://hookrelay:hookrelay@127.0.0.1:5432/hookrelay_load
export REDIS_URL=redis://127.0.0.1:6379/0

# Ingest only: 1 API process, 200 req/s for 60 s
API_WORKERS=1 bash loadtest/run-local.sh ingest.js -e RATE=200 -e DURATION=60s

# End to end with a realistic receiver: 4 endpoints, 20-50 ms latency, 2% 5xx
WORKERS=2 bash loadtest/run-local.sh e2e.js -e RATE=150 -e DURATION=60s -e ENDPOINTS=4 \
  -e 'RECEIVER_QUERY=latency_ms=20&jitter_ms=30&failure_rate=0.02&failure_status=500,503'

# Backlog drain (delivery throughput): ingest first, start workers 30 s later
API_WORKERS=2 WORKERS=1 WORKER_CONCURRENCY=32 START_WORKERS_AFTER_S=30 \
  bash loadtest/run-local.sh e2e.js -e RATE=300 -e DURATION=20s -e ENDPOINTS=2 \
  -e E2E_P95_MS=600000 -e DRAIN_TIMEOUT_S=300
```

`make loadtest-smoke` runs the same two short smokes as the CI `loadtest` job.

### Chaos under load

`CHAOS_CMD` runs any fault-injection command `CHAOS_AFTER_S` seconds (default 20) after k6 starts,
logging to `OUT_DIR/chaos.log`; `e2e.js` then judges the run with the same lost / duplicate checks.
A real Postgres restart (the measured run is in [docs/capacity.md](../docs/capacity.md#8-chaos-results-correctness-under-faults)):

```bash
CHAOS_AFTER_S=25 CHAOS_CMD='pg_ctl -D /path/to/pgdata restart -m fast -w' \
  API_WORKERS=2 WORKERS=2 bash loadtest/run-local.sh e2e.js -e RATE=50 -e DURATION=60s -e ENDPOINTS=4 \
  -e 'RECEIVER_QUERY=latency_ms=20&jitter_ms=30&failure_rate=0.02&failure_status=500,503' \
  -e E2E_P95_MS=600000 -e DRAIN_TIMEOUT_S=180
```

Expect a handful of ingest `503 database_unavailable` responses (with `Retry-After`) during the
restart window, so the ingest error-rate threshold may trip; the delivery checks must not.

Read `delivery_throughput_per_s` (acknowledged deliveries per second between the receiver's
first and last arrival), `deliveries_lost`, `deliveries_acked_duplicates` and the
`e2e_lag_*` gauges (envelope `timestamp` = ingest time → receiver arrival) from the summary.

## The mock receiver

`hookrelay-mock-receiver` (module `hookrelay.mock_receiver`) is a small ASGI app on uvicorn:

```bash
uv run hookrelay-mock-receiver --port 9000 --latency-ms 20 --jitter-ms 30 \
  --failure-rate 0.05 --failure-status 500,503,429 --retry-after-s 1 --hang-rate 0.01 --hang-ms 30000
```

- **Behaviour** = latency + uniform jitter, failure rate and statuses (`Retry-After` on 429/503),
  hang rate (holds the request past the sender's timeout) and the success status. Defaults come
  from flags / `MOCK_RECEIVER_*` env vars; `PUT /config` replaces them at runtime
  (`{"default": {...}, "paths": {"/hook/a": {...}}}`); an endpoint URL's query string overrides
  them per endpoint (`/hook/x?latency_ms=800&failure_rate=0.2`). A seeded RNG makes runs repeatable.
- **Accounting** per `(path, webhook-id)`: `received` (arrivals), `acked` (a 2xx actually written
  to a still-connected client; a sender that timed out and hung up is counted as `disconnected`,
  not acknowledged). `acked_duplicates` is the number that matters for at-least-once: a message
  processed twice. `GET /stats`, `POST /reset`, `GET|PUT /config`, `GET /health`.

The chaos tests (`tests/integration/test_chaos.py`) run the same receiver in-process.

## Caveats

- k6, Postgres, the receiver and all hookrelay processes share one machine in these runs; on a
  laptop / CI runner they compete for CPU. Treat CI numbers as a wiring check only.
- `APP_ENV=development` is used because only development may deliver to a loopback receiver
  (`DELIVERY_ALLOW_PRIVATE_NETWORKS` is refused in production by design). Credentials are
  random per run.
