# hookrelay

[![CI](https://github.com/smraddhi18/hookrelay/actions/workflows/ci.yml/badge.svg)](https://github.com/smraddhi18/hookrelay/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.12%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

**hookrelay** is a multi-tenant webhook delivery platform, the kind of service that sits behind
"Webhooks" settings pages at Stripe, GitHub or Shopify. Tenants register HTTPS endpoints and
subscribe them to event types; producers `POST` events once; hookrelay fans each event out and
delivers **signed** HTTP requests to every subscribed endpoint, with retries, back-pressure,
dead-lettering, replay and a full delivery-attempt log.

It is built milestone by milestone with production habits: typed config, structured logs,
migrations with downgrade paths, tests against real Postgres, containers and CI from day one.
See the **[roadmap](ROADMAP.md)** for the plan and progress log.

> **Status:** Milestone 0 (foundation) is complete. The service skeleton, configuration,
> logging, error model, health/readiness probes, database layer, migrations, Docker and CI are
> in place. Delivery features land from Milestone 1 onwards.

## Features

| Area | Feature | Status |
| --- | --- | --- |
| Foundation | App factory, typed env config with validation, `.env.example` | ✅ M0 |
| Foundation | structlog JSON logs, request-ID propagation (`X-Request-ID`), access log | ✅ M0 |
| Foundation | Uniform error envelope (`AppError`, validation, HTTP, unhandled) | ✅ M0 |
| Foundation | `/health` liveness, `/ready` readiness with Postgres timeout + drain awareness | ✅ M0 |
| Foundation | Async SQLAlchemy 2 + asyncpg, Alembic (async), baseline `tenants` schema | ✅ M0 |
| Foundation | Docker (multi-stage, non-root), compose stack, GitHub Actions CI | ✅ M0 |
| Tenancy | Tenants, hashed API keys with roles + rotation, endpoints, subscriptions | 🔜 M1 |
| Ingestion | `POST /v1/events`, idempotency keys, transactional outbox, fan-out | 🔜 M2 |
| Delivery | `SKIP LOCKED` workers, Standard-Webhooks HMAC-SHA256 signing, timeouts, retries with jittered backoff, SSRF guard, attempt log | 🔜 M3 |
| Isolation | Per-endpoint circuit breakers, rate limits, concurrency caps, DLQ, replay | 🔜 M4 |
| Operability | Secret rotation (dual signing), delivery log search, endpoint health | 🔜 M5 |
| Redis | Worker wake-ups, cached lookups, token buckets | 🔜 M6 |
| Observability | Prometheus metrics, OpenTelemetry tracing, dashboard API | 🔜 M7 |
| Scale | k6/locust load tests, chaos tests, capacity plan | 🔜 M8 |

## Architecture

### Target system

```mermaid
flowchart LR
    subgraph Clients
        P[Producer services]
        T[Tenant dashboard / CLI]
    end

    subgraph API["hookrelay API (FastAPI, stateless, N replicas)"]
        ING["POST /v1/events<br/>auth + idempotency"]
        MGMT["Endpoints / subscriptions<br/>replay / delivery log"]
    end

    subgraph PG["Postgres 16 (source of truth)"]
        EV[(events)]
        OB[(outbox)]
        DL[(deliveries<br/>+ attempts)]
        CFG[(tenants, api_keys,<br/>endpoints, subscriptions)]
    end

    R[("Redis 7<br/>wake-ups, cache,<br/>token buckets")]

    subgraph Workers["Workers (N replicas)"]
        FAN["Fan-out relay<br/>outbox → deliveries"]
        DW["Delivery workers<br/>SKIP LOCKED leases"]
    end

    subgraph Edge["Egress"]
        SSRF["SSRF guard<br/>+ HMAC signer"]
    end

    EP["Tenant endpoints<br/>(HTTPS receivers)"]
    OBS["Prometheus / Grafana / Jaeger"]

    P -->|signed API key| ING
    T --> MGMT
    ING -->|"one transaction:<br/>event + outbox row"| EV
    ING --> OB
    MGMT --> CFG
    MGMT --> DL
    OB --> FAN
    FAN -->|"per matching subscription"| DL
    FAN -. publish wake-up .-> R
    R -. wake .-> DW
    DW -->|lease, record attempt,<br/>retry / DLQ| DL
    DW --> SSRF --> EP
    DW -. rate limit / breaker .-> R
    API -. metrics, traces .-> OBS
    Workers -. metrics, traces .-> OBS
```

### What exists today (Milestone 0)

```mermaid
flowchart LR
    C[Client] -->|"HTTP + optional X-Request-ID"| MW

    subgraph App["FastAPI app (create_app)"]
        MW["RequestContextMiddleware<br/>request ID, structlog context, access log"]
        EH["Exception handlers<br/>AppError / validation / HTTP / unhandled → error envelope"]
        H["GET /health<br/>liveness"]
        RD["GET /ready<br/>readiness"]
        LS["Lifespan<br/>engine create / dispose, draining flag"]
        MW --> H
        MW --> RD
        MW --- EH
    end

    RD -->|"SELECT 1 with timeout"| PG[("Postgres 16<br/>tenants, alembic_version")]
    LS --> PG
    MIG["alembic upgrade head<br/>(one-shot job)"] --> PG
    App -->|"JSON logs"| OUT[(stdout)]
```

### Request flow (today)

1. `RequestContextMiddleware` (pure ASGI) reads `X-Request-ID`. A caller-supplied ID is kept only
   if it is 1 to 128 chars of `[A-Za-z0-9._:-]`, otherwise a UUID is minted. This stops log injection.
2. The ID goes into `scope["state"]` and structlog contextvars, so every log line of the
   request (including SQLAlchemy and uvicorn logs bridged through stdlib) carries it.
3. The router runs. Any error becomes one JSON shape:
   `{"error": {"code", "message", "details", "request_id"}}`. Unhandled exceptions are logged
   with a traceback and returned as a generic 500; exception text never reaches the client.
4. The response gets `X-Request-ID`, and one `http.request` access-log line records
   method, path, status and duration.

### Planned delivery flow (M2 to M4)

1. **Ingest:** authenticate the API key, check `Idempotency-Key`, then insert the `event` and an `outbox`
   row **in the same transaction** and return `202`. No dual write to a broker.
2. **Fan-out:** a relay claims outbox rows (`SKIP LOCKED`), resolves matching subscriptions and inserts
   one `delivery` per endpoint (unique on `(event_id, endpoint_id)`).
3. **Deliver:** workers lease due deliveries (`FOR UPDATE SKIP LOCKED`, lease token for fencing),
   check the endpoint's breaker and rate limit, resolve and vet the target IP (SSRF guard),
   sign with HMAC-SHA256 (Standard Webhooks headers) and POST with strict timeouts.
4. **Record:** every attempt is logged. 2xx marks the delivery done. Other outcomes schedule a retry with
   exponential backoff and full jitter (honouring `Retry-After`) until attempts run out, then it is dead-lettered.
   Tenants can replay anything.

## Design decisions and trade-offs

- **Postgres is the source of truth, with a transactional outbox instead of Kafka.** Events, deliveries and
  attempts need durable state, joins and auditing anyway. Writing the event and its outbox
  row atomically removes the classic dual-write bug (DB commit succeeds, broker publish fails).
  `SKIP LOCKED` gives a correct multi-worker queue without another stateful system. The trade-off is
  a lower throughput ceiling than a log-based broker. M8's capacity plan measures it, and
  partitioning plus retention are the planned levers before adding a broker.
- **At-least-once delivery.** Exactly-once over HTTP to third-party receivers is impossible. Workers
  can crash after the receiver got the request but before the attempt is recorded. hookrelay
  therefore guarantees at-least-once delivery and sends a stable `webhook-id` so receivers can de-duplicate.
- **FastAPI with async I/O.** The workload is I/O-bound: many concurrent outbound HTTP calls with slow
  receivers, plus Postgres round-trips. asyncio lets one process keep hundreds of requests in flight cheaply.
  FastAPI gives typed request/response models and OpenAPI docs for free. CPU work (HMAC, JSON) is small.
- **SQLAlchemy 2 async + asyncpg, Alembic for migrations.** Explicit SQL control where it matters
  (`SKIP LOCKED`, `RETURNING`, advisory locks) while keeping typed models. Every migration has
  a downgrade, and CI checks `upgrade → upgrade → check → downgrade → upgrade`.
- **Liveness ≠ readiness.** `/health` never touches dependencies, so a database blip does not get
  every pod restarted. `/ready` has a hard timeout (a hung TCP connect cannot hang the probe) and
  flips to 503 as soon as shutdown begins, so load balancers drain the instance first.
- **Fail-soft startup.** If Postgres is unreachable at boot the API still starts and reports
  not-ready, instead of crash-looping and adding load during an incident.
- **Defensive DB defaults.** Server-side `statement_timeout` and `idle_in_transaction_session_timeout`,
  `pool_pre_ping`, bounded pool and overflow, `application_name` for `pg_stat_activity`. Migrations
  run with `lock_timeout=5s` so a migration waiting on a hot table fails fast instead of
  blocking all traffic behind its lock.
- **Schema rules enforced in the database.** CHECK constraints (slug format, status values, name
  length), deterministic constraint names, text + CHECK instead of native ENUMs (cheaper to
  evolve), and a trigger-maintained `updated_at` that also covers raw SQL writes.
- **Baseline migration choice.** `0001` creates `tenants` rather than an empty revision. It exercises
  the whole toolchain (async env, naming convention, CHECKs, server defaults, a trigger, downgrade)
  from day one, and tenants are the root of every later foreign key.
- **App factory and injected dependencies.** Importing the package has no side effects. Tests build apps with
  their own settings and a fake or real database, and the lifespan owns resource cleanup.

## Getting started

### Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/) (0.13.x)
- Postgres 16 (or use Docker Compose)

### Local development

```bash
uv sync                         # creates .venv from uv.lock (incl. dev tools)
cp .env.example .env            # adjust DATABASE_URL if needed
uv run alembic upgrade head     # apply migrations
uv run hookrelay                # http://localhost:8000/docs
```

Quality gate (the same commands CI runs; see the `Makefile`):

```bash
uv run ruff check . && uv run ruff format --check .
uv run mypy
uv run alembic upgrade head && uv run alembic check && uv run alembic downgrade base && uv run alembic upgrade head
TEST_DATABASE_URL=postgresql://hookrelay:hookrelay@localhost:5432/hookrelay uv run pytest
```

Integration tests need `TEST_DATABASE_URL` pointing at a role that may `CREATE DATABASE`. Each run
creates and drops its own scratch databases, so your data is never touched. Without the variable,
integration tests are skipped and unit tests still run.

### Docker Compose

```bash
docker compose up --build   # postgres:16 + redis:7 + one-shot migrate job + api on :8000
curl -i localhost:8000/ready
docker compose down -v
```

The `migrate` service runs `alembic upgrade head` and exits. `api` starts only after it completes
successfully. The image runs as UID 10001 and has a stdlib-only `HEALTHCHECK`.

## Configuration

All settings are environment variables (or `.env`), validated at startup. See
[`.env.example`](.env.example) for the full list. Highlights:

| Variable | Default | Notes |
| --- | --- | --- |
| `APP_ENV` | `development` | `production` enforces JSON logs and forbids `DEBUG` / `DB_ECHO` |
| `DATABASE_URL` | `postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay` | `postgres://` and `postgresql://` are normalised to asyncpg; other databases are rejected |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `5` | per process |
| `DB_STATEMENT_TIMEOUT_MS` | `15000` | applied server-side to every connection |
| `READINESS_TIMEOUT_S` | `2` | `/ready` gives up and returns 503 after this |
| `LOG_FORMAT` / `LOG_LEVEL` | `json` / `INFO` | `console` for local readability |
| `REQUEST_ID_HEADER` | `X-Request-ID` | correlation header accepted and returned |
| `REDIS_URL` | unset | validated now, used from M6 |

## API

Interactive docs: `/docs` (Swagger UI), `/redoc`, schema at `/openapi.json` (disable with
`DOCS_ENABLED=false`).

| Method | Path | Purpose | Responses |
| --- | --- | --- | --- |
| `GET` | `/health` | Liveness: process is up (no dependency checks) | `200 {"status":"ok","service","version"}` |
| `GET` | `/ready` | Readiness: Postgres reachable within `READINESS_TIMEOUT_S` and not draining | `200` / `503` with per-check status and latency |

Error format (all non-2xx responses):

```json
{
  "error": {
    "code": "validation_error",
    "message": "Request validation failed",
    "details": [{"loc": ["query", "limit"], "msg": "...", "type": "greater_than_equal"}],
    "request_id": "6f1c0e3b2a5d4c4f9e1a2b3c4d5e6f70"
  }
}
```

## Project layout

```
src/hookrelay/
  main.py               app factory + lifespan
  __main__.py           `hookrelay` / `python -m hookrelay` (uvicorn)
  healthcheck.py        container HEALTHCHECK probe (stdlib only)
  api/                  routers + response schemas
  core/                 config, logging, errors, request context middleware
  db/                   engine/session lifecycle, declarative base, models
  services/             business logic (framework-free)
alembic/                async env + versioned migrations
tests/unit/             fast tests (fake database)
tests/integration/      real Postgres: migrations, schema invariants, timeouts, probes
```

## Scaling notes

- The API is stateless. Scale it horizontally behind a load balancer and size the total Postgres connections
  as `replicas × (DB_POOL_SIZE + DB_MAX_OVERFLOW)`. Add PgBouncer (transaction mode) once replicas grow.
- Workers (M3+) scale independently. `SKIP LOCKED` leasing lets N workers share the queue without
  coordination, and per-endpoint concurrency caps keep one slow receiver from starving the others.
- Hot tables (`deliveries`, `delivery_attempts`) are designed for time-based partitioning and
  retention (M5/M8). Redis (M6) only accelerates. Losing it degrades latency, not correctness.

## Roadmap

See [ROADMAP.md](ROADMAP.md): Milestone 0 through Milestone 8, with a dated progress log.

## License

[MIT](LICENSE)
