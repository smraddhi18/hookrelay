# hookrelay roadmap

One production-grade system, built in milestones. Each milestone lands with migrations, tests
(unit + integration on real Postgres), docs and a green CI run. Delivery semantics are
**at-least-once** throughout; every milestone must preserve that guarantee.

Legend: `[x]` done, `[ ]` planned.

## Milestone 0: Foundation ✅

- [x] `src/` layout: `api/` (routers, schemas), `core/` (config, logging, errors, request context), `db/` (engine, session, models), `services/`
- [x] App factory (`create_app`) with no import-time side effects; `python -m hookrelay` / `hookrelay` entrypoint
- [x] pydantic-settings config from env vars with validation (URL normalisation to asyncpg, bounds, production guards) and `.env.example`
- [x] structlog JSON logging (stdlib + uvicorn bridged), secret redaction, one structured access-log line per request
- [x] Request-ID middleware (pure ASGI): accepts a safe `X-Request-ID` or mints one, binds it to logs, returns it on every response (including 500s)
- [x] Consistent error envelope: typed `AppError` hierarchy + handlers for validation / HTTP / unhandled errors (no internals leaked)
- [x] `GET /health` (liveness, no dependencies) and `GET /ready` (Postgres ping with hard timeout, 503 when down or draining)
- [x] Lifespan-managed async engine (pool sizing, pre-ping, server-side `statement_timeout`, `application_name`), graceful dispose on shutdown
- [x] Alembic configured for async with lock/statement timeouts; baseline migration `0001` (tenants + `updated_at` trigger)
- [x] uv-managed `pyproject.toml` + committed `uv.lock`; ruff (lint + format), mypy strict, pytest + pytest-asyncio + httpx
- [x] Integration tests on real Postgres in throwaway databases (migration up/down roundtrip, schema = models, constraints, timeouts)
- [x] Multi-stage Dockerfile (uv, non-root, stdlib healthcheck) and docker-compose (api + one-shot migrate + Postgres 16 + Redis 7)
- [x] GitHub Actions CI: lint, types, migration roundtrip + `alembic check`, tests with a Postgres 16 service, docker build + container smoke test

## Milestone 1: Tenants, API keys, endpoints and subscriptions ✅

- [x] Tenants admin API (create / get / suspend) behind an operator token
- [x] Tenant API keys: random secret shown once, stored as SHA-256 hash with prefix lookup, roles (`admin`, `publisher`, `reader`), rotation with overlap window, revocation
- [x] Deny-by-default auth dependency; per-request tenant scoping in every query
- [x] Endpoints CRUD (URL, description, enabled flag, custom headers, timeout, per-endpoint secret)
- [x] Event types registry and subscriptions (endpoint ↔ event type, wildcard `invoice.*` support)
- [x] Cursor (keyset) pagination, ETag/`If-Match` optimistic concurrency on updates
- [x] Migrations `0002+` with FKs, partial unique indexes, CHECKs; OpenAPI examples

## Milestone 2: Event ingestion, idempotency and the transactional outbox

- [ ] `POST /v1/events` with JSON payload limits and schema checks
- [ ] `Idempotency-Key` support (tenant-scoped, request fingerprint, replayed response, TTL)
- [ ] Transactional outbox: event row + outbox row in one transaction
- [ ] Fan-out relay: outbox → one `delivery` row per matching subscription, exactly once per (event, endpoint)
- [ ] Event retrieval and listing APIs; retention column for later partitioning

## Milestone 3: Delivery workers

- [ ] Separate worker process; leasing with `FOR UPDATE SKIP LOCKED`, lease expiry, heartbeats, fencing tokens
- [ ] Standard-Webhooks signing (`webhook-id`, `webhook-timestamp`, `webhook-signature` = HMAC-SHA256), verification helper + docs
- [ ] httpx client with connect/read/total timeouts, response size cap, no redirects by default
- [ ] Retries with exponential backoff + full jitter, `Retry-After` support, max attempts / max age
- [ ] SSRF guard: resolve-then-connect, block private/link-local/metadata ranges, scheme/port allow-list, DNS rebinding protection
- [ ] Delivery-attempt log (status, latency, truncated response, error class)
- [ ] Graceful drain on SIGTERM (finish or release in-flight leases)

## Milestone 4: Back-pressure and failure isolation

- [ ] Per-endpoint circuit breaker (closed / open / half-open) persisted in Postgres
- [ ] Per-endpoint rate limits and max in-flight concurrency
- [ ] Dead-letter state after exhausted retries; endpoint auto-disable after sustained failure with notification hook
- [ ] Replay API: single delivery, by event, by time range / filter (rate-limited, idempotent)

## Milestone 5: Operability for tenants

- [ ] Endpoint secret rotation with dual-signing overlap window
- [ ] Delivery log search (status, event type, endpoint, time range) with keyset pagination and indexes
- [ ] Endpoint health summary (success rate, p50/p95 latency, last error, breaker state)
- [ ] Message/payload retention policy and cleanup job

## Milestone 6: Redis

- [ ] Worker wake-ups via Redis pub/sub (Postgres polling remains the fallback)
- [ ] Cached subscription lookups for fan-out with explicit invalidation
- [ ] Token-bucket rate limits in Redis (Lua), degraded-mode behaviour when Redis is down

## Milestone 7: Observability

- [ ] Prometheus metrics (ingest rate, fan-out lag, delivery latency histograms, attempts by outcome, breaker states, queue depth)
- [ ] OpenTelemetry tracing from ingest → fan-out → delivery (trace context propagated to webhooks)
- [ ] Dashboard API (throughput, success rate, latency percentiles per tenant / endpoint)
- [ ] Prometheus + Grafana + Jaeger in compose

## Milestone 8: Load, chaos and capacity

- [ ] k6 / locust load tests (ingest + delivery to a mock receiver with configurable latency/failure)
- [ ] Chaos tests: Postgres restart, slow/failing receivers, worker kill mid-delivery (no lost or duplicated-beyond-at-least-once deliveries)
- [ ] Capacity plan: throughput per worker, pool sizing, partitioning / retention strategy, scaling limits

## Progress log

### 2026-10-30: Milestone 0 complete

- Project bootstrapped: FastAPI app factory, typed settings, structlog JSON logs, request-ID middleware, error envelope.
- `/health` (liveness) and `/ready` (Postgres ping with a 2 s default timeout, 503 when down or draining).
- Async SQLAlchemy engine owned by the lifespan; Alembic async env; baseline migration `0001` creates `tenants` (slug CHECK, status CHECK, unique slug, `gen_random_uuid()` ids) plus a shared `updated_at` trigger function.
- 121 tests (91 unit, 30 integration on Postgres 16), 95% line+branch coverage; ruff, ruff format, mypy strict and the Alembic upgrade/downgrade roundtrip all pass locally.
- Dockerfile, docker-compose and CI added. Next: Milestone 1 (tenant API keys, endpoints, subscriptions).

### 2026-10-31: Milestone 1 complete

- Operator API under `/admin` (create / list / get / suspend / resume tenants, bootstrap API keys) behind `OPERATOR_TOKEN` (constant-time compare; unset = disabled with `403`).
- Tenant API keys `hrk_<prefix>_<secret>`: shown once, SHA-256 at rest with a unique prefix index, dummy-digest compare for unknown prefixes, roles `reader < publisher < admin`, expiry judged by the DB clock, throttled `last_used_at` in its own transaction, rotation with a configurable overlap window (`FOR UPDATE` + partial unique index on `rotated_from_id` → `409` for a concurrent second rotation), idempotent revocation.
- Deny-by-default auth: a unit test audits the OpenAPI spec so every operation except `/health` and `/ready` declares a security scheme and answers `401` anonymously; tenant id comes only from the authenticated principal, and cross-tenant reads are `404`.
- Endpoints CRUD with URL policy (https only, no credentials, no private / loopback / metadata IP literals), custom-header deny-list and size limits, timeout bounds, soft delete, reader-masked header values, and a per-endpoint signing secret encrypted with AES-256-GCM (key id + AAD bound to tenant and endpoint, previous-key support) revealed only through an admin route with `Cache-Control: no-store`.
- Event-type registry (archive / unarchive), subscriptions with exact / `prefix.*` / `*` patterns, and `GET /v1/subscriptions/match` fan-out preview verified against the reference matcher.
- Keyset cursor pagination on every list; `ETag` / `If-Match` concurrency (`428` missing, `412` stale with current version, `*` to force; 5 concurrent writers → exactly one wins).
- Migrations `0002` (api_keys), `0003` (endpoints), `0004` (event_types, subscriptions): composite same-tenant FKs, partial unique indexes, CHECKs for every invariant; stepwise downgrade test.
- Structured logs never contain secrets (credential redaction, tracebacks rendered without frame locals). New env vars: `OPERATOR_TOKEN`, `SECRETS_ENCRYPTION_KEY(_PREVIOUS)`, `API_KEY_*`, `ENDPOINT_*`; production refuses to start without an encryption key.
- 374 tests (291 unit, 83 integration on Postgres 16), 97% line+branch coverage (coverage now traces SQLAlchemy greenlets); ruff, ruff format, mypy strict, Alembic roundtrip + `alembic check` pass. CI's docker smoke test now also checks `401` on `/v1` and creates a tenant through the operator API.
- Next: Milestone 2 (`POST /v1/events`, idempotency keys, transactional outbox, fan-out relay).
