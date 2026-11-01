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

## Milestone 2: Event ingestion, idempotency and the transactional outbox ✅

- [x] `POST /v1/events` with JSON payload limits and schema checks
- [x] `Idempotency-Key` support (tenant-scoped, request fingerprint, replayed response, TTL)
- [x] Transactional outbox: event row + outbox row in one transaction
- [x] Fan-out relay: outbox → one `delivery` row per matching subscription, exactly once per (event, endpoint)
- [x] Event retrieval and listing APIs; retention column for later partitioning

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

### 2026-11-01: Milestone 2 complete

- `POST /v1/events` (publisher role): the body is read by the route itself after auth, with `415` for non-JSON media types, `413 request_too_large` on a declared or streamed body over `EVENT_MAX_PAYLOAD_BYTES` + 16 KiB, strict UTF-8 JSON (depth ≤ 64, no `NaN`/`Infinity`/`\u0000`/lone surrogates), canonical re-serialisation and `413 payload_too_large` on the canonical payload size. Registered, non-archived types only; unique `external_id` per tenant (`409 event_exists`); `occurred_at` skew check; `202` + `Location`.
- `Idempotency-Key`: tenant-scoped, 1 to 255 printable ASCII chars, SHA-256 fingerprint over method + path + canonical body; claim is an `INSERT ... ON CONFLICT DO UPDATE WHERE expired` in the request transaction, so concurrent duplicates block on the row lock (bounded by `IDEMPOTENCY_LOCK_TIMEOUT_MS` → `409 idempotency_key_in_progress` + `Retry-After`); replays return the stored `202` with `Idempotent-Replayed: true`; mismatch → `422`; errors roll back the claim and are never cached; expired keys can be reused. 8 concurrent identical requests → 1 event, 7 replays.
- Transactional outbox: event + `event_outbox` row + stored response commit together (verified by forcing a failure after the insert: no rows survive).
- Fan-out relay `python -m hookrelay.relay` / `hookrelay-relay`: `FOR UPDATE SKIP LOCKED` batches, one `INSERT ... SELECT ... ON CONFLICT (event_id, endpoint_id) DO NOTHING` per batch against enabled, non-deleted, matching endpoints; per-row savepoint fallback parks poison rows with exponential backoff (`attempts`, `last_error`, `available_at`); adaptive idle polling; housekeeping purges expired idempotency keys, old processed outbox rows and expired events (cascading deliveries); heartbeat file + `--healthcheck`; graceful `SIGTERM`/`SIGINT`; `--once` mode. 4 concurrent relays over 60 events × 3 endpoints → exactly 180 deliveries; crash mid-batch + outbox replay → no duplicates.
- Read APIs: `GET /v1/events` (type and time-range filters, keyset pagination, metadata only), `GET /v1/events/{id}` (with payload), `GET /v1/events/{id}/deliveries`. `retain_until` on events and deliveries (`EVENT_RETENTION_DAYS`).
- Migrations `0005` (events, event_outbox with a partial pending index, idempotency_keys) and `0006` (deliveries with status/attempt/next-attempt columns ready for M3, `updated_at` trigger); composite same-tenant FKs and CHECKs for every invariant; stepwise downgrade test extended.
- New env vars `EVENT_*`, `IDEMPOTENCY_*`, `RELAY_*`; compose gains a `relay` service with a heartbeat health check; CI's docker smoke test now publishes an event, checks the idempotent replay, runs the relay from the image and asserts one delivery, then runs the relay loop, probes its health check and stops it with `SIGTERM`.
- 476 tests (340 unit, 136 integration on Postgres 16), 97% line+branch coverage; ruff, ruff format, mypy strict, Alembic roundtrip + `alembic check` pass; the OpenAPI deny-by-default audit covers the new routes.
- Next: Milestone 3 (delivery workers: leasing, Standard-Webhooks signing, retries with jitter, SSRF guard, attempt log).
