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

## Milestone 3: Delivery workers ✅

- [x] Separate worker process; leasing with `FOR UPDATE SKIP LOCKED`, lease expiry, heartbeats, fencing tokens
- [x] Standard-Webhooks signing (`webhook-id`, `webhook-timestamp`, `webhook-signature` = HMAC-SHA256), verification helper + docs
- [x] httpx client with connect/read/total timeouts, response size cap, no redirects by default
- [x] Retries with exponential backoff + full jitter, `Retry-After` support, max attempts / max age
- [x] SSRF guard: resolve-then-connect, block private/link-local/metadata ranges, scheme/port allow-list, DNS rebinding protection
- [x] Delivery-attempt log (status, latency, truncated response, error class)
- [x] Graceful drain on SIGTERM (finish or release in-flight leases)

## Milestone 4: Back-pressure and failure isolation ✅

- [x] Per-endpoint circuit breaker (closed / open / half-open) persisted in Postgres
- [x] Per-endpoint rate limits and max in-flight concurrency
- [x] Dead-letter state after exhausted retries; endpoint auto-disable after sustained failure with notification hook
- [x] Replay API: single delivery, by event, by time range / filter (rate-limited, idempotent)

## Milestone 5: Operability for tenants ✅

- [x] Endpoint secret rotation with dual-signing overlap window
- [x] Delivery log search (status, event type, endpoint, time range) with keyset pagination and indexes
- [x] Endpoint health summary (success rate, p50/p95 latency, last error, breaker state)
- [x] Message/payload retention policy and cleanup job

## Milestone 6: Redis ✅

- [x] Worker wake-ups via Redis pub/sub (Postgres polling remains the fallback)
- [x] Cached subscription lookups for fan-out with explicit invalidation
- [x] Token-bucket rate limits in Redis (Lua), degraded-mode behaviour when Redis is down

## Milestone 7: Observability ✅

- [x] Prometheus metrics (ingest rate, fan-out lag, delivery latency histograms, attempts by outcome, breaker states, queue depth)
- [x] OpenTelemetry tracing from ingest → fan-out → delivery (trace context propagated to webhooks)
- [x] Dashboard API (throughput, success rate, latency percentiles per tenant / endpoint)
- [x] Prometheus + Grafana + Jaeger in compose

## Milestone 8: Load, chaos and capacity ✅

- [x] k6 / locust load tests (ingest + delivery to a mock receiver with configurable latency/failure)
- [x] Chaos tests: Postgres restart, slow/failing receivers, worker kill mid-delivery (no lost or duplicated-beyond-at-least-once deliveries)
- [x] Capacity plan: throughput per worker, pool sizing, partitioning / retention strategy, scaling limits

## Milestone 9: Partitioning and a production deploy

The capacity plan's next levers, plus the deployment story an operator would actually run.

- [x] Native range partitioning by `created_at` for `delivery_attempts` (append-only, largest): daily partitions + DEFAULT (migration `0011`, tested downgrade), premade by a maintenance job (`hookrelay-partitions`, Helm CronJob), retention by `DETACH` + `DROP`, online migration path documented in [docs/partitioning.md](docs/partitioning.md)
- [ ] Partition `deliveries` and `events` too, and key attempts by their delivery's creation day so expiry is the partition drop alone (today the event purge cascades most attempt rows first); relay-driven premake as a second safety net; tooling for the online (legacy-partition / dual-write + backfill) migration
- [x] Batched completion writes in workers (one transaction for several finished attempts, still fenced per lease) to cut the one-completion-transaction-per-delivery cost (leases are already batched)
- [x] PgBouncer (transaction pooling) in compose and docs: asyncpg prepared-statement settings, connection budget per process, re-measured pool sizing
- [x] Helm chart: API / relay / worker Deployments, a pre-upgrade migration Job, liveness / readiness / heartbeat probes, PodDisruptionBudgets, `SIGTERM` grace above the drain timeout, secrets from Kubernetes Secrets
- [x] Autoscaling: HPA for the API on CPU, KEDA (Prometheus scaler) for workers on delivery demand (new `hookrelay_deliveries_due` + `deliveries_delivering`; optional fan-out backlog trigger)
- [x] CI: `helm lint` + a kind cluster smoke (install chart, run the k6 e2e smoke against it)
- [ ] Re-run the M8 capacity matrix with the load generator, Postgres and hookrelay on separate nodes and publish the delta against `docs/capacity.md`

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

### 2026-11-02: Milestone 3 complete

- Delivery worker `python -m hookrelay.worker` / `hookrelay-worker`: one statement leases `min(WORKER_BATCH_SIZE, free slots)` due deliveries (`FOR UPDATE OF d SKIP LOCKED`, enabled endpoint + active tenant only) and sets `delivering`, a fresh `lease_token`, `leased_until`, `leased_by` and `attempt_count + 1`; complete / renew / release all match the token (fencing), so a stalled worker's late result is discarded (`worker.lease_lost`). Lease renewal every `WORKER_LEASE_RENEW_INTERVAL_S` cancels attempts whose lease was lost; a reaper turns expired leases into `lease_expired` attempts and a retry (or `failed` on the last attempt). Concurrency semaphore, adaptive idle polling, heartbeat file + `--healthcheck`, `--once`, `SIGTERM`/`SIGINT` drain (`WORKER_DRAIN_TIMEOUT_S`, then unfinished leases are released with the attempt refunded). 5 concurrent workers over 40 deliveries → each delivered exactly once with one attempt row.
- Standard-Webhooks signing (`webhook-id` = event id, `webhook-timestamp`, `webhook-signature: v1,<base64 HMAC-SHA256>`) with a stdlib-only `verify()` helper (constant-time compare, multiple signatures, 5-minute tolerance), test vectors, and a receiver example in the README.
- httpx client: endpoint `timeout_ms` as the total deadline + `DELIVERY_CONNECT_TIMEOUT_MS`, response body capped at `DELIVERY_RESPONSE_MAX_BYTES`, `follow_redirects=False`, `trust_env=False`, connection reuse.
- Retry policy: 2xx succeeded; 408/409/425/429, 5xx, timeouts, connect/DNS/TLS/protocol errors retryable; other 4xx, 3xx and blocked destinations terminal. Full-jitter exponential backoff (`DELIVERY_BACKOFF_BASE_S`/`_MAX_S`), `Retry-After` (seconds or HTTP date) as a floor capped by `DELIVERY_RETRY_AFTER_MAX_S`, `DELIVERY_MAX_ATTEMPTS` and `DELIVERY_MAX_AGE_S` (`max_age_exceeded`) → terminal `failed`; DB clock throughout.
- SSRF guard as a custom httpcore network backend: resolve, vet every address (loopback, RFC 1918, CGNAT, link-local, cloud metadata, multicast, reserved, IPv4-mapped/compatible IPv6, NAT64 / 6to4 embeddings), connect to the vetted IP while Host/SNI keep the name, re-resolved per connection (DNS-rebinding safe); scheme (`https`, `http` only with `ENDPOINT_ALLOW_HTTP`) and port allow-list (`DELIVERY_ALLOWED_PORTS`); `DELIVERY_ALLOW_PRIVATE_NETWORKS` dev switch refused in production.
- Migration `0007`: `delivery_attempts` (append-only, unique per delivery + number, same-tenant FK, outcome/status/latency/size CHECKs, masked request headers, truncated response), delivery `failed` status with `completed_at`, lease columns + fencing token with a "delivering iff leased" CHECK, `last_status_code` / `last_error_class`, lease-expiry partial index; downgrade maps `failed` → `dead` and in-flight → `pending` (roundtrip test).
- Read APIs (reader role, tenant-scoped): `GET /v1/deliveries/{id}` and `GET /v1/deliveries/{id}/attempts` (newest first, keyset pagination); event deliveries list gains last status / error class.
- New env vars `WORKER_*` and `DELIVERY_*`; compose gains a `worker` service with a heartbeat health check and a 30 s stop grace period; CI's docker smoke test now starts a mock receiver, runs the worker from the image, asserts a `succeeded` delivery with one attempt, verifies the received signature with `hookrelay.core.signing.verify`, then runs the worker loop, probes its health check and stops it with `SIGTERM`.
- 732 tests (565 unit, 167 integration on Postgres 16), 98% line+branch coverage; ruff, ruff format, mypy strict, Alembic roundtrip + `alembic check` pass; the OpenAPI deny-by-default audit covers the new routes.
- Next: Milestone 4 (back-pressure and failure isolation: per-endpoint circuit breakers, rate limits and concurrency caps, dead-letter state, replay API).

### 2026-11-03: Milestone 4 complete

- Per-endpoint circuit breaker persisted in `endpoint_states` (one row per endpoint, backfilled and created by an `AFTER INSERT` trigger on `endpoints`): attempts are classified as availability success (2xx, receiver-answered 4xx, 3xx), failure (5xx, 408, 429, timeouts, connect / DNS / TLS errors) or neutral (secret unavailable, lease expired); `BREAKER_FAILURE_THRESHOLD` consecutive failures open it for `BREAKER_COOLDOWN_BASE_S × 2^trips` (capped by `BREAKER_COOLDOWN_MAX_S`); after the cooldown exactly one half-open probe is leased fleet-wide, and its success closes the breaker while its failure re-opens it for longer. Open breakers defer deliveries in `pending` without spending attempts. `GET /v1/endpoints/{id}/circuit` (reader) shows state, streak, `open_until`, trips, in-flight and limits.
- Per-endpoint `max_in_flight` (`ENDPOINT_DEFAULT_MAX_IN_FLIGHT`, capped by `ENDPOINT_MAX_IN_FLIGHT`) and `rate_limit_per_s` (lazy-refill token bucket, one second of burst, capped by `ENDPOINT_MAX_RATE_LIMIT_PER_S`), validated on create / PATCH (`422 max_in_flight_too_high` / `rate_limit_too_high`). Enforced in the lease transaction: eligible endpoints are locked `FOR UPDATE OF s SKIP LOCKED` and ordered least-recently-leased first, a pure `allocate()` shares free slots round-robin within each endpoint's quota, and an `unnest` + `LATERAL ... SKIP LOCKED LIMIT quota` lease takes each endpoint's oldest due rows before tokens are spent. Two workers never exceed an endpoint's cap; a busy endpoint cannot starve a quiet one.
- Dead-letter state: a retryable error with the run's budget spent (`DELIVERY_MAX_ATTEMPTS` since the last replay, or past `run_started_at + DELIVERY_MAX_AGE_S`) ends `dead`; permanent errors stay `failed`; the reaper dead-letters an expired lease on the last attempt. Endpoints failing for `ENDPOINT_AUTO_DISABLE_AFTER_S` are disabled (`auto_disabled_at`) in the same transaction that queues an `endpoint.disabled` row in a `notifications` outbox; the relay dispatches it through a pluggable `Notifier` (default: structured `WARNING` log) with exponential retry and `abandoned` after `NOTIFICATION_MAX_ATTEMPTS`. Re-enabling an auto-disabled endpoint resets its breaker.
- Replay API: `POST /v1/deliveries/{id}/replay`, `POST /v1/events/{id}/replay` (publisher; status / endpoint filters, default `failed` + `dead`) and `POST /v1/replays` (admin; required time window bounded by `REPLAY_MAX_WINDOW_S`, plus status / endpoint / event-type filters). A replay re-queues the same delivery (same `webhook-id`, attempt numbers keep increasing) with a fresh retry budget via `attempt_offset` / `run_started_at`; terminal deliveries only (`409 delivery_not_replayable`), disabled endpoints skipped (`409 endpoint_unavailable` for a single replay); at most `REPLAY_MAX_DELIVERIES` per call with `has_more`. `Idempotency-Key` support (stored `202`, `422` on mismatch); per-tenant rate limit (`REPLAY_RATE_LIMIT_PER_MINUTE`, advisory lock + sliding 60 s count over `replay_requests` → `429 replay_rate_limited` + `Retry-After`); every request leaves an audit row listed by `GET /v1/replays`.
- Migration `0008`: endpoint limit columns + `auto_disabled_at`, `endpoint_states` (state / streak / cooldown / bucket CHECKs), delivery `attempt_offset`, `run_started_at`, `replay_count`, per-endpoint partial due / delivering indexes, `notifications` and `replay_requests` tables; existing exhausted `failed` rows move to `dead` while permanent failures stay `failed`. Downgrade maps `dead` → `failed` and drops the rest (roundtrip test with seeded rows). Housekeeping purges old notifications and replay audit rows (`NOTIFICATION_RETENTION_S`).
- New env vars `ENDPOINT_*_MAX_IN_FLIGHT`, `ENDPOINT_MAX_RATE_LIMIT_PER_S`, `BREAKER_*`, `ENDPOINT_AUTO_DISABLE_AFTER_S`, `REPLAY_*`, `NOTIFICATION_*`; compose passes the main ones to every service.
- 803 tests (617 unit, 186 integration on Postgres 16), 98% line+branch coverage; ruff, ruff format, mypy strict, Alembic roundtrip + `alembic check` pass; the OpenAPI deny-by-default audit covers the new routes. Docker image build not re-verified locally (no Docker on the build host); CI's docker job covers it.
- Next: Milestone 5 (operability for tenants: secret rotation with dual signing, delivery log search, endpoint health summary, retention policy).

### 2026-11-04: Milestone 5 complete

- Signing-secret rotation: `POST /v1/endpoints/{id}/secret/rotate` (admin, `If-Match` required, `*` to opt out; optional `overlap_s` up to `ENDPOINT_SECRET_MAX_OVERLAP_S` and bring-your-own `whsec_` secret of 24-64 bytes) moves the current ciphertext to `previous_secret_ciphertext` (never decrypted, same AAD) and encrypts the new one in one conditional `UPDATE`. Until `previous_secret_expires_at` the worker signs every attempt with both secrets (`v1,<new> v1,<old>`); the attempt log redacts each signature. At most two secrets sign; `DELETE /v1/endpoints/{id}/secret/previous` ends the overlap early (idempotent); an undecryptable previous secret falls back to single signing. Concurrent rotations with one ETag: exactly one wins.
- Delivery log search: `GET /v1/deliveries` (reader) with repeatable `status`, `event_type`, `endpoint_id`, inclusive `created_after` / exclusive `created_before` (`422 invalid_time_range`), newest first with the shared keyset cursor. `deliveries.event_type` is denormalised by a `BEFORE INSERT` trigger and served by `(tenant_id, status, created_at, id)` / `(tenant_id, event_type, created_at, id)` indexes (EXPLAIN-tested: index scan, no sort).
- Endpoint health: `GET /v1/endpoints/{id}/health?window_s=` (reader): attempts by outcome, success rate, p50 / p95 / p99 latency, errors by class, last error (any time), last success, breaker state and capped backlog, computed on read from an index-only scan over a covering `(endpoint_id, created_at) INCLUDE (outcome, latency_ms, error_class)` index on `delivery_attempts.endpoint_id` (trigger-maintained), with a capped window (`422 window_too_large`) and sample (`sampled`).
- Payload retention: `EVENT_PAYLOAD_RETENTION_DAYS` (< `EVENT_RETENTION_DAYS`, 0 = off) drops payloads of events whose deliveries are all settled and scrubs their stored response bodies; events stay until `retain_until`. Batches of `RETENTION_BATCH_SIZE` in their own transactions (`SKIP LOCKED` events, deliveries share-locked through a `MATERIALIZED` CTE so in-flight completions and replays are waited for), a moving cursor past busy rows, `RETENTION_MAX_BATCHES` budget and `RETENTION_BATCH_PAUSE_MS` pause. Runs in relay housekeeping and as `hookrelay-retention` / `python -m hookrelay.retention` (exit 3 when the budget ran out; compose `retention` service, profile `ops`). Purged events answer replays with `409 payload_purged` (bulk skips them); a worker meeting one fails it `payload_purged` without a request and without touching the breaker.
- Migration `0009`: endpoint previous-secret columns + CHECKs, `deliveries.event_type` and `delivery_attempts.endpoint_id` with backfills, triggers and a same-tenant FK, search / health indexes, nullable `events.payload` with `payload_purged_at` and a both-or-neither CHECK; downgrade restores purged payloads as `{}` (roundtrip test with seeded rows).
- New env vars `ENDPOINT_SECRET_*`, `ENDPOINT_HEALTH_*`, `EVENT_PAYLOAD_RETENTION_DAYS`, `RETENTION_*`, cross-validated at startup.
- 859 tests (all passing; Postgres 16 integration tests for rotation, search, health, retention races and the schema), 98% line+branch coverage; ruff, ruff format, mypy strict, Alembic roundtrip + `alembic check` pass; the OpenAPI deny-by-default audit covers the new routes.
- Next: Milestone 6 (Redis: worker wake-ups, cached lookups, token buckets).

### 2026-11-05: Milestone 6 complete

- Redis is optional (`REDIS_URL` unset → every Redis feature off, Postgres paths unchanged). Fail-soft
  `redis.asyncio` client (`hookrelay.redis_`): jittered reconnect, commands fail fast while down (no
  offline queue), dedicated pub/sub connection, `CLIENT LIST` names (`hookrelay:api:cmd`,
  `hookrelay:relay:cmd:<id>`, `hookrelay:worker:cmd:<id>` / `:sub:<id>`), connected/disconnected logged
  once, `redis.error` throttled.
- Worker wake-ups: after the relay inserts pending deliveries (and after a replay re-queues) a
  coalesced publish lands on the global `{prefix}:wake:deliveries` channel (leading + trailing window
  so a stampede of events does not flood Redis). Workers subscribe on a dedicated connection and reset
  their poll delay on message; payload is observational only. When Redis is down or `WAKEUP_ENABLED`
  is false, the existing adaptive Postgres polling continues unchanged. `SIGTERM` unsubscribes and
  closes cleanly.
- Subscription cache for fan-out: cache-aside of matching enabled endpoint IDs keyed by
  `(tenant_id, event_type)` with TTL, short negative cache for empty results, and single-flight
  (process lock + Redis `SET NX` lock). Explicit tenant-wide invalidation on subscription
  create/delete, endpoint enable/disable/delete/soft-delete, and event-type archive. On miss or
  Redis error the relay falls back to the SQL `pattern_matches_sql` join — never invents matches;
  `ON CONFLICT (event_id, endpoint_id) DO NOTHING` still guarantees exactly-once inserts.
- Token-bucket rate limits in Redis (Lua, Redis `TIME` for refill, burst = 1 s of tokens, key
  `hookrelay:rl:endpoint:<uuid>`). When Redis is healthy the lease path takes tokens from Redis and
  still spends Postgres tokens (dual-update) so a failover is not unlimited. When Redis is down the
  existing Postgres bucket is used — never fail open. `max_in_flight` and the circuit breaker stay
  authoritative in Postgres.
- `/ready` reports a Redis ping when `REDIS_URL` is set; by default (`REDIS_REQUIRED_FOR_READY=false`)
  a Redis outage does not 503 the API (workers degrade). Set the flag when Redis is required for
  traffic.
- No schema migration (Redis-only). New settings: `REDIS_*` timeouts / reconnect / key prefix,
  `WAKEUP_*`, `SUBSCRIPTION_CACHE_*`, `RATE_LIMIT_REDIS_*`, `REDIS_REQUIRED_FOR_READY`; effective
  flags default on when `REDIS_URL` is set. Compose, `.env.example` and CI (Redis 7 service +
  `REDIS_URL` on the test job) updated.
- 892 tests (670 unit, 222 integration on Postgres 16 + Redis 7), 97% line+branch coverage; ruff,
  ruff format, mypy strict, Alembic `check` pass; the OpenAPI deny-by-default audit still covers
  every route (M6 adds none).
- Next: Milestone 7 (Observability: Prometheus metrics, OpenTelemetry tracing, dashboard API).

### 2026-11-06: Milestone 7 complete

- Prometheus metrics via ``prometheus-client``: process-local registry with ``service=api|relay|worker``
  labels. Counters/histograms cover event ingest, outbox processing, delivery attempts by outcome
  and latency; gauges cover outbox pending/lag, deliveries pending/delivering and circuit-breaker
  state counts. ``GET /metrics`` is unauthenticated by default (scrape-network assumption; never
  exposes tenant payloads) and optionally protected by ``METRICS_TOKEN``. Relay/worker expose a
  side-car scrape port when ``METRICS_PORT`` is set. Gauges refresh from Postgres on scrape / on
  the relay housekeeping interval.
- OpenTelemetry tracing (off unless ``OTEL_EXPORTER_OTLP_ENDPOINT`` / ``TRACING_OTLP_ENDPOINT`` is
  set): ingest → fan-out → delivery spans. W3C ``traceparent``/``tracestate`` are persisted on
  ``events`` (migration ``0010``) so the worker continues the ingest trace and injects those
  headers on outbound webhooks (deny-listed as tenant custom headers).
- Tenant dashboard ``GET /v1/dashboard`` (reader+): throughput, success rate, latency p50/p95/p99
  and per-endpoint breakdown over a bounded window, computed on read from ``delivery_attempts``
  like endpoint health (no counter drift).
- Compose adds Prometheus (9090), Grafana (3000) and Jaeger all-in-one (16686 / OTLP 4318) with
  scrape config under ``deploy/``; ``.env.example`` documents the new knobs.
- New deps: ``prometheus-client``, ``opentelemetry-api``, ``opentelemetry-sdk``,
  ``opentelemetry-exporter-otlp-proto-http``. Migration ``0010`` (trace columns only).
- 903 tests (all passing; Postgres 16 + Redis 7 integration), ~96% line+branch coverage; ruff,
  ruff format, mypy strict, Alembic `check` pass; the OpenAPI deny-by-default audit treats
  `/metrics` as public (optional `METRICS_TOKEN` at runtime). Docker unavailable on the build
  host; CI covers the image.
- Next: Milestone 8 (load, chaos and capacity).

### 2026-11-10: Milestone 8 complete

- k6 harness (`loadtest/`): `run-local.sh` migrates a dedicated database and starts the mock receiver,
  API (`API_WORKERS` uvicorn processes), relays and workers as real processes, runs one script and
  collects the k6 summary, the receiver's `/stats` and every process log. `ingest.js` is a
  constant-arrival-rate `POST /v1/events` (10% `Idempotency-Key`, half replays; < 1% errors, p95 <
  250 ms). `e2e.js` publishes to `ENDPOINTS` endpoints and, after a drain, fails on **any lost
  delivery or any delivery acknowledged twice**, reporting throughput and ingest → receiver lag.
  `START_WORKERS_AFTER_S` turns it into a backlog-drain benchmark; `CHAOS_CMD` / `CHAOS_AFTER_S`
  inject a fault (e.g. a real `pg_ctl restart`) mid-run. A CI `loadtest` job runs a short ingest +
  flaky-receiver e2e smoke on Postgres 16 + Redis 7 (`make loadtest-smoke` locally).
- `hookrelay-mock-receiver`: latency + jitter, failure rate / statuses (`Retry-After` on 429 / 503),
  hang rate past the sender's timeout, per endpoint via the URL query string or `PUT /config`,
  seeded RNG; acknowledgement accounting per `(path, webhook-id)` that only counts a 2xx written to a
  still-connected client.
- Chaos suite (`tests/integration/test_chaos.py`, 9 tests, real Postgres): worker `SIGKILL` with 8
  requests held by the receiver (redelivered once each, 0 acknowledged twice), 8 rounds of
  terminating every backend during traffic (60 events / 180 deliveries all succeed, loops survive),
  a backend killed between a 2xx and the completion write (retried, no redelivery; with retries off,
  exactly one), slow vs healthy receiver isolation, flaky / 429 / hanging receivers converging to
  exactly-once acknowledgement, `SIGTERM` drain during an outage, and relay connection loss not
  parking healthy outbox rows. Every scenario reconciles receiver acknowledgements with
  `delivery_attempts`.
- Restart-safe write paths: transient DB errors are `503 database_unavailable` + `Retry-After` on the
  API instead of `500`; workers retry completion writes (`WORKER_COMPLETE_RETRIES`, fenced by the
  lease token, so idempotent); a drain that cannot release leases leaves them to the reaper; the
  relay retries a batch on a fresh connection after a disconnect. A real `pg_ctl restart` under load
  found that a restarting server's `57P03` (`cannot_connect_now`) was not classified as transient
  (5 API 500s and 24 acknowledged deliveries stranded until lease expiry); connection-level
  SQLSTATEs (`08xxx`, `57P01-57P03`, `53300`) are now transient and the rerun had 0 500s, 0 stranded,
  0 lost, 0 duplicates.
- Worker performance from profiling: sharded httpcore pools (≤ 16 connections per shard; 120.7 →
  472.0 deliveries/s for a 128-slot worker), explicit `sniffio` dependency, and a saturation refill
  linger (`WORKER_REFILL_LINGER_MS`).
- [docs/capacity.md](docs/capacity.md) with measured numbers only (one shared 8-vCPU box running
  everything, stated up front): ~200 ingest req/s per API process (p99 107.6 ms), 800 with four;
  265-430 deliveries/s per worker process (CPU-bound, ≈ 2 ms CPU per delivery); 547/s with 2 workers
  on 16 endpoints; 300 deliveries/s steady state at p95 lag 180 ms with 2 workers × 32; default DB
  pool sufficient up to 128 slots; ~1.4 KB per event and ~2.1 KB per delivery + attempt on disk;
  partitioning / retention strategy, scaling limits and a sizing formula.
- New settings `WORKER_REFILL_LINGER_MS`, `WORKER_COMPLETE_RETRIES`, `WORKER_COMPLETE_RETRY_BASE_MS`,
  `WORKER_COMPLETE_RETRY_MAX_MS`; new entry point `hookrelay-mock-receiver`; `make test-chaos`,
  `make mock-receiver`, `make loadtest-smoke`. No schema migration.
- 943 tests (711 unit, 232 integration on Postgres 16 + Redis), 97% line+branch coverage; ruff, ruff
  format, mypy strict, Alembic upgrade / downgrade roundtrip + `alembic check` pass; both CI k6 smokes
  pass locally (0 lost, 0 acknowledged twice).
- Next: Milestone 9 (time partitioning, batched completions, PgBouncer, Helm + autoscaling).

### 2026-11-12: Milestone 9a (batched completions + PgBouncer)

- Group commit for worker completions: `CompletionBatcher` (generic, DB-free flush policy: full,
  every in-flight attempt waiting, `WORKER_COMPLETE_BATCH_LINGER_MS` after the oldest entry, or
  drain) feeds `delivery_queue.complete_many()`: one `UPDATE deliveries ... FROM unnest(...)` fenced
  per `(id, lease_token)` with rows locked in id order, one multi-row `delivery_attempts` insert, and
  breaker updates in endpoint order (consecutive successes collapsed). A lost lease skips only its
  row; a stale token or a duplicated `(delivery, token)` in the same batch never lands twice;
  transient errors retry the whole batch (idempotent by fencing); any other error re-writes the
  batch one attempt per transaction so only the poisonous row fails; drain writes queued results
  before releasing leases; lease renewal now locks in id order too (no renewal / completion
  deadlocks). New metrics `hookrelay_worker_completion_batch_size`,
  `hookrelay_worker_completion_flush_seconds`, `hookrelay_worker_completion_retries_total`.
- PgBouncer in transaction mode: compose `pgbouncer` service (`edoburu/pgbouncer:v1.26.1-p0`,
  `deploy/pgbouncer/pgbouncer.ini`: 20 + 5 reserve server connections, cap 40) in front of the API,
  relay, worker and retention job; `DB_PGBOUNCER=true` turns off asyncpg's statement caches, uses
  unique prepared-statement names and moves `statement_timeout` / `idle_in_transaction_session_timeout`
  to the pooler's `connect_query`; Alembic uses `DATABASE_DIRECT_URL`. A unit test audits that the
  code uses no session-level Postgres feature; integration tests run against a real PgBouncer
  (default asyncpg settings proven to break, pooled mode survives many clients on few server
  connections, full ingest → fan-out → batched delivery through the pooler). CI installs PgBouncer
  for the test job and adds a k6 e2e smoke behind it in the loadtest job.
- Measured on the shared 8-vCPU box (docs/capacity.md section 11), 1 worker × 128 slots draining
  ~12,000 deliveries against 50 ms receivers, 0 lost / 0 duplicates in every run: batch 1 → 32 took
  388.5 → 759.6 deliveries/s (329 → 694-716 on 11-11), commits per delivery ≈ 1.08 → ≈ 0.08, worker
  CPU per delivery ≈ 2.5 → 1.1 ms; through PgBouncer 372.2 (batch 1) and 618.5 (batch 32).
- New settings `WORKER_COMPLETE_BATCH_MAX` (32), `WORKER_COMPLETE_BATCH_LINGER_MS` (5),
  `DB_PGBOUNCER`, `DATABASE_DIRECT_URL`. No schema migration (head stays `0010`).
- 992 tests (744 unit, 248 integration on Postgres 16 + Redis + PgBouncer), 97% line+branch coverage; ruff, ruff format, mypy strict, Alembic upgrade / downgrade roundtrip + `alembic check`
  pass; integration suite runs with no skips (PgBouncer tests included).
- Next: Milestone 9b, native range partitioning of `delivery_attempts` (then `deliveries` / `events`).

### 2026-11-13: Milestone 9b (production deploy: Helm, probes, autoscaling, kind CI)

- Helm chart `deploy/helm/hookrelay` (chart 0.1.0, works with Helm 3.19 and 4.3): API / relay /
  worker Deployments from the one image (`hookrelay`, `hookrelay-relay`, `hookrelay-worker`), a
  retention CronJob (`concurrencyPolicy: Forbid`), API Service, ServiceAccount (token not mounted),
  ConfigMap for non-secret settings, Secret via `envFrom` (`secrets.existingSecret`, or
  chart-managed values for dev / CI only), `values.schema.json` (unknown keys rejected), NOTES and
  `helm test` (`/ready` through the Service). Pods: UID 10001, read-only root FS, all capabilities
  dropped, seccomp `RuntimeDefault`, memory-backed `/tmp`, soft topology spread, config / secret
  checksums roll the pods.
- Migrations: Alembic as a `pre-install,pre-upgrade` hook Job (weight 0) with a hook-scoped
  ServiceAccount and (dev / CI) Secret at weight -10, since the chart's own do not exist yet at
  pre-install; settings inlined, `DATABASE_DIRECT_URL` (never PgBouncer), `backoffLimit` 2.
- Probes and shutdown: API `/health` startup + liveness, `/ready` readiness, `preStop` sleep 5 s,
  new `API_GRACEFUL_SHUTDOWN_S` (20) for uvicorn, grace 40 s; relay / worker exec probes on their
  heartbeat files through a new stdlib-only `python -m hookrelay.healthcheck --heartbeat relay|worker`
  (~0.06 s vs ~0.7 s CPU per probe for `python -m hookrelay.worker --healthcheck`); worker grace 45 s
  ≥ `WORKER_DRAIN_TIMEOUT_S` 25 + 10. The chart fails rendering when a budget is violated, when a
  secret / credential-looking key or a chart-owned setting is put in `config`, or when
  `DB_PGBOUNCER=true` has no direct URL for migrations. PDBs `maxUnavailable: 1` per role.
- Autoscaling: `autoscaling/v2` HPA on CPU for the API; KEDA ScaledObject for workers on
  `max(hookrelay_deliveries_due) + max(hookrelay_deliveries_delivering)` per 75% of
  `WORKER_CONCURRENCY`, `ignoreNullValues: false` + fallback replicas, scale-down stabilisation,
  optional `hookrelay_outbox_pending` trigger. New gauge `hookrelay_deliveries_due` (pending, due
  now, endpoint breaker not cooling down) because `deliveries_pending` also counts far-future
  retries and breaker-blocked backlogs. ServiceMonitor (API) + PodMonitor (relay / worker
  side-car ports) with `honorLabels` so the app's `service` label survives.
- CI: new `helm` job: `deploy/helm/ci/validate.sh` (`helm lint --strict`, `helm template` for four
  value sets through `kubeconform -strict` with KEDA / Prometheus Operator CRD schemas, nine
  guard-rail cases that must fail), the chart-invariant pytest (must not skip there), then kind:
  image build + `kind load`, in-cluster Postgres 16 / Redis 7 / mock receiver
  (`deploy/helm/ci/deps.yaml`), `helm install --wait`, schema at head, `helm test`, `/ready`, the
  k6 e2e smoke through port-forwards (no lost / duplicated deliveries), `helm upgrade` (migration
  hook re-runs), worker `SIGTERM` drain. The lint job now runs actionlint (with shellcheck) and the
  existing run blocks were made shellcheck-clean.
- Verified locally: helm lint / template / kubeconform (Kubernetes 1.34 schemas) and the guard
  rails with Helm 4.3.1 and 3.19.0; actionlint 1.7.7 + shellcheck 0.10 clean. No docker / kind on
  the build box, so the kind job has not run yet; instead the exact rendered commands, env and
  probe commands of `values-kind.yaml` ran as local processes: migration Job twice (idempotent,
  `0010 (head)`), all probes passing (exec probes ~60 ms), the k6 e2e smoke 302 / 302 deliveries
  acknowledged, 0 lost, 0 duplicates, and a worker `SIGTERM` exit 0 with `worker.stopped`.
- 1022 tests (773 unit, 249 integration on Postgres 16 + Redis + PgBouncer), 97% line+branch
  coverage (missed lines 155 → 143); ruff, ruff format, mypy strict, Alembic roundtrip + `alembic
  check` pass. No schema migration (head stays `0010`).
- Next: native range partitioning of `delivery_attempts` (then `deliveries` / `events`) and the
  multi-node capacity re-run.

### 2026-11-14: Milestone 9c (partitioned `delivery_attempts`, kind CI fix)

- CI fix first: the kind job of run #141 failed because `hookrelay-mock-receiver` crash-looped.
  Kubernetes injects service-link variables for every Service in the namespace, so the CI Service
  `mock-receiver` produced `MOCK_RECEIVER_PORT=tcp://10.96.23.64:9000`, and the receiver converted its
  env defaults with an eager `int()`. The later "Error: release: not found" was only the
  diagnostics calling `helm history` before `helm install` had run. Now every pod spec (chart and
  `deploy/helm/ci/deps.yaml`) sets `enableServiceLinks: false`. The same collision was latent for
  the app: a Service named `metrics` would set `METRICS_PORT`. The receiver reports a bad port or
  number as a usage error naming the variable, with an `enableServiceLinks` hint. A chart test
  checks every rendered pod and the kind dependencies. `kind-smoke.sh` prints previous-container
  logs and restart counts, guards `helm history`, skips terminating worker pods, and runs the
  partition CronJob once as a Job.
- Migration `0011`: `delivery_attempts` is `PARTITION BY RANGE (created_at)`, one partition per UTC
  day that has rows plus today .. today + 7, and a DEFAULT. Rows are copied before indexes exist;
  keys, indexes, FKs and the trigger are re-created on the parent (cloned to partitions). The PK
  is now `(id, created_at)` and the attempt-number unique key gained `created_at` (Postgres
  requires the partition key). Uniqueness across days rests on the lease fence; a test pins that.
  The downgrade copies back into the `0010` shape and fails loudly on a duplicate. Alembic ignores
  partitions (`include_name`), so `alembic check` stays clean.
- `hookrelay-partitions` (`python -m hookrelay.partitions [--dry-run]`; compose `partitions`,
  profile `ops`; Helm `partitions` CronJob, hourly): session advisory lock on a direct connection
  (single runner, a second one exits 0), `FINALIZE` of pending detaches, idempotent premake via
  `CREATE TABLE LIKE` + bound `CHECK` + `ATTACH` (moving rows out of DEFAULT first), retention by
  `DETACH` (plain while a DEFAULT exists, since Postgres refuses `CONCURRENTLY` then; `CONCURRENTLY`
  otherwise) + `DROP`, orphan drop, batched DEFAULT sweep. Every DDL statement runs under
  `lock_timeout` with capped-backoff retries; exhausted steps are deferred to the next run (exit
  3). New settings `PARTITION_PREMAKE_DAYS` (7), `PARTITION_RETENTION_DAYS` (0 = follow
  `EVENT_RETENTION_DAYS`, validated `>=` it), `PARTITION_LOCK_TIMEOUT_MS` (2000),
  `PARTITION_DDL_RETRIES` (5). New gauges `hookrelay_partitions`,
  `hookrelay_partition_premade_seconds`, `hookrelay_partition_default_rows`.
- Integration tests on Postgres 16: routing (including DEFAULT and the endpoint trigger), FKs and
  cascade, `EXPLAIN` plan-time pruning and run-time pruning (`Subplans Removed`), premake
  idempotency, the DEFAULT → partition move, retention drop + sweep + orphan + dry run, a
  concurrent detach without DEFAULT, `FINALIZE` of an interrupted one, lock-timeout retries then
  deferral, a second runner, gauges, CLI exit codes, and a data-carrying `0010 → 0011 → 0010 →
  0011` roundtrip.
- Known limitation, recorded above: the event purge (`ON DELETE CASCADE`) still removes most
  attempt rows before their day can be dropped. [docs/partitioning.md](docs/partitioning.md)
  covers this, the online migration paths and the alerts.
- 1066 tests (800 unit, 266 integration on Postgres 16 + Redis + PgBouncer, no skips), 97%
  line+branch coverage; ruff, ruff format, mypy strict, Alembic upgrade / downgrade roundtrip +
  `alembic check`, helm lint / template / kubeconform / guard rails, actionlint and shellcheck pass.
  Migration head `0011`. The kind job itself still runs only in GitHub Actions (no docker on the
  build box).
- Next: partition `deliveries` / `events` with the partition key aligned to the delivery's
  creation day, and the multi-node capacity re-run.
