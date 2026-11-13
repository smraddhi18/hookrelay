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

> **Status:** Milestones 0 (foundation), 1 (tenancy), 2 (ingestion), 3 (delivery workers),
> 4 (back-pressure and failure isolation), 5 (operability for tenants), 6 (Redis), 7 (observability)
> and 8 (load, chaos and capacity) are complete; Milestone 9 is in progress: batched (group-committed)
> completion writes and PgBouncer transaction pooling (9a) and a production Kubernetes deploy (9b:
> Helm chart with a migration hook, probes, PDBs, HPA for the API and KEDA for workers, linted,
> kubeconform-validated and smoke-tested on kind in CI) have landed; time partitioning is next. Tenants authenticate with hashed, role-scoped API keys, manage endpoints, event types
> and wildcard subscriptions, and publish events through `POST /v1/events` with `Idempotency-Key`
> support. Each event and its outbox row commit in one transaction; a relay process fans the
> outbox out into exactly one `delivery` per (event, endpoint); and delivery workers lease due
> deliveries with `SKIP LOCKED` + fencing tokens, POST them signed (Standard Webhooks) through an
> SSRF-guarded HTTP client, record every attempt and retry with full-jitter backoff until the
> delivery succeeds, fails permanently (`failed`) or exhausts its retries (`dead`, the dead-letter
> state). Every endpoint has a Postgres-persisted circuit breaker, an in-flight cap and an
> optional rate limit enforced at lease time, so one broken receiver cannot starve the others;
> endpoints failing for days are auto-disabled with an operator notification, and finished
> deliveries can be replayed singly, per event or in bulk. Tenants rotate signing secrets with a
> dual-signing overlap window, search their delivery log, read a per-endpoint health summary, and
> a bounded, concurrent-safe retention job drops old payloads and ended secret overlaps.
> Redis accelerates wake-ups, fan-out lookups and rate limits without being needed for
> correctness; Prometheus metrics, OpenTelemetry traces and a tenant dashboard make it observable;
> and k6 load tests, chaos tests (worker `SIGKILL`, Postgres restarts, slow / failing receivers)
> and a measured [capacity plan](docs/capacity.md) show how far one box goes and what breaks first.

## Features

| Area | Feature | Status |
| --- | --- | --- |
| Foundation | App factory, typed env config with validation, `.env.example` | ✅ M0 |
| Foundation | structlog JSON logs, request-ID propagation (`X-Request-ID`), access log | ✅ M0 |
| Foundation | Uniform error envelope (`AppError`, validation, HTTP, unhandled) | ✅ M0 |
| Foundation | `/health` liveness, `/ready` readiness with Postgres timeout + drain awareness | ✅ M0 |
| Foundation | Async SQLAlchemy 2 + asyncpg, Alembic (async), baseline `tenants` schema | ✅ M0 |
| Foundation | Docker (multi-stage, non-root), compose stack, GitHub Actions CI | ✅ M0 |
| Tenancy | Operator API (create / get / list / suspend / resume tenants) behind a constant-time-checked token | ✅ M1 |
| Tenancy | Tenant API keys: shown once, SHA-256 at rest, prefix lookup, roles `admin` / `publisher` / `reader`, expiry, rotation with overlap window, revocation | ✅ M1 |
| Tenancy | Deny-by-default auth (audited in tests), tenant scoping in every query and in composite FKs | ✅ M1 |
| Tenancy | Endpoints CRUD: URL policy, header deny-list, timeout bounds, AES-256-GCM-encrypted signing secret (admin-only reveal), soft delete | ✅ M1 |
| Tenancy | Event-type registry (archive / unarchive) and subscriptions with `*` / `invoice.*` wildcards, fan-out preview | ✅ M1 |
| API | Keyset cursor pagination, `ETag` / `If-Match` optimistic concurrency (428 / 412), OpenAPI examples + security schemes | ✅ M1 |
| Ingestion | `POST /v1/events`: strict JSON (media type, size limits checked before and while reading, depth limit, no `NaN` / `\u0000`), registered non-archived types, `external_id` uniqueness, `occurred_at` skew check | ✅ M2 |
| Ingestion | `Idempotency-Key`: tenant-scoped, request fingerprint, replayed response (`Idempotent-Replayed: true`), `422` on mismatch, concurrent duplicates serialised (`409` + `Retry-After` past a lock timeout), TTL, errors never cached | ✅ M2 |
| Ingestion | Transactional outbox: event + outbox row (+ idempotency record) in one transaction | ✅ M2 |
| Ingestion | Fan-out relay process: `SKIP LOCKED` batches, `ON CONFLICT DO NOTHING` deliveries (exactly one per event and endpoint), poison-row isolation with backoff, adaptive polling, heartbeat health check, graceful `SIGTERM` | ✅ M2 |
| Ingestion | Event read APIs (list with filters, get with payload, deliveries per event) and retention (`retain_until`, purged by the relay) | ✅ M2 |
| Delivery | Worker process: `SKIP LOCKED` leases sized to free slots, lease expiry + renewal, fencing tokens on every transition, expired-lease reaper, concurrency semaphore, heartbeat `--healthcheck`, `--once`, `SIGTERM` drain that releases unfinished leases unspent | ✅ M3 |
| Delivery | Standard-Webhooks signing (`webhook-id` / `webhook-timestamp` / `webhook-signature`, HMAC-SHA256) plus a dependency-free `verify()` helper for receivers | ✅ M3 |
| Delivery | httpx client: connect + total timeouts, response-body cap, no redirects, no proxies from env, connection reuse | ✅ M3 |
| Delivery | Retries: 5xx / 408 / 409 / 425 / 429 / timeouts / connection errors, full-jitter exponential backoff, capped `Retry-After` floor, max attempts and max age → terminal `failed` | ✅ M3 |
| Delivery | SSRF guard: resolve-then-connect to a vetted, pinned IP (every resolved address checked, re-resolved per connection), private / loopback / CGNAT / link-local / metadata / IPv4-mapped IPv6 blocked, scheme and port allow-list; dev opt-in refused in production | ✅ M3 |
| Delivery | Attempt log (`delivery_attempts`: status, latency, truncated response, error class, worker, masked request headers) and `GET /v1/deliveries/{id}` / `.../attempts` | ✅ M3 |
| Isolation | Per-endpoint circuit breaker (closed / open / half-open) persisted in `endpoint_states`: consecutive availability failures open it with exponential cooldown, one half-open probe closes or re-opens it; deliveries wait in `pending` without spending attempts; `GET /v1/endpoints/{id}/circuit` | ✅ M4 |
| Isolation | Per-endpoint `max_in_flight` (deployment default + cap) and `rate_limit_per_s` token bucket, enforced in the lease statement across all workers; round-robin allocation with least-recently-leased fairness | ✅ M4 |
| Isolation | Dead-letter state: retries exhausted (attempts or age, per run) → `dead`; permanent errors stay `failed`; the reaper dead-letters expired leases on the last attempt | ✅ M4 |
| Isolation | Auto-disable after sustained failure (`ENDPOINT_AUTO_DISABLE_AFTER_S`) with an `endpoint.disabled` notification in a transactional notification outbox, dispatched by the relay with retries; re-enabling resets the breaker | ✅ M4 |
| Isolation | Replay API: one delivery, an event's deliveries, or a bulk time window + filters (admin); fresh retry budget, same `webhook-id`; `Idempotency-Key`, per-tenant rate limit (`429` + `Retry-After`), capped batches with `has_more`, audit trail `GET /v1/replays` | ✅ M4 |
| Operability | Signing-secret rotation with a dual-signing overlap window (both secrets sign, space-separated `v1,` signatures), `If-Match` required, bring-your-own secret, early end of the overlap; the previous secret stays AES-256-GCM-encrypted | ✅ M5 |
| Operability | Delivery log search `GET /v1/deliveries`: status (any of several), event type, endpoint, `[created_after, created_before)`; keyset pagination served by dedicated indexes | ✅ M5 |
| Operability | Endpoint health `GET /v1/endpoints/{id}/health`: success rate, p50 / p95 / p99 latency, errors by class, last error, breaker state, backlog; bounded window and sample | ✅ M5 |
| Operability | Payload retention (`EVENT_PAYLOAD_RETENTION_DAYS`) + cleanup job: batched, budgeted, `SKIP LOCKED`, never purges a payload still being delivered; runs in relay housekeeping or as `hookrelay-retention` | ✅ M5 |
| Redis | Worker wake-ups via Redis pub/sub (Postgres poll fallback); subscription-cache for fan-out with explicit invalidation; Lua token-bucket rate limits with Postgres degraded mode (never fail open) | ✅ M6 |
| Observability | Prometheus metrics, OpenTelemetry tracing, dashboard API | ✅ M7 |
| Scale | k6 load tests (`loadtest/`): constant-rate ingest and end-to-end delivery to `hookrelay-mock-receiver` (latency, jitter, failure / `429` / hang rates per endpoint), failing on any lost or twice-acknowledged delivery; CI smoke job | ✅ M8 |
| Scale | Chaos tests on real Postgres: worker `SIGKILL` mid-delivery, backend-termination storms, a backend killed between the receiver's 2xx and the completion write, slow / flaky / hanging receivers, drain during an outage; a real `pg_ctl restart` under load (`CHAOS_CMD`) | ✅ M8 |
| Scale | Restart-safe write paths: transient DB errors (incl. `57P03` while Postgres restarts) are `503` + `Retry-After` on the API and fenced, retried completions in workers; sharded HTTP connection pools; measured [capacity plan](docs/capacity.md) | ✅ M8 |
| Scale | Group-committed completion writes in workers: up to `WORKER_COMPLETE_BATCH_MAX` finished attempts per transaction, each row still fenced by its own lease token; partial batches skip only the fenced-out rows, a poisonous row falls back to one transaction per attempt, drain writes queued results instead of releasing them | ✅ M9 |
| Scale | PgBouncer (transaction pooling) in compose and CI: `DB_PGBOUNCER=true` disables asyncpg statement caches and uses unique prepared-statement names, server-side timeouts move to the pooler, migrations bypass it (`DATABASE_DIRECT_URL`), a unit test audits that no session-level Postgres feature is used | ✅ M9 |
| Deploy | Helm chart (`deploy/helm/hookrelay`): API / relay / worker Deployments from the same image, Alembic `pre-install,pre-upgrade` hook Job through `DATABASE_DIRECT_URL`, retention CronJob, ConfigMap + `existingSecret` (secrets refused in the ConfigMap), non-root read-only pods, `values.schema.json` | ✅ M9 |
| Deploy | Probes: `/health` liveness, `/ready` readiness for the API; stdlib-only heartbeat exec probes for relay / worker (`python -m hookrelay.healthcheck --heartbeat worker`, ~0.06 s vs ~0.7 s CPU); `preStop` + `API_GRACEFUL_SHUTDOWN_S` and worker drain budgets enforced against `terminationGracePeriodSeconds`; PodDisruptionBudgets | ✅ M9 |
| Deploy | Autoscaling: HPA (`autoscaling/v2`) on CPU for the API; KEDA Prometheus scaler for workers on delivery demand (`hookrelay_deliveries_due` + `hookrelay_deliveries_delivering`) with a fallback replica count when metrics are missing; ServiceMonitor / PodMonitor | ✅ M9 |
| Deploy | CI `helm` job: `helm lint --strict`, `helm template` × 4 value sets through `kubeconform -strict` (+ KEDA / Prometheus Operator CRD schemas), guard-rail and chart-invariant tests, then a kind cluster: install, `helm test`, k6 e2e (no lost / duplicated deliveries), upgrade, worker `SIGTERM` drain | ✅ M9 |
| Scale | Time partitioning of hot tables | 🔜 M9 |

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

    PGB["PgBouncer (M9)<br/>transaction pooling<br/>many clients → few<br/>server connections"]
    MIG["alembic migrate job"]

    subgraph Workers["Workers (N replicas)"]
        FAN["Fan-out relay<br/>outbox → deliveries"]
        DW["Delivery workers<br/>SKIP LOCKED leases,<br/>group-committed completions"]
    end

    subgraph Edge["Egress"]
        SSRF["SSRF guard<br/>+ HMAC signer"]
    end

    EP["Tenant endpoints<br/>(HTTPS receivers)"]
    OBS["Prometheus / Grafana / Jaeger"]

    subgraph Harness["Load & chaos harness (M8, dev / CI only)"]
        K6["k6<br/>ingest.js / e2e.js"]
        MOCK["hookrelay-mock-receiver<br/>latency, failures, hangs,<br/>ack accounting"]
        CHAOS["Chaos: worker SIGKILL,<br/>Postgres restart / backend kills"]
    end

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
    DW -->|"lease; batched, fenced<br/>completions; retry / DLQ"| DL
    API ==>|"all SQL (DB_PGBOUNCER=true)"| PGB
    Workers ==>|all SQL| PGB
    PGB ==>|"default_pool_size<br/>server connections"| PG
    MIG ==>|"DATABASE_DIRECT_URL<br/>(session state)"| PG
    DW --> SSRF --> EP
    DW -. rate limit / breaker .-> R
    API -. metrics, traces .-> OBS
    Workers -. metrics, traces .-> OBS
    K6 -.->|"constant-rate POST /v1/events"| ING
    SSRF -.->|"loopback in dev / CI"| MOCK
    K6 -.->|"poll /stats: lost or acked twice?"| MOCK
    CHAOS -. faults .-> PG
    CHAOS -. faults .-> DW
```

Thin arrows into Postgres show *logical* data flow; physically every API, relay and worker
connection goes through PgBouncer (thick arrows) and only migrations connect directly.

### Tenancy and auth (Milestone 1)

```mermaid
flowchart LR
    OP["Operator<br/>(platform admin)"] -->|"Bearer OPERATOR_TOKEN"| MW
    TC["Tenant client<br/>(dashboard / CLI / service)"] -->|"Bearer hrk_&lt;prefix&gt;_&lt;secret&gt;"| MW

    subgraph App["FastAPI app (create_app, stateless)"]
        MW["RequestContextMiddleware<br/>request ID, structlog context, access log"]
        EH["Exception handlers<br/>→ one error envelope"]
        subgraph Auth["Deny-by-default auth (api/deps.py)"]
            OPA["require_operator<br/>constant-time compare"]
            KA["authenticate<br/>format → prefix lookup → SHA-256 compare<br/>→ revoked / expired / suspended"]
            RR["require_role<br/>reader &lt; publisher &lt; admin"]
        end
        subgraph Routers["Routers (thin: HTTP ↔ contracts)"]
            ADM["/admin/tenants"]
            V1["/v1/api-keys, /v1/endpoints,<br/>/v1/event-types, subscriptions, /v1/me"]
            H["/health, /ready"]
        end
        subgraph Services["Services (framework-free, tenant_id on every query)"]
            TS["tenants"]
            AK["api_keys"]
            ES["endpoints + endpoint_rules"]
            ET["event_types / subscriptions"]
        end
        SB["SecretBox<br/>AES-256-GCM, key id, AAD"]
        MW --> OPA --> ADM --> TS
        MW --> KA --> RR --> V1
        V1 --> AK
        V1 --> ES
        V1 --> ET
        ADM --> AK
        ES --> SB
        MW --> H
        MW --- EH
    end

    subgraph PG["Postgres 16"]
        T[(tenants)]
        K[(api_keys)]
        E[(endpoints)]
        EVT[(event_types)]
        S[(subscriptions)]
    end
    TS --> T
    AK --> K
    ES --> E
    ET --> EVT
    ET --> S
    S -. "composite FK (tenant_id, endpoint_id)" .-> E
    S -. "composite FK (tenant_id, event_type_id)" .-> EVT
    MIG["alembic upgrade head<br/>(one-shot job)"] --> PG
    App -->|"JSON logs, secrets redacted"| OUT[(stdout)]
```

### Ingestion and fan-out (Milestone 2)

```mermaid
flowchart LR
    PUB["Producer<br/>(publisher key)"] -->|"POST /v1/events<br/>Idempotency-Key"| ING

    subgraph API["FastAPI app"]
        ING["events router<br/>read_body: media type, size, depth<br/>canonical JSON + fingerprint"]
        IDS["idempotency.claim<br/>INSERT ... ON CONFLICT<br/>(row lock = in-flight marker)"]
        EVS["events.ingest_event<br/>type check, external_id, skew"]
        READ["GET /v1/events<br/>/v1/events/{id}<br/>/v1/events/{id}/deliveries"]
        ING --> IDS --> EVS
    end

    subgraph PG["Postgres 16"]
        IK[(idempotency_keys)]
        EV[(events)]
        OB[(event_outbox<br/>partial index: pending)]
        DL[(deliveries<br/>unique event_id, endpoint_id)]
        SUB[(subscriptions +<br/>endpoints)]
    end

    IDS --> IK
    EVS -->|"one transaction:<br/>event + outbox row<br/>+ stored response"| EV
    EVS --> OB
    READ --> EV
    READ --> DL

    subgraph Relay["Relay processes (python -m hookrelay.relay, N replicas)"]
        CL["claim batch<br/>FOR UPDATE SKIP LOCKED"]
        FO["fan-out INSERT ... SELECT<br/>ON CONFLICT DO NOTHING"]
        PZ["per-row savepoints on failure<br/>→ backoff via available_at"]
        HK["housekeeping<br/>expired keys / processed outbox / expired events"]
        CL --> FO
        FO -. batch failed .-> PZ
    end

    OB --> CL
    SUB --> FO
    FO -->|"mark processed_at<br/>same transaction"| DL
    HK --> PG
    DL -. "leased by" .-> W["Delivery workers (M3)"]
```

### Delivery workers (Milestone 3)

```mermaid
flowchart LR
    subgraph PG["Postgres 16"]
        DL[(deliveries<br/>pending / delivering /<br/>succeeded / failed)]
        DA[(delivery_attempts<br/>append-only)]
        EPT[(endpoints<br/>encrypted secret)]
    end

    subgraph Worker["Delivery workers (python -m hookrelay.worker, N replicas)"]
        LS["lease_due<br/>FOR UPDATE SKIP LOCKED<br/>≤ free slots, new lease_token"]
        SEM["semaphore<br/>WORKER_CONCURRENCY"]
        SG["sign<br/>Standard Webhooks HMAC-SHA256"]
        HC["httpx client<br/>timeouts, body cap,<br/>no redirects"]
        GB["SSRF backend<br/>resolve → vet every IP →<br/>connect to pinned IP"]
        CP["complete<br/>WHERE lease_token = mine<br/>+ attempt row"]
        RN["renew leases<br/>(lost lease → cancel)"]
        RP["reaper<br/>expired leases → retry / failed"]
        LS --> SEM --> SG --> HC --> GB
        HC --> CP
    end

    subgraph Receivers["Tenant receivers"]
        R1["https://api.acme.example/hooks<br/>verify(secret, headers, body)"]
        R2["https://hooks.globex.example"]
    end

    DL --> LS
    EPT --> SG
    GB -->|"POST, Host/SNI = name"| R1
    GB --> R2
    GB -. "blocked: 10/8, 127/8, 100.64/10,<br/>169.254.169.254, ::ffff:..." .-x X["internal network"]
    CP -->|"succeeded / pending + next_attempt_at / failed"| DL
    CP --> DA
    RN --> DL
    RP --> DL
    RP --> DA
    API["GET /v1/deliveries/{id}<br/>/v1/deliveries/{id}/attempts"] --> DL
    API --> DA
```

### Back-pressure and failure isolation (Milestone 4)

```mermaid
flowchart LR
    subgraph PG["Postgres 16"]
        DL[(deliveries<br/>pending / delivering / succeeded /<br/>failed / dead)]
        ES[(endpoint_states<br/>breaker + token bucket)]
        EPT[(endpoints<br/>max_in_flight, rate_limit_per_s,<br/>enabled, auto_disabled_at)]
        NT[(notifications<br/>outbox)]
        RR[(replay_requests<br/>audit + rate ledger)]
    end

    subgraph Worker["Delivery worker"]
        EL["1 eligible endpoints<br/>FOR UPDATE OF state SKIP LOCKED<br/>in-flight < cap, breaker allows,<br/>tokens ≥ 1, least recently leased first"]
        AL["2 allocate free slots<br/>round-robin, ≤ per-endpoint limit<br/>(1 probe if half-open)"]
        LS["3 lease per endpoint<br/>SKIP LOCKED LIMIT quota"]
        SP["4 spend tokens,<br/>open → half_open"]
        CP["complete (fenced)<br/>→ record_outcome"]
        EL --> AL --> LS --> SP
    end

    subgraph Relay["Fan-out relay"]
        ND["notify_once<br/>SKIP LOCKED, retry, abandon"]
    end

    subgraph API["API"]
        RP["POST /v1/deliveries/{id}/replay<br/>POST /v1/events/{id}/replay<br/>POST /v1/replays (admin)"]
        CI["GET /v1/endpoints/{id}/circuit"]
    end

    ES --> EL
    DL --> LS
    SP --> ES
    CP -->|"success → closed<br/>failure ≥ threshold → open"| ES
    CP -->|"sustained failure"| EPT
    CP -->|"endpoint.disabled"| NT
    NT --> ND --> OPS["Notifier<br/>(log / pager / email)"]
    RP -->|"advisory lock + 60 s count"| RR
    RP -->|"pending, fresh run budget"| DL
    CI --> ES
```

### Operability for tenants (Milestone 5)

```mermaid
flowchart LR
    subgraph API["API"]
        RS["POST /v1/endpoints/{id}/secret/rotate<br/>(admin, If-Match)"]
        EO["DELETE /v1/endpoints/{id}/secret/previous"]
        SR["GET /v1/deliveries<br/>status, event_type, endpoint, time"]
        HL["GET /v1/endpoints/{id}/health<br/>window_s"]
    end

    subgraph PG["Postgres 16"]
        EPT[(endpoints<br/>secret + previous secret<br/>+ overlap expiry)]
        DL[(deliveries<br/>+ event_type via trigger)]
        DA[(delivery_attempts<br/>+ endpoint_id via trigger)]
        EV[(events<br/>payload / payload_purged_at)]
        ES[(endpoint_states)]
    end

    subgraph Worker["Delivery worker"]
        LS["lease: secret + unexpired<br/>previous secret"]
        SG["sign with both:<br/>v1,new v1,old"]
    end

    subgraph Ret["Retention (relay housekeeping<br/>or hookrelay-retention)"]
        PP["purge payloads<br/>SKIP LOCKED events,<br/>FOR SHARE deliveries"]
        XS["clear ended overlaps"]
    end

    RS -->|"version = If-Match:<br/>current → previous"| EPT
    EO --> EPT
    EPT --> LS --> SG --> RCV["Receiver<br/>(either secret verifies)"]
    SR -->|"(tenant, status or type, created_at, id)<br/>or (endpoint, created_at, id) indexes"| DL
    HL -->|"index-only scan,<br/>capped sample"| DA
    HL --> ES
    HL -->|"capped count"| DL
    PP -->|"all deliveries settled"| EV
    PP -->|"scrub response bodies"| DA
    XS --> EPT
```


### Redis (Milestone 6)

```mermaid
flowchart LR
    subgraph Producers["Relay / API / replays"]
        FAN["Fan-out relay<br/>inserts deliveries"]
        RP["Replay re-queue"]
        INV["Subscription / endpoint<br/>mutations"]
    end

    subgraph Redis["Redis 7 (optional accelerator)"]
        WAKE[("{prefix}:wake:deliveries<br/>pub/sub")]
        CACHE[("{prefix}:sub:{tenant}:{type}<br/>TTL + invalidate")]
        RL[("{prefix}:rl:endpoint:{id}<br/>Lua token bucket")]
    end

    subgraph Workers["Delivery workers"]
        SUB["WakeupSubscriber<br/>dedicated connection"]
        LOOP["Poll loop:<br/>wake → delay=0<br/>else adaptive PG poll"]
        LEASE["lease_due:<br/>Redis RL then dual-spend PG"]
    end

    subgraph PG["Postgres 16 (source of truth)"]
        DL[(deliveries)]
        SUBS[(subscriptions<br/>+ endpoints)]
        ES[(endpoint_states<br/>tokens / breaker / in-flight)]
    end

    FAN -->|"ON CONFLICT insert"| DL
    FAN -. coalesced publish .-> WAKE
    FAN -->|"cache-aside<br/>(SQL fallback)"| CACHE
    CACHE -. miss .-> SUBS
    RP -. wake .-> WAKE
    INV -. invalidate tenant .-> CACHE
    WAKE --> SUB --> LOOP --> LEASE
    LEASE -->|"take tokens"| RL
    LEASE -->|"max_in_flight + breaker<br/>+ dual-update tokens"| ES
    LEASE --> DL
```

When `REDIS_URL` is unset every Redis feature is off and the M0–M5 Postgres paths are unchanged.
Losing Redis after it was configured degrades latency / hot-path contention, never correctness or
at-least-once delivery.

### Load, chaos and capacity (Milestone 8)

```mermaid
flowchart LR
    subgraph Run["loadtest/run-local.sh (one machine)"]
        K6["k6 script<br/>ingest.js | e2e.js"]
        API["API<br/>(API_WORKERS uvicorn processes)"]
        REL["relay(s)"]
        WK["workers<br/>(WORKERS × WORKER_CONCURRENCY)"]
        MOCK["mock receiver<br/>?latency_ms&jitter_ms&failure_rate<br/>&failure_status&hang_rate"]
        CH["CHAOS_CMD after CHAOS_AFTER_S<br/>(e.g. pg_ctl restart)"]
    end
    PG[("Postgres 16")]
    RD[("Redis 7")]
    OUT["OUT_DIR: k6 summary JSON,<br/>receiver /stats, process logs"]

    K6 -->|"provision tenant + endpoints<br/>(operator API)"| API
    K6 -->|"constant-arrival-rate<br/>POST /v1/events"| API
    API --> PG
    REL --> PG
    WK --> PG
    REL -. wake-ups .-> RD
    RD -. wake-ups .-> WK
    WK -->|"signed POSTs"| MOCK
    K6 -->|"teardown: events × ENDPOINTS<br/>== acked_unique? duplicates == 0?"| MOCK
    CH -. fault .-> PG
    K6 --> OUT
    MOCK --> OUT
```

The mock receiver counts a delivery as **acknowledged** only when a 2xx was written to a client
that was still connected (a sender that timed out and hung up is `disconnected`), keyed by
`(path, webhook-id)`. "Lost" is `expected - acked_unique`; "acked twice" is the duplicate a
receiver would actually process twice. The chaos tests run the same receiver in-process and also
reconcile it against `delivery_attempts` (every extra acknowledgement must be explained by a
`lease_expired` attempt).

### Batched completions and PgBouncer (Milestone 9a)

```mermaid
flowchart LR
    subgraph W["Delivery worker process"]
        T1["attempt task 1"]
        T2["attempt task 2"]
        TN["attempt task N<br/>(WORKER_CONCURRENCY)"]
        B["CompletionBatcher<br/>flush when full | all in-flight waiting |<br/>linger elapsed | drain"]
        WR["writer: one transaction per batch<br/>transient error → retry whole batch<br/>other error → one tx per attempt"]
        REN["lease renewer / reaper"]
    end
    PGB["PgBouncer<br/>pool_mode = transaction"]
    PG[("Postgres 16<br/>deliveries · delivery_attempts ·<br/>endpoint_states")]

    T1 -->|"submit(attempt)"| B
    T2 --> B
    TN --> B
    B --> WR
    WR -->|"UPDATE … FROM unnest(ids, tokens, …)<br/>fenced per (id, lease_token), id order<br/>+ multi-row attempt INSERT<br/>+ breaker updates in endpoint order"| PGB
    REN -->|"renew: same id-ordered locks"| PGB
    PGB --> PG
```

### Kubernetes deployment (Milestone 9b)

```mermaid
flowchart TB
    subgraph Helm["helm install / upgrade"]
        HOOK["pre-install / pre-upgrade hooks<br/>ServiceAccount + Secret (weight -10)<br/>Job: alembic upgrade head (weight 0)"]
    end

    subgraph NS["namespace (one release)"]
        CM["ConfigMap<br/>non-secret settings"]
        SEC["Secret (existingSecret)<br/>DATABASE_URL, DATABASE_DIRECT_URL,<br/>REDIS_URL, OPERATOR_TOKEN, keys"]
        SVC["Service :80 → api :8000"]

        subgraph APID["Deployment api (HPA: CPU 70%)"]
            A1["api pod<br/>/health liveness · /ready readiness<br/>preStop 5 s + graceful 20 s < grace 40 s"]
        end
        subgraph RD["Deployment relay"]
            R1["relay pod<br/>heartbeat exec probe<br/>:9100 metrics"]
        end
        subgraph WD["Deployment worker (KEDA ScaledObject)"]
            W1["worker pods<br/>heartbeat exec probe · :9100 metrics<br/>drain 25 s + 10 ≤ grace 45 s"]
        end
        CJ["CronJob retention<br/>concurrencyPolicy: Forbid"]
        PDB["PodDisruptionBudgets<br/>maxUnavailable: 1 per role"]
        SM["ServiceMonitor (api) +<br/>PodMonitor (relay, worker)<br/>honorLabels"]
        SO["ScaledObject<br/>max(deliveries_due) + max(delivering)<br/>÷ 75% of WORKER_CONCURRENCY<br/>fallback replicas on metric errors"]
        HPA["HPA autoscaling/v2"]
    end

    PROM["Prometheus"]
    KEDA["KEDA operator"]
    PGB["PgBouncer<br/>(DB_PGBOUNCER=true)"]
    PG[("Postgres 16")]
    REDIS[("Redis 7")]
    EP["Tenant endpoints"]

    HOOK ==>|"DATABASE_DIRECT_URL<br/>(never the pooler)"| PG
    HOOK -->|"must succeed before<br/>new pods roll out"| APID
    CM -. envFrom .-> A1 & R1 & W1 & CJ
    SEC -. envFrom .-> A1 & R1 & W1 & CJ
    SVC --> A1
    A1 & R1 & W1 & CJ ==> PGB ==> PG
    A1 & R1 & W1 -.-> REDIS
    W1 -->|"signed POST"| EP
    SM --> PROM
    PROM -->|"scrape /metrics"| A1 & R1 & W1
    KEDA -->|"query"| PROM
    KEDA --> SO --> WD
    HPA --> APID
    PDB -. limits voluntary evictions .-> APID & RD & WD
```

One image, three long-running roles and two one-shot ones. The migration Job is a Helm hook, so
`helm upgrade` stops before any new pod starts if a migration fails, and old pods keep serving
the (backward-compatible) schema meanwhile. Pods read settings from the ConfigMap and secrets
from a Secret the operator manages (`secrets.existingSecret`); the chart refuses secret-looking
keys in the ConfigMap. Every pod runs as UID 10001 with a read-only root filesystem, all
capabilities dropped and a memory-backed `/tmp` (heartbeat files). Workers scale on queue demand
through KEDA, the API on CPU through an HPA.

### Request flow (with auth)

1. `RequestContextMiddleware` (pure ASGI) reads `X-Request-ID`. A caller-supplied ID is kept only
   if it is 1 to 128 chars of `[A-Za-z0-9._:-]`, otherwise a UUID is minted. This stops log injection.
   The ID is bound to structlog contextvars, so every log line of the request carries it.
2. **Authentication is opt-out, not opt-in.** Every `/v1` route depends on `authenticate` and every
   `/admin` route on `require_operator`. A unit test walks the OpenAPI spec and fails the build if
   any operation (other than `/health` and `/ready`) lacks a security requirement, and it sends
   an anonymous request to every operation expecting `401`.
3. `/admin/*`: the bearer token is compared with `OPERATOR_TOKEN` using `hmac.compare_digest`
   (constant time). No token configured → `403 operator_api_disabled`; wrong / missing → `401`.
4. `/v1/*`: the key must match `hrk_<12 char prefix>_<43 char secret>`; malformed keys are rejected
   **before** touching the database. The prefix (unique index) finds the row; the SHA-256 of the
   presented key is compared in constant time with the stored digest (an unknown prefix still
   compares against a dummy digest, so both failures look and time the same). Then:
   revoked → `401 api_key_revoked`, expired (judged by the **database** clock) →
   `401 api_key_expired`, tenant suspended → `403 tenant_suspended`. `last_used_at` is written in
   its own short transaction, throttled to `API_KEY_LAST_USED_RESOLUTION_S`.
5. The resulting `Principal` (tenant id, key id, role) is the only source of `tenant_id` for the
   services. Route handlers never take a tenant id from the path or body; every query filters on
   `principal.tenant_id`, so another tenant's resource is indistinguishable from a missing one (`404`).
6. `require_role` enforces `reader < publisher < admin` (`403 insufficient_role` with the required role).
7. The service runs inside one transaction (commit before the response is sent). Any error becomes
   `{"error": {"code", "message", "details", "request_id"}}`; unhandled exceptions are logged with a
   traceback **without frame locals** (so headers and keys never reach logs) and returned as a generic 500.
8. The response gets `X-Request-ID` (and `ETag` / `Location` / `Cache-Control: no-store` where relevant),
   and one `http.request` access-log line records method, path, status and duration.

### Ingest and fan-out flow (Milestone 2)

1. **Authenticate** a `publisher` (or `admin`) key. A malformed `Idempotency-Key` (not 1 to 255
   printable ASCII chars) is rejected with `400` before anything else happens.
2. **Read the body defensively.** Only `application/json` (or `+json`) is accepted (`415`). A
   `Content-Length` above `EVENT_MAX_PAYLOAD_BYTES` + 16 KiB envelope headroom is refused before
   reading; a chunked body is cut off at the same limit (`413 request_too_large`). JSON must be
   UTF-8 and at most 64 levels deep, without `NaN`/`Infinity`, `\u0000` or lone surrogates
   (which `jsonb` cannot store), so bad input is a `422`, never a `500`. The payload is
   re-serialised canonically (sorted keys, no whitespace) and its size checked against
   `EVENT_MAX_PAYLOAD_BYTES` (`413 payload_too_large`).
3. **Claim the idempotency key** (when sent): `INSERT ... ON CONFLICT (tenant_id, key) DO UPDATE ...
   WHERE expired` inside the request transaction, with `lock_timeout = IDEMPOTENCY_LOCK_TIMEOUT_MS`.
   - Row with a stored response and the same fingerprint (SHA-256 of method, path and canonical body):
     return the stored `202` body with `Idempotent-Replayed: true`. Nothing new is written.
   - Same key, different request: `422 idempotency_key_mismatch`.
   - Another request is still holding the key (uncommitted claim): this request waits on the row
     lock; past the timeout it gets `409 idempotency_key_in_progress` with `Retry-After: 1`.
4. **Write the event:** check the type is registered and not archived, `occurred_at` is at most
   `EVENT_OCCURRED_AT_MAX_FUTURE_S` ahead, then insert the `events` row (`retain_until = now() +
   EVENT_RETENTION_DAYS`) and its `event_outbox` row, and store the response on the idempotency row.
   One commit covers all three; any error rolls everything back, including the key claim.
5. Return `202 Accepted` with `Location: /v1/events/{id}`.
6. **Relay:** each relay loop claims up to `RELAY_BATCH_SIZE` due outbox rows `FOR UPDATE SKIP LOCKED`,
   inserts deliveries with one `INSERT ... SELECT` over enabled, non-deleted endpoints whose
   subscriptions match (exact or prefix wildcard) using `ON CONFLICT (event_id, endpoint_id) DO NOTHING`,
   and sets `processed_at` in the same transaction. If the batch statement fails, it retries row by
   row inside savepoints; a failing row gets `attempts + 1`, `last_error` and
   `available_at = now() + min(2^(attempts-1), RELAY_RETRY_MAX_S)` seconds so it cannot block the others.
7. **Housekeeping** (every `RELAY_HOUSEKEEPING_INTERVAL_S`, in bounded batches): delete expired idempotency
   keys, processed outbox rows older than `RELAY_OUTBOX_RETENTION_S`, and events past `retain_until`
   (their outbox rows and deliveries cascade).

### Delivery flow (Milestone 3)

1. **Lease.** Each worker loop leases at most `min(WORKER_BATCH_SIZE, free slots)` due deliveries:
   `status = 'pending' AND next_attempt_at <= now()` on an enabled, non-deleted endpoint of an active
   tenant, `FOR UPDATE OF d SKIP LOCKED`, oldest-due first. The same statement sets
   `status = 'delivering'`, a fresh `lease_token` (UUID), `leased_until = now() + WORKER_LEASE_S`,
   `leased_by` and `attempt_count + 1`. A CHECK makes "delivering without a lease" unrepresentable.
2. **Prepare.** The endpoint's signing secret is decrypted (AES-256-GCM, AAD bound to tenant and
   endpoint) and the body rendered once as compact JSON `{"type", "timestamp", "data"}`. Headers:
   the endpoint's custom headers, then `webhook-id` (the event id, stable across retries),
   `webhook-timestamp` and `webhook-signature: v1,<base64 HMAC-SHA256 of id.timestamp.body>`.
3. **Send.** A shared httpx client with the endpoint's `timeout_ms` as the total deadline and
   `DELIVERY_CONNECT_TIMEOUT_MS` for the handshake, `follow_redirects=False`, `trust_env=False`
   and `Accept-Encoding: identity`. Its network backend resolves the host, refuses the request if
   **any** resolved address is blocked, and connects to a vetted IP while the URL (and so `Host`
   and TLS SNI / certificate checks) keeps the name. At most `DELIVERY_RESPONSE_MAX_BYTES` of the
   response body is read.
4. **Classify.** 2xx → `succeeded`. 3xx → terminal (redirects are never followed). 408 / 409 / 425 /
   429 and 5xx, timeouts, connection / DNS / TLS / protocol errors → retryable. Other 4xx and a
   blocked destination → terminal `failed`.
5. **Complete under the fence.** One `UPDATE ... WHERE id = :id AND lease_token = :mine AND
   status = 'delivering'` sets the new state (`succeeded`, `failed` with `completed_at`, or
   `pending` with `next_attempt_at`), `last_status_code` and `last_error_class`, and clears the
   lease; the attempt row is inserted in the same transaction. Zero rows updated means another
   worker owns the delivery now: the result is discarded (`worker.lease_lost`).
   Since Milestone 9 this step is **group-committed**: finished attempts queue in the worker's
   `CompletionBatcher` and one transaction writes up to `WORKER_COMPLETE_BATCH_MAX` of them with a
   single `UPDATE deliveries ... FROM unnest(:ids, :tokens, :statuses, ...)` (each row still matched
   on its own `lease_token`; rows locked in id order), one multi-row `delivery_attempts` insert and
   one breaker update per affected endpoint (endpoint order). A batch is written as soon as it is
   full, as soon as every in-flight attempt is waiting on it (so an idle worker never waits), or
   `WORKER_COMPLETE_BATCH_LINGER_MS` after its oldest entry arrived. Each attempt task gets its own
   result back: the completion, or `None` if its lease was lost (only that row is skipped).
6. **Retry schedule.** `delay = uniform(0, min(DELIVERY_BACKOFF_MAX_S, DELIVERY_BACKOFF_BASE_S ×
   2^(attempt-1)))` (full jitter), raised to the receiver's `Retry-After` (seconds or HTTP date,
   capped at `DELIVERY_RETRY_AFTER_MAX_S`). After `DELIVERY_MAX_ATTEMPTS`, or when the next attempt
   would be later than `created_at + DELIVERY_MAX_AGE_S` (`max_age_exceeded`), the delivery is `failed`.
7. **Leases stay honest.** In-flight leases are renewed every `WORKER_LEASE_RENEW_INTERVAL_S`; a lease
   that could not be renewed cancels its attempt. A reaper (every `WORKER_RECLAIM_INTERVAL_S`) turns
   leases that expired (crashed or stalled worker) into a `lease_expired` attempt and a retry, or
   `failed` if attempts are exhausted.
8. **Shutdown.** `SIGTERM` / `SIGINT` stop leasing, give in-flight attempts `WORKER_DRAIN_TIMEOUT_S`
   to finish, then cancel the rest and release their leases back to `pending` with the attempt
   refunded (nothing was recorded, so it does not count against `DELIVERY_MAX_ATTEMPTS`).
   Attempts that already finished but are still queued for a group commit are written first
   (bounded by the drain budget, at least 1 s): releasing them would redeliver a webhook the
   receiver already acknowledged.

### Back-pressure flow (Milestone 4)

1. **Who may be leased.** One statement locks the `endpoint_states` rows of endpoints with due work
   (`FOR UPDATE OF s SKIP LOCKED`, so two workers never plan for the same endpoint at once) and
   keeps those where in-flight deliveries `< COALESCE(max_in_flight, ENDPOINT_DEFAULT_MAX_IN_FLIGHT)`,
   the breaker is `closed` (or `open` past `open_until` / `half_open` with nothing in flight), and the
   lazily refilled token bucket holds at least one token. Endpoints are ordered by `last_leased_at
   NULLS FIRST`, so a tenant with a deep backlog cannot starve a quiet one.
2. **How many.** A pure `allocate()` shares the worker's free slots round-robin, never above an
   endpoint's limit: `min(free capacity, due rows, floor(tokens))`, and exactly 1 for a probe.
3. **Lease.** `unnest(endpoint_ids, quotas)` + `LATERAL (... FOR UPDATE SKIP LOCKED LIMIT quota)` takes
   each endpoint's oldest due rows (served by `ix_deliveries_endpoint_due`), all in the same
   transaction that then spends the tokens and moves `open → half_open`.
4. **Outcome.** After the fenced completion, the attempt is classified: 2xx and "receiver answered"
   4xx / redirects count as success (the endpoint is up), 5xx / 408 / 429 / timeouts / connection,
   DNS and TLS errors as failure, our own problems (secret unavailable, lease expired) as neutral.
   `BREAKER_FAILURE_THRESHOLD` consecutive failures open the breaker for `BREAKER_COOLDOWN_BASE_S ×
   2^trips` (capped); a half-open probe's success closes it, its failure re-opens it for longer.
5. **Dead letters.** A retryable error with the run's budget spent (attempts since the last replay
   ≥ `DELIVERY_MAX_ATTEMPTS`, or the next try later than `run_started_at + DELIVERY_MAX_AGE_S`) ends
   `dead`; a permanent error ends `failed`.
6. **Auto-disable.** When failures have lasted `ENDPOINT_AUTO_DISABLE_AFTER_S`, the same transaction
   sets `enabled = false, auto_disabled_at = now()` and queues an `endpoint.disabled` notification;
   the relay dispatches it (exponential retry, `abandoned` after `NOTIFICATION_MAX_ATTEMPTS`). The
   backlog waits in `pending`; `PATCH {"enabled": true}` resets the breaker and resumes it.
7. **Replay.** A replay sets finished deliveries back to `pending` (same id, so the same
   `webhook-id`), with `attempt_offset = attempt_count` and `run_started_at = now()` so the new run
   has a full budget while attempt numbers keep increasing. Each request takes a per-tenant
   advisory lock, counts the tenant's `replay_requests` in the last minute (`429` past
   `REPLAY_RATE_LIMIT_PER_MINUTE`), replays at most `REPLAY_MAX_DELIVERIES` (`has_more` says
   whether to call again) and writes its audit row, all in one transaction with the
   `Idempotency-Key` record.

### Operability flows (Milestone 5)

1. **Rotate.** `POST /v1/endpoints/{id}/secret/rotate` (admin, `If-Match` required, body optional:
   `overlap_s`, `secret`) is one conditional `UPDATE ... WHERE version = :expected RETURNING`: the
   current ciphertext moves to `previous_secret_ciphertext` (moved, never decrypted; it stays bound
   to the tenant + endpoint AAD), the new secret is encrypted in the same statement, and
   `previous_secret_expires_at = now() + overlap_s`. The response shows the new secret once
   (`Cache-Control: no-store`) with the new `ETag`.
2. **Dual signing.** The lease returns the previous ciphertext only while it is unexpired; the
   worker signs each attempt with both secrets (`webhook-signature: v1,<new> v1,<old>`), so a
   receiver holding either verifies. The attempt log stores `v1,[redacted]` per signature. If the
   old secret cannot be decrypted the attempt is signed with the new one only (logged).
3. **End the overlap.** When the receiver has deployed the new secret, `DELETE
   .../secret/previous` stops dual signing at once (idempotent, `If-Match` optional); otherwise the
   lease ignores it after `previous_secret_expires_at` and retention clears the ciphertext.
4. **Search.** `GET /v1/deliveries?status=failed&status=dead&event_type=invoice.paid&endpoint_id=...
   &created_after=...&created_before=...` (reader) returns deliveries newest first with the same
   keyset cursor as every list. The tenant predicate is always applied, so another tenant's
   endpoint id simply matches nothing.
5. **Health.** `GET /v1/endpoints/{id}/health?window_s=3600` (reader) aggregates the newest
   `ENDPOINT_HEALTH_MAX_ATTEMPTS` attempts of the window in one statement (`count FILTER`,
   `percentile_cont`), adds the most recent failed attempt (any time), the breaker row and the
   pending backlog (counted up to a cap).
6. **Retention.** For events older than `EVENT_PAYLOAD_RETENTION_DAYS`, a batch takes up to
   `RETENTION_BATCH_SIZE` unpurged events in `(created_at, id)` order with `FOR UPDATE SKIP LOCKED`,
   share-locks their deliveries (waiting for workers and replays that hold them), skips events with
   a delivery still `pending` / `delivering` or not yet fanned out, sets `payload = NULL,
   payload_purged_at = now()` and scrubs stored response bodies of their attempts. Each batch is its
   own short transaction; the cursor moves past skipped rows, the run stops at a short batch, at
   `RETENTION_MAX_BATCHES` (exit code 3 for the CLI) or on `SIGTERM`. Ended secret overlaps are
   cleared the same way. Deliveries and attempts themselves still live until `retain_until`.


### Redis flows (Milestone 6)

1. **Wake-up.** After the relay commits new `pending` deliveries (and after a replay re-queues),
   `WakeupPublisher` triggers a coalesced publish on `{REDIS_KEY_PREFIX}:wake:deliveries`. The
   payload (`source`, `sentAt`) is observational; presence of the message is the signal. Workers
   with `WAKEUP_ENABLED` subscribe on a dedicated connection; on message they set an event that
   short-circuits the idle poll delay. If Redis is down the adaptive Postgres poll continues.
2. **Subscription cache.** Fan-out asks `SubscriptionCache.get_or_load(tenant, event_type)` first.
   Hit → endpoint IDs. Miss → single-flight load via the SQL `pattern_matches_sql` join, then `SET`
   with TTL (short negative TTL for empty). Mutations (subscriptions, endpoint enable/disable/delete,
   event-type archive) call `invalidate_tenant`. Redis errors → SQL path; the insert stays
   `ON CONFLICT DO NOTHING`.
3. **Rate limit.** When `RATE_LIMIT_REDIS_ENABLED` and Redis is reachable, `lease_due` takes tokens
   from the Lua bucket before leasing and still spends Postgres tokens (dual-update). On Redis
   error it falls back to `floor(pg_tokens)` — never unlimited. `max_in_flight` and the breaker stay
   in Postgres.
4. **Readiness.** `/ready` always pings Postgres. When `REDIS_URL` is set it also reports a Redis
   ping; by default (`REDIS_REQUIRED_FOR_READY=false`) Redis down does not 503 the API.

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
  FastAPI gives typed request/response models and OpenAPI docs for free. CPU work (HMAC, JSON) is small
  per request, but it adds up: M8 measured ≈ 2 ms of worker CPU per delivery, so one worker process
  tops out near one core and scales out by processes ([capacity plan](docs/capacity.md)).
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

### Milestone 1 decisions

- **API keys are SHA-256 hashed, not bcrypt/argon2.** Keys carry 256 bits of randomness (43 url-safe
  chars), so brute-forcing the hash is infeasible and a slow KDF would only add latency to every
  request. Passwords need slow hashes because humans choose them; random keys do not. The 12-char
  prefix is stored in clear with a unique index, so lookup is one indexed read and the prefix can be
  shown in UIs and logs to identify a key without revealing it.
- **Rotation = new key + overlap window, not in-place change.** `POST /v1/api-keys/{id}/rotate`
  mints a successor and sets the old key's `expires_at` to `now() + API_KEY_ROTATION_OVERLAP_S` (default 24 h,
  `0` for immediate), never extending an earlier expiry. Callers can roll deployments without downtime. The old row is locked `FOR UPDATE`, and
  a partial unique index on `rotated_from_id` guarantees at most one successor even under concurrent
  rotations (the loser gets `409 api_key_already_rotated`, verified with 5 parallel requests).
- **Endpoint signing secrets are encrypted (AES-256-GCM), not hashed.** Unlike API keys, hookrelay must
  *use* the secret later to sign deliveries, so it must be recoverable. Ciphertexts are
  `v1.<key id>.<nonce+ciphertext>`; the key id lets `SECRETS_ENCRYPTION_KEY_PREVIOUS` keep old rows
  readable during a key rotation, and the AAD binds each ciphertext to `(tenant, endpoint)`, so a
  ciphertext copied onto another row fails to decrypt instead of signing with someone else's secret.
  A CHECK (`secret_ciphertext LIKE 'v1.%'`) stops plaintext from ever being stored.
- **Tenant isolation in two layers.** The service layer filters on `tenant_id` everywhere, and the
  schema makes cross-tenant links impossible: subscriptions reference
  `(tenant_id, endpoint_id)` and `(tenant_id, event_type_id)` through composite foreign keys, so even
  raw SQL cannot subscribe tenant A's endpoint to tenant B's event type.
- **Keyset (cursor) pagination.** `ORDER BY created_at DESC, id DESC` with an opaque base64url cursor of
  `(created_at, id)` and a supporting composite index. Unlike `OFFSET`, page N costs the same as page 1
  and rows inserted during iteration are neither skipped nor duplicated. Malformed cursors give
  `400 invalid_cursor`.
- **Optimistic concurrency with a version column.** `ETag` is the strong tag `"<version>"`. `PATCH`
  requires `If-Match` (`428` if missing, `412` with the current version if stale, `*` to force);
  the check is a single `UPDATE ... WHERE version = $n RETURNING`, so two writers holding the same
  ETag cannot both win (5 concurrent writers → exactly one `200`). Weak ETags are rejected for writes.
- **Soft-deleted endpoints, hard-deleted subscriptions.** Endpoints will be referenced by deliveries
  (M2+), so deletion sets `deleted_at` (a CHECK forces `enabled = false`) and removes its subscriptions;
  a partial unique index `(tenant_id, url) WHERE deleted_at IS NULL` lets the URL be registered again.
- **Wildcards are prefix patterns.** `invoice.*` matches any type starting with `invoice.` (including
  `invoice.paid.partial`), `*` matches everything. Exact patterns must reference a registered,
  non-archived event type (FK + CHECK `wildcard ⇔ event_type_id IS NULL`); wildcards may precede
  the types they will match. Matching is a single indexed SQL query that a property-style test
  checks against the pure-Python reference matcher.
- **Config-time URL validation is not the SSRF defence.** `https` only, no credentials in URLs, no IP
  literals in private / loopback / link-local / metadata ranges, and no `localhost` unless
  `ENDPOINT_ALLOW_*` is set for development. DNS can change after validation, so the delivery worker
  (M3) will resolve, vet and pin the IP on every connection.
- **Secrets never echo.** API keys and signing secrets appear in exactly one response
  (`Cache-Control: no-store`); readers and publishers see custom header values masked as `********`;
  the logging pipeline redacts credential-like keys and never renders traceback locals.

### Milestone 2 decisions

- **Transactional outbox, not "publish after commit".** The event, its outbox row and the stored
  idempotent response commit together, so an accepted event is never lost and a rolled-back one is
  never fanned out. The outbox is a separate narrow table with a partial index on pending rows, so
  the relay's claim query stays fast no matter how large `events` grows, and processed rows are
  purged on their own schedule.
- **Exactly-once fan-out from at-least-once processing.** A relay can crash after inserting deliveries
  but before committing, or a row can be processed twice after an operator resets it. The unique
  `(event_id, endpoint_id)` constraint plus `ON CONFLICT DO NOTHING` makes re-processing harmless; the
  tests run 4 relays concurrently over 60 events × 3 endpoints, crash one mid-batch and replay the
  outbox, and always find exactly 180 deliveries.
- **`SKIP LOCKED` instead of a leader.** Any number of relays can run; they take disjoint batches
  without coordination, and a dead relay's locks vanish with its connection. Ordering across events
  is not guaranteed (it never is with parallel consumers); receivers get `occurred_at` and ids.
- **Subscriptions are evaluated at relay time, not at ingest.** Ingest stays a constant-cost write,
  and a subscription created a second after publishing still catches an event that has not been
  fanned out yet. The trade-off: during a relay backlog, unsubscribing also stops not-yet-fanned-out
  events. Deliveries inherit the event's `retain_until`.
- **Idempotency record = an uncommitted row lock.** Claiming the key is an `INSERT ... ON CONFLICT`
  in the same transaction as the write, so a concurrent duplicate simply blocks in Postgres until the
  first commits (then replays) or rolls back (then proceeds). No separate "in progress" state can be
  left behind by a crash. `lock_timeout` bounds the wait (`409` + `Retry-After`) so a slow request
  cannot pin connections of its retries. Verified with 8 concurrent identical requests → 1 event, 7 replays.
- **Only successes are remembered.** Errors roll back the claim, so a client that fixes a `422`
  (for example, registers the missing event type) can retry with the same key. This follows the
  IETF idempotency-key draft's intent; the trade-off is that a non-deterministic failure is retried
  rather than replayed, which is what clients want for a publish endpoint.
- **Fingerprints use the canonical body.** Key order and whitespace do not change the fingerprint, so
  re-serialising clients still replay; any semantic change (another payload, type or `occurred_at`) is
  a mismatch. Floats are kept as Python floats (IEEE-754 doubles), so numbers beyond double precision
  are normalised; producers needing exact decimals should send them as strings.
- **Limits enforced before work.** The declared `Content-Length` is checked before the body is read, a
  streaming body is cut off at the limit, and JSON depth is bounded, so a hostile request cannot make
  the API buffer megabytes or recurse deeply. The payload limit applies to the canonical form, which
  is exactly what is stored and later signed.
- **Poison rows back off, healthy rows flow.** A set-based batch is fast; when it fails the relay
  falls back to per-row savepoints, parks only the failing row with exponential backoff and records
  the error, so one bad row costs one retry per backoff period rather than a stuck queue.
- **Retention is a column now, partitions later.** `retain_until` is indexed on events and deliveries
  and purged in bounded batches by the relay. When volume calls for it, the same column becomes the
  range-partition key and purging becomes `DROP PARTITION` (M8 capacity plan).
- **Relay health is a heartbeat file.** The relay serves no HTTP, so the container probe
  (`python -m hookrelay.relay --healthcheck`) checks that the loop touched its heartbeat within
  `RELAY_HEARTBEAT_MAX_AGE_S`. A wedged loop is restarted even if the process is alive.

### Milestone 3 decisions

- **Fencing tokens, not "the lease probably did not expire".** A worker can stall (GC pause, network
  partition) past its lease while another worker re-leases the delivery. Every lease mints a new
  `lease_token`, and renew / complete / release all match it, so the stale worker's late write is a
  no-op instead of overwriting the newer attempt's outcome. Tests take a lease over by hand and
  check the old token can neither complete, renew nor release.
- **Attempts are counted when leased.** `attempt_count` increments in the lease statement, so a worker
  that crashes mid-attempt still spends the attempt (the reaper writes a `lease_expired` attempt row);
  a poison delivery that crashes workers cannot loop forever. A *graceful* drain refunds the attempt,
  because nothing reached the attempt log.
- **At-least-once, deduplicated by `webhook-id`.** A worker can crash after the receiver answered
  but before the result commits; the next attempt sends the same `webhook-id` (the event id) and
  a fresh signature. Receivers dedupe on `webhook-id`; hookrelay never claims exactly-once.
- **Lease only what can be sent now.** The lease size is capped by free semaphore slots, so a busy
  worker never holds leases it cannot work on (and that would expire while queued).
- **Paused, not failed.** Deliveries of a disabled or deleted endpoint or a suspended tenant are simply
  not leased; re-enabling resumes them. Their max age still applies when they are next attempted.
- **Retry decisions in Python, timestamps from Postgres.** The policy (classification, full jitter,
  `Retry-After`, max attempts / age) is a pure, unit-tested module; the lease and completion SQL use
  the database clock, so worker clock skew cannot make deliveries early or late.
- **`Retry-After` is a floor with a cap.** A receiver asking for 120 s is never retried sooner, but
  a hostile `Retry-After: 99999999` cannot park a delivery for years (`DELIVERY_RETRY_AFTER_MAX_S`).
- **`failed`, not `dead`, in M3.** `failed` was terminal with `completed_at`; `dead` was reserved for
  the M4 dead-letter queue (which now holds exhausted retries; `failed` means a permanent error).
  The 0007 downgrade maps `failed` back to `dead` so older code still sees a valid state.
- **The SSRF guard lives in the connection, not in a pre-check.** Validating the URL at config time
  (M1) and resolving before the request both lose to DNS rebinding: a name can resolve to a public
  IP for the check and to `169.254.169.254` for the connect. hookrelay's httpcore network backend
  resolves, vets every returned address (a mixed public/private answer is refused) and connects to
  the vetted IP itself, on every new connection. Redirects are never followed and proxies from the
  environment are ignored, so neither can bounce a request inward.
- **Dev escape hatch with a hard stop.** `DELIVERY_ALLOW_PRIVATE_NETWORKS` unlocks loopback, RFC 1918
  and CGNAT ranges for local receivers; configuration refuses it when `APP_ENV=production`, and
  link-local / metadata addresses stay blocked even when it is on.
- **Attempt logs are debuggable but not leaky.** Stored request headers mask custom header values
  (`********`) and redact the signature (`v1,[redacted]`); bodies are cut at `DELIVERY_RESPONSE_MAX_BYTES`
  and stored as valid UTF-8; logs carry ids, status and error class, never URLs, secrets or bodies.
- **Secret decryption failures are retryable.** If `SECRETS_ENCRYPTION_KEY` is misconfigured nothing
  has been sent yet; retrying lets an operator fix the key without losing deliveries.

### Milestone 4 decisions

- **Breaker state in Postgres, not in worker memory.** With N workers an in-memory breaker sees 1/N of
  the failures and each worker probes on its own. `endpoint_states` is one row per endpoint, locked
  `SKIP LOCKED` in the lease and `FOR UPDATE` on completion (lock order: delivery → state → endpoint),
  so every worker sees the same state and exactly one probe runs. The cost is one small row update
  per attempt; Redis (M6) can take the hot counters later without changing the semantics.
- **Limits enforced where work is claimed.** `max_in_flight` and the token bucket are checked inside
  the lease transaction, against `count(*) WHERE status = 'delivering'` (a partial index), so they
  hold across all workers and survive restarts; there is no separate counter that can drift. The
  bucket refills lazily (`tokens + elapsed × rate`, capped at one second's worth) so there is no
  ticker process.
- **Open breakers defer, they do not fail.** While the breaker is open, due deliveries stay `pending`
  and spend no attempts, so a receiver that is down for an hour does not burn its retry budget;
  their max age still applies, measured from the start of the run.
- **Availability vs. correctness signals.** A `400` or `410` means the receiver is up and rejecting
  this message, so it fails the delivery but counts as a *success* for the breaker; otherwise one
  bad payload could open the circuit for every other message. Lease expiry and secret problems are
  ours, so they are neutral.
- **`dead` vs `failed`.** Both are terminal; `dead` (retries exhausted) is what operators replay after
  fixing a receiver, `failed` (permanent 4xx, blocked destination) usually needs a payload or config
  fix first. The 0008 migration reclassifies old rows by their last error and its downgrade maps
  `dead` back to `failed`.
- **Replay reuses the delivery.** A new row would need its own `webhook-id`, breaking receivers'
  dedupe. Reusing it keeps the id and the full attempt history; `attempt_offset` / `run_started_at`
  give the new run a fresh budget. Only terminal deliveries can be replayed (`409` otherwise), and
  deliveries of disabled endpoints are skipped (`409` for a single replay).
- **The audit table is the rate limiter.** Replay rate limits count `replay_requests` rows from the
  last 60 s under a per-tenant transaction-scoped advisory lock, so concurrent API replicas agree
  and an idempotent retry (stored response) costs nothing. A sliding-window count is exact and
  cheap at this volume; M6's Redis token buckets are the step for hot APIs.
- **Bulk replay is bounded.** It needs an explicit, bounded window (`REPLAY_MAX_WINDOW_S`), is
  admin-only, and replays at most `REPLAY_MAX_DELIVERIES` per call with `has_more`, so a click
  cannot rewrite millions of rows in one transaction. Rows already being replayed are skipped
  (`SKIP LOCKED`) rather than waited on.
- **Notifications are an outbox too.** Auto-disable and its notification commit together; the relay
  delivers it later with retries. The default notifier logs a structured `WARNING`; a pager or
  email notifier implements one `send()` method.

### Milestone 5 decisions

- **Dual signing over "accept both" on our side.** The receiver is the verifier, so the only way to
  rotate without dropping messages is to sign with both secrets for a while (Standard Webhooks
  allows several signatures). The window is bounded (`ENDPOINT_SECRET_MAX_OVERLAP_S`, 7 days by
  default) and at most two secrets ever sign: rotating during an overlap replaces the previous one.
- **`If-Match` is required on rotate.** A blindly retried rotate would replace the secret the client
  just received and push the one the receiver still uses out of the overlap. `If-Match: *` opts out
  explicitly; concurrent rotations with one ETag yield exactly one `200` and `412` for the rest.
- **Expiry is enforced at read time, cleanup is lazy.** The lease ignores an expired previous secret,
  so correctness never depends on the cleanup job; retention clears the ciphertext later without a
  version bump (a dead secret signs nothing, so the representation did not change).
- **Denormalised `event_type` and `endpoint_id`, set by triggers.** Search by event type and health per
  endpoint would otherwise join `events` / `deliveries` for every row. Both are immutable, so a
  `BEFORE INSERT` trigger copies them and writers cannot get them wrong; same-tenant composite FKs
  still reject cross-tenant rows. The cost is a PK lookup per insert.
- **One index per filter, not one per combination.** `(tenant_id, status, created_at, id)`,
  `(tenant_id, event_type, created_at, id)` and `(endpoint_id, created_at, id)` each give an ordered
  walk without a sort; combined filters use the most selective one and filter the rest. Covering
  every combination would cost write amplification on the hottest table.
- **Health is computed on read, bounded.** No rollup table to keep consistent: a covering index
  `(endpoint_id, created_at) INCLUDE (outcome, latency_ms, error_class)` makes the aggregate an
  index-only scan, the window is capped (`ENDPOINT_HEALTH_MAX_WINDOW_S`), the sample is capped
  (`ENDPOINT_HEALTH_MAX_ATTEMPTS`, newest first, flagged `sampled`) and the backlog count is capped.
  M7's dashboard rollups are the step for fleet-wide views.
- **Retention never races a delivery.** Events are claimed `SKIP LOCKED` (concurrent runners split the
  work) and their deliveries share-locked through a `MATERIALIZED` CTE, so a worker completing or a
  replay re-queueing a delivery is waited for and then seen. Without the CTE the planner pushes the
  status filter into the locking scan and a `dead` delivery being replayed is neither locked nor
  waited for; a concurrency test caught exactly that. A purged event cannot be replayed (`409
  payload_purged`; bulk replays skip it), and a worker that still meets one fails it
  `payload_purged`: terminal, no request sent, neutral for the breaker.
- **Payload, not history.** Retention drops payloads and response bodies (the potentially sensitive
  part) but keeps events, deliveries and attempt metadata until `retain_until`, so the delivery log
  and health stay accurate. `EVENT_PAYLOAD_RETENTION_DAYS` must be below `EVENT_RETENTION_DAYS`.
- **Bounded batches over one big `UPDATE`.** Short transactions keep row locks and WAL bursts small,
  a pause between batches gives replicas and autovacuum room, and a batch budget makes every run
  finish; the CLI exits `3` when there is more to do so a CronJob can alert or run again.


### Milestone 6 decisions

- **Pub/sub wake-ups over `LISTEN/NOTIFY`.** NOTIFY payloads are capped, queue when no listener, and
  share the Postgres connection pool with lease traffic. Redis pub/sub is a dedicated side channel:
  workers keep one subscribed connection and command connections stay free for leases. Postgres
  adaptive polling remains the correctness fallback, so Redis is never on the durability path.
- **One global wake channel, coalesced.** Per-endpoint channels would multiply publishes with
  endpoint count while the worker still has to ask Postgres which deliveries are due. A leading +
  trailing coalescer collapses a stampede of events into about two publishes per window.
- **Cache-aside with explicit invalidation, never invent matches.** Fan-out membership changes are
  rare relative to ingest; a short TTL plus tenant-wide invalidate on every mutating path is enough.
  Single-flight (process lock + Redis `SET NX`) stops a cold key from hammering Postgres. On any
  Redis miss/error the SQL matcher is authoritative.
- **Redis rate limit with Postgres dual-update, never fail open.** Taking tokens in Redis removes
  hot `endpoint_states` contention from the lease transaction when Redis is healthy. Spending the
  Postgres bucket on the same allow keeps a failover from going unlimited (the degraded path uses
  `floor(pg_tokens)`). The trade-off is slightly higher Postgres contention when Redis is down —
  preferred over silently exceeding the tenant's limit. Breaker and `max_in_flight` stay in Postgres
  because they are correctness across workers, not just soft pacing.
- **Soft `/ready` for Redis.** The API can serve ingest and reads with Redis down; only workers
  degrade. `REDIS_REQUIRED_FOR_READY=true` is available when an operator wants Redis in the
  critical path. Report the check either way so probes and dashboards see it.
- **Package name `hookrelay.redis_`.** A top-level `redis/` package would shadow the official
  `redis` dependency on some import paths; the trailing underscore keeps the layout obvious without
  that footgun.

### Milestone 8 decisions

- **k6 against real processes, not in-process benchmarks.** The harness starts the API, relay,
  workers and the mock receiver exactly as deployed (separate processes, real Postgres and Redis),
  so the numbers include serialisation, connection pools and the OS scheduler. k6 (Go) generates
  constant-arrival-rate load without the coordinated-omission problem of closed-loop Python load
  generators.
- **Correctness is the threshold, speed is reported.** `e2e.js` fails on any lost delivery or any
  delivery acknowledged twice; throughput and lag are recorded, not asserted (except a generous lag
  bound). CI runs a short smoke on every push as a wiring check; shared runners are too noisy for
  capacity numbers, which come from dedicated runs written up in [docs/capacity.md](docs/capacity.md).
- **A purpose-built mock receiver.** Per-endpoint behaviour in the URL query string
  (`?latency_ms=800&failure_rate=0.2`), seeded randomness for repeatable runs, `Retry-After` on
  `429` / `503`, and hangs past the sender's timeout. Its accounting distinguishes *received* from
  *acknowledged* (2xx written to a still-connected client), which is what at-least-once is about.
- **Deterministic chaos in CI.** Faults are injected at exact points (a worker `SIGKILL`-ed while the
  receiver holds its 8 requests, a backend killed between the receiver's 2xx and the completion
  write) instead of random sleeps, so the tests assert exact attempt / duplicate counts and never
  flake on timing. "Postgres restart" in CI is a loop of `pg_terminate_backend` over every session of
  the test database (what clients observe during a restart or failover; service containers cannot
  be restarted mid-job); a real `pg_ctl restart` under load is reproduced locally with
  `CHAOS_CMD` and is in the capacity plan.
- **Retry the completion, not the request.** A Postgres blip right after a receiver's 2xx used to
  mean the lease expired and the message was sent again. The worker now retries the completion
  write on transient DB errors with capped exponential backoff (`WORKER_COMPLETE_RETRIES`). It is
  safe because completion is fenced by the lease token: a retry after a commit whose
  acknowledgement was lost matches nothing and is reported as a lost lease, never applied twice.
- **Classify by SQLSTATE, not by exception class.** The real restart run showed that a restarting
  server refuses new connections with `57P03`, which SQLAlchemy surfaces as a generic `DBAPIError`
  that is not flagged as a disconnect. Connection-level SQLSTATEs (`08xxx`, `57P01-57P03`, `53300`)
  are now transient: the API answers `503 database_unavailable` + `Retry-After` (clients retry with
  the same `Idempotency-Key`), workers retry completions, and the relay abandons the batch for a
  fresh connection instead of running poison-row isolation. Statement-level failures (timeouts,
  constraint violations) are deliberately *not* transient for the relay, so a poisonous row is still
  parked rather than retried forever.
- **Profile, then fix the measured hot spot.** The worker was CPU-bound well below the Little's-law
  limit. httpcore scans all pooled connections on every request, so the transport now shards its
  pool (≤ 16 connections per shard, least-busy shard): 120.7 → 472.0 deliveries/s for one 128-slot
  worker on the benchmark box. `sniffio` is declared explicitly because its absence turned
  httpcore's async-backend probe into a failed import per pool operation. A short refill linger
  batches lease transactions at saturation; it measured within noise and is kept as a cheap bound
  on lease-transaction rate (`WORKER_REFILL_LINGER_MS=0` disables it).
- **Honest numbers.** Every figure in the capacity plan cites the run it came from; the shared-box
  caveat is stated up front, an outlier run is shown and explained rather than dropped silently, and
  a step whose load generator was killed is reported as "no result".

### Milestone 9a decisions (batched completions, PgBouncer)

- **Group commit, not a write-behind cache.** M8 showed one completion transaction per delivery
  (fenced `UPDATE`, attempt `INSERT`, breaker `UPDATE`, `COMMIT` = a WAL flush and a pool checkout)
  as the per-delivery database cost. The worker now hands finished attempts to a small batcher and
  each attempt task *awaits its own result*, so nothing is acknowledged before it is durable and a
  crash loses only attempts that were never recorded (redelivered, as with at-least-once before).
  The trade-off is up to `WORKER_COMPLETE_BATCH_LINGER_MS` (5 ms) of extra latency per completion
  under load, and none when idle: the batch flushes as soon as every in-flight attempt is waiting.
- **Fencing stays per row.** The batch statement joins `unnest(ids, tokens, ...)` on
  `(id, lease_token)` and returns `(id, token)` for the rows it updated, so a lost lease in the
  middle of a batch yields `None` for that attempt only (no attempt row, no breaker signal), a stale
  attempt of a re-leased delivery cannot land next to the current one, and a duplicated
  `(delivery, token)` is completed once. Re-running a batch after an ambiguous commit is safe for the
  same reason: rows that already committed no longer match.
- **Partial failure is isolated.** A transient error (`08xxx`, `57P0x`, pool timeout) retries the
  whole batch with backoff; any other error means one row is poisonous, so the batch is re-written
  one attempt per transaction and only the culprit fails (its lease expires into the reaper).
- **Deadlock-free by lock ordering.** Batched completions and lease renewals touch overlapping rows
  of the same worker; both lock `deliveries` in id order, and breaker rows are updated in endpoint
  order, so concurrent batches, renewals and the reaper cannot form a cycle (tested with three
  workers renewing every 50 ms). Consecutive successes on one endpoint collapse into one breaker
  update; failures are applied in arrival order.
- **`WORKER_COMPLETE_BATCH_MAX=1` is the old path.** Kept as an escape hatch and as the benchmark
  baseline.
- **PgBouncer in transaction mode, and what it costs.** A server connection belongs to a client
  for one transaction, so N processes × pool size client connections share `default_pool_size`
  server connections. That rules out session state: asyncpg's statement caches are disabled and
  statements get unique names (`__asyncpg_<uuid>__`) instead of the per-connection
  `__asyncpg_stmt_1__` that collides across clients; startup parameters PgBouncer does not track
  (`statement_timeout`, `idle_in_transaction_session_timeout`) are refused, so they move to the
  pooler's `connect_query` (asyncpg's `command_timeout` still bounds every query client-side);
  and a unit test audits the code for session-level features (`LISTEN`, session advisory locks,
  session `SET`, temp tables). Migrations need session state, so Alembic uses
  `DATABASE_DIRECT_URL`. The integration suite proves the default asyncpg settings break behind a
  real PgBouncer and that `DB_PGBOUNCER=true` survives many clients on few server connections.
  The cost is a re-prepare per statement (no cache) and an extra hop; in exchange Postgres
  connection count stops scaling with replicas.

### Milestone 9b decisions (Helm, probes, autoscaling)

- **Migrations as a Helm hook Job, not an init container.** An init container runs once per pod:
  ten replicas race to migrate on every rollout, every pod restart re-runs it, and a failure shows
  up as crash-looping pods while old ones are already being replaced. A `pre-install,pre-upgrade`
  Job runs once per release, and a failure stops `helm upgrade` before any Deployment changes.
  The catch is that pre-install hooks run before the chart's own resources exist, so the Job
  inlines its settings, uses a hook-scoped ServiceAccount and, in dev / CI, a hook-scoped copy of
  only the secrets Alembic needs (production points it at `existingSecret`). It always connects
  through `DATABASE_DIRECT_URL` (Alembic's session `SET lock_timeout` must not go through
  transaction pooling), and each revision is its own transaction with a 5 s lock timeout, so the
  Job's `backoffLimit` retry resumes cleanly. Because old pods run against the new schema during
  the rollout, migrations must be expand / contract (additive first, destructive a release later).
  GitOps tools that ignore Helm hooks (Argo CD maps them to PreSync) keep the same ordering.
- **KEDA on queue demand for workers, HPA on CPU for the API.** A worker waiting on slow receivers
  is nearly idle on CPU while the backlog grows, so CPU would scale workers the wrong way. The
  ScaledObject asks Prometheus for `max(hookrelay_deliveries_due) + max(hookrelay_deliveries_delivering)`
  and targets 75% of `WORKER_CONCURRENCY` per pod. `hookrelay_deliveries_due` is new in 9b:
  `hookrelay_deliveries_pending` also counts retries scheduled hours ahead and backlogs behind an
  open breaker, which no number of workers can deliver sooner, so scaling on it would pin a dead
  endpoint's retry queue at `maxReplicaCount`. `max()` (not `sum()`) because every API / relay
  replica reports the same database-wide count. `ignoreNullValues: false` plus `fallback.replicas`
  means "Prometheus is down" holds a fixed, safe size instead of reading as zero demand. The API is
  stateless and CPU-bound per request, so a plain `autoscaling/v2` HPA fits; when either
  autoscaler is on, the Deployment omits `replicas` so `helm upgrade` does not fight it.
- **Grace periods are derived from the code's shutdown paths and enforced.** API: `preStop` sleeps
  5 s (endpoint removal is asynchronous, so the pod keeps serving until kube-proxy / ingress stop
  routing), then uvicorn gets `API_GRACEFUL_SHUTDOWN_S` (20 s, now a setting) for in-flight
  requests; `terminationGracePeriodSeconds` must exceed the sum (40 s by default). Worker:
  `SIGTERM` stops leasing, in-flight attempts get `WORKER_DRAIN_TIMEOUT_S` (25 s), queued
  completions are flushed and the rest of the leases released; the grace must be at least
  drain + 10 s (45 s). The chart fails rendering if either budget is violated. A SIGKILLed worker
  still loses nothing (its leases expire and are reclaimed), but its deliveries are retried late
  and can be sent twice, which is what the budget avoids.
- **PDBs with `maxUnavailable: 1`, not `minAvailable`.** It works for any replica count (a
  `minAvailable: 1` PDB on a single replica blocks node drains forever) while keeping voluntary
  evictions one pod at a time per role; `unhealthyPodEvictionPolicy: AlwaysAllow` lets a
  crash-looping pod be evicted even when the budget is spent. Topology spread across nodes and
  zones is soft (`ScheduleAnyway`) so a small cluster still schedules.
- **Cheap, dependency-free probes.** Liveness never checks dependencies (`/health` for the API, the
  heartbeat file for relay / worker, which they touch even on iterations whose database work
  failed), so a Postgres outage takes API pods out of the Service via `/ready` but restarts
  nothing. The original `python -m hookrelay.worker --healthcheck` imported the whole worker
  (~0.7 s CPU per probe, measured); exec probes run every 10-15 s in every pod, so
  `python -m hookrelay.healthcheck --heartbeat worker` now reads the two env vars with the stdlib
  only (~0.06 s), with a unit test pinning its defaults to `Settings`.
- **Secrets never in the ConfigMap or the image.** Pods get secrets with `envFrom: secretRef`, so
  the Secret's keys are the env var names and any secret manager that writes a Secret works.
  `secrets.values` (chart-managed Secret) exists only for dev / CI and NOTES warns about it.
  Rendering fails if a known secret, a credential-looking key or a chart-owned setting (ports,
  drain / shutdown budgets) is put in `config`, and `values.schema.json` rejects unknown keys.
- **Chart facts pinned to the code.** A pytest renders the chart and checks that every env var
  name is a real `Settings` field (Settings ignores unknown variables, so a typo would silently do
  nothing), that commands are real console scripts, that KEDA queries only use gauges the code
  exports, and the hook, probe, hardening and budget invariants.

## Getting started

### Prerequisites

- Python 3.12+ and [uv](https://docs.astral.sh/uv/) (0.13.x)
- Postgres 16 (or use Docker Compose)
- Redis 7 (optional locally; required for M6 integration tests / compose when exercising wake / cache / RL)
- [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) (optional; only for `loadtest/`)

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

`make test-chaos` runs only the chaos suite; `make loadtest-smoke` runs the CI k6 smoke against a
local stack (needs [k6](https://grafana.com/docs/k6/latest/set-up/install-k6/) and a dedicated
`DATABASE_URL`).

Integration tests need `TEST_DATABASE_URL` pointing at a role that may `CREATE DATABASE`. Each run
creates and drops its own scratch databases, so your data is never touched. Without the variable,
integration tests are skipped and unit tests still run.

### First tenant in 60 seconds

```bash
export OPERATOR_TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(32))")
uv run hookrelay &

# 1. Operator creates a tenant and bootstraps its first admin key (the key is shown once)
TENANT=$(curl -s -X POST localhost:8000/admin/tenants -H "Authorization: Bearer $OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' -d '{"slug": "acme", "name": "Acme Corp"}' | jq -r .id)
KEY=$(curl -s -X POST localhost:8000/admin/tenants/$TENANT/api-keys \
  -H "Authorization: Bearer $OPERATOR_TOKEN" -H 'Content-Type: application/json' \
  -d '{"name": "bootstrap", "role": "admin"}' | jq -r .key)

# 2. The tenant registers an event type, an endpoint and a subscription
curl -s -X POST localhost:8000/v1/event-types -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' -d '{"name": "invoice.paid"}'
EP=$(curl -s -X POST localhost:8000/v1/endpoints -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' -d '{"url": "https://example.com/webhooks"}' | jq -r .id)
curl -s -X POST localhost:8000/v1/endpoints/$EP/subscriptions -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' -d '{"event_type": "invoice.*"}'
curl -s "localhost:8000/v1/subscriptions/match?event_type=invoice.paid" -H "Authorization: Bearer $KEY"

# 3. Publish an event (safe to retry with the same Idempotency-Key) and fan it out
EVENT=$(curl -s -X POST localhost:8000/v1/events -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: inv_1042-paid' \
  -d '{"event_type": "invoice.paid", "payload": {"invoice_id": "inv_1042", "amount": 4200}}' | jq -r .id)
uv run hookrelay-relay --once     # or run `uv run hookrelay-relay` continuously
curl -s localhost:8000/v1/events/$EVENT/deliveries -H "Authorization: Bearer $KEY"

# 4. Deliver it (signed POST to the endpoint) and inspect the attempt log
uv run hookrelay-worker --once    # or run `uv run hookrelay-worker` continuously
DELIVERY=$(curl -s localhost:8000/v1/events/$EVENT/deliveries -H "Authorization: Bearer $KEY" | jq -r '.data[0].id')
curl -s localhost:8000/v1/deliveries/$DELIVERY -H "Authorization: Bearer $KEY"
curl -s localhost:8000/v1/deliveries/$DELIVERY/attempts -H "Authorization: Bearer $KEY"
```

To deliver to a receiver on your machine (for example `http://127.0.0.1:9000/hook`), run the API
with `ENDPOINT_ALLOW_HTTP=true ENDPOINT_ALLOW_PRIVATE_NETWORKS=true` and the worker with
`ENDPOINT_ALLOW_HTTP=true DELIVERY_ALLOW_PRIVATE_NETWORKS=true` and the port in
`DELIVERY_ALLOWED_PORTS`; production refuses the private-network switch.

### Docker Compose

```bash
docker compose up --build   # postgres:16 + pgbouncer + redis:7 + one-shot migrate job + api on :8000 + relay + worker
curl -i localhost:8000/ready
docker compose down -v
```

Compose ships local-only defaults for `OPERATOR_TOKEN` and `SECRETS_ENCRYPTION_KEY` (export your own
values to override them). The `migrate` service runs `alembic upgrade head` and exits. `api` starts only after it completes
successfully. The image runs as UID 10001 and has a stdlib-only `HEALTHCHECK`. The `relay` service
reuses the same image with `python -m hookrelay.relay` and overrides the health check with the
relay's heartbeat probe; scale it with `docker compose up --scale relay=3`. The `worker` service
runs `python -m hookrelay.worker` the same way (heartbeat health check, `stop_grace_period: 30s`
above the 25 s drain) and scales with `--scale worker=4`. Compose runs in production mode, so the
worker only delivers to public addresses.

Since Milestone 9 the API, relay, worker and retention job reach Postgres through the `pgbouncer`
service (`edoburu/pgbouncer`, transaction pooling, config in `deploy/pgbouncer/pgbouncer.ini`,
`DB_PGBOUNCER=true`), while `migrate` connects straight to Postgres through `DATABASE_DIRECT_URL`.
Scaling `api` / `worker` replicas therefore adds client connections to PgBouncer, not server
connections to Postgres. To bypass the pooler, run with
`HOOKRELAY_DATABASE_URL=postgresql+asyncpg://hookrelay:hookrelay@postgres:5432/hookrelay DB_PGBOUNCER=false`.

### Kubernetes (Helm)

The chart lives in [`deploy/helm/hookrelay`](deploy/helm/hookrelay) (Helm 3.x or 4.x, Kubernetes
1.27+). Build and push the image yourself (no public image is published), create the Secret, then
install:

```bash
docker build -t registry.example.com/hookrelay:0.1.0 . && docker push registry.example.com/hookrelay:0.1.0

kubectl create namespace hookrelay
kubectl -n hookrelay create secret generic hookrelay \
  --from-literal=DATABASE_URL='postgresql+asyncpg://hookrelay:<password>@pgbouncer:6432/hookrelay' \
  --from-literal=DATABASE_DIRECT_URL='postgresql+asyncpg://hookrelay:<password>@postgres:5432/hookrelay' \
  --from-literal=REDIS_URL='redis://redis:6379/0' \
  --from-literal=OPERATOR_TOKEN="$(python -c 'import secrets; print(secrets.token_urlsafe(32))')" \
  --from-literal=SECRETS_ENCRYPTION_KEY="$(python -c 'import base64, secrets; print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())')"

helm upgrade --install hookrelay deploy/helm/hookrelay -n hookrelay \
  --set image.repository=registry.example.com/hookrelay --set image.tag=0.1.0 \
  --set secrets.existingSecret=hookrelay --set-string config.DB_PGBOUNCER=true \
  --set api.autoscaling.enabled=true \
  --set worker.autoscaling.enabled=true --set metrics.serviceMonitor.enabled=true \
  --wait
helm test hookrelay -n hookrelay
```

- The Secret must exist before `helm install`: the migration hook runs before any chart resource.
- `config` holds non-secret settings (strings, see `.env.example`); role settings live under `api`,
  `relay`, `worker` (`api.gracefulShutdownSeconds`, `worker.drainTimeoutSeconds`,
  `worker.concurrency`, `*.terminationGracePeriodSeconds`, `*.metricsPort`, probes, resources,
  PDBs). `values.schema.json` documents and validates every key.
- `worker.autoscaling.enabled` needs [KEDA](https://keda.sh) and a Prometheus that scrapes the
  release (`metrics.serviceMonitor.enabled` for the Prometheus Operator); override the query with
  `worker.autoscaling.prometheus.query` if your labels differ (the default assumes one release per
  namespace). `api.autoscaling.enabled` needs metrics-server.
- `deploy/helm/ci/validate.sh` runs the same lint / template / kubeconform / guard-rail checks as
  CI; `deploy/helm/ci/kind-smoke.sh` repeats the kind smoke on any kind cluster
  (`docker build -t hookrelay:kind . && kind load docker-image hookrelay:kind`).

## Configuration

All settings are environment variables (or `.env`), validated at startup. See
[`.env.example`](.env.example) for the full list. Highlights:

| Variable | Default | Notes |
| --- | --- | --- |
| `APP_ENV` | `development` | `production` enforces JSON logs and forbids `DEBUG` / `DB_ECHO` |
| `DATABASE_URL` | `postgresql+asyncpg://hookrelay:hookrelay@localhost:5432/hookrelay` | `postgres://` and `postgresql://` are normalised to asyncpg; other databases are rejected |
| `DB_POOL_SIZE` / `DB_MAX_OVERFLOW` | `10` / `5` | per process |
| `DB_STATEMENT_TIMEOUT_MS` | `15000` | applied server-side to every connection (behind PgBouncer: set it in the pooler's `connect_query`; still enforced client-side) |
| `DB_PGBOUNCER` | `false` | `true` when `DATABASE_URL` points at PgBouncer in transaction mode: no statement caches, unique prepared-statement names, no untracked startup parameters |
| `DATABASE_DIRECT_URL` | unset | Postgres itself, bypassing the pooler, for Alembic; required when `DB_PGBOUNCER=true` |
| `READINESS_TIMEOUT_S` | `2` | `/ready` gives up and returns 503 after this |
| `API_GRACEFUL_SHUTDOWN_S` | `20` | uvicorn's graceful shutdown after `SIGTERM` (Kubernetes: keep `terminationGracePeriodSeconds` above `preStop` + this; the Helm chart enforces it) |
| `LOG_FORMAT` / `LOG_LEVEL` | `json` / `INFO` | `console` for local readability |
| `REQUEST_ID_HEADER` | `X-Request-ID` | correlation header accepted and returned |
| `REDIS_URL` | unset | `redis://` or `rediss://`; unset → every Redis feature off (Postgres paths unchanged) |
| `REDIS_CONNECT_TIMEOUT_MS` / `REDIS_COMMAND_TIMEOUT_MS` | `1000` / `500` | socket connect / per-command timeout |
| `REDIS_RECONNECT_BASE_MS` / `REDIS_RECONNECT_MAX_MS` | `100` / `5000` | equal-jitter reconnect backoff (base ≤ max) |
| `REDIS_KEY_PREFIX` | `hookrelay` | prefix for wake / cache / rate-limit keys (`{prefix}:wake:deliveries`, …) |
| `WAKEUP_ENABLED` | unset | default on when `REDIS_URL` set; worker pub/sub wake-ups |
| `WAKEUP_COALESCE_MS` | `50` | leading + trailing publish coalesce window |
| `SUBSCRIPTION_CACHE_ENABLED` | unset | default on when `REDIS_URL` set |
| `SUBSCRIPTION_CACHE_TTL_S` | `60` | positive-entry TTL; empty results use a shorter negative TTL |
| `RATE_LIMIT_REDIS_ENABLED` | unset | default on when `REDIS_URL` set; Lua bucket with Postgres fallback |
| `REDIS_REQUIRED_FOR_READY` | `false` | when true, Redis down makes `/ready` 503 |
| `OPERATOR_TOKEN` | unset | ≥ 32 chars; enables `/admin`. Unset → every `/admin` call gets `403 operator_api_disabled` |
| `SECRETS_ENCRYPTION_KEY` | unset | base64 of 32 bytes (AES-256-GCM) for endpoint signing secrets. **Required in production**; development falls back to a fixed insecure key |
| `SECRETS_ENCRYPTION_KEY_PREVIOUS` | unset | old key during a key rotation (decrypt only) |
| `API_KEY_ROTATION_OVERLAP_S` | `86400` | default overlap during which a rotated key still works (0 to 604800) |
| `API_KEY_LAST_USED_RESOLUTION_S` | `60` | minimum seconds between `last_used_at` writes per key |
| `ENDPOINT_ALLOW_HTTP` / `ENDPOINT_ALLOW_PRIVATE_NETWORKS` | `false` / `false` | development escape hatches for `http://` and localhost / private targets |
| `ENDPOINT_DEFAULT_TIMEOUT_MS` / `ENDPOINT_MAX_TIMEOUT_MS` | `10000` / `30000` | per-endpoint delivery timeout default and cap (1000 to 60000) |
| `EVENT_MAX_PAYLOAD_BYTES` | `262144` | max canonical JSON size of `payload`; the request may be 16 KiB larger for the envelope |
| `EVENT_OCCURRED_AT_MAX_FUTURE_S` | `300` | allowed clock skew for a client-supplied `occurred_at` |
| `EVENT_RETENTION_DAYS` | `30` | events and their deliveries are purged after this |
| `IDEMPOTENCY_KEY_TTL_S` | `86400` | how long an `Idempotency-Key` is remembered (60 to 604800) |
| `IDEMPOTENCY_LOCK_TIMEOUT_MS` | `2000` | wait for a concurrent request with the same key before `409` + `Retry-After` |
| `RELAY_BATCH_SIZE` | `100` | outbox rows claimed per relay transaction |
| `RELAY_POLL_MIN_MS` / `RELAY_POLL_MAX_MS` | `100` / `2000` | idle polling backoff bounds (min ≤ max enforced) |
| `RELAY_RETRY_MAX_S` | `300` | cap of the exponential backoff for a failing outbox row |
| `RELAY_HOUSEKEEPING_INTERVAL_S` / `RELAY_OUTBOX_RETENTION_S` | `60` / `3600` | purge cadence; how long processed outbox rows are kept |
| `RELAY_HEARTBEAT_FILE` / `RELAY_HEARTBEAT_MAX_AGE_S` | `/tmp/hookrelay-relay.heartbeat` / `60` | liveness file for `--healthcheck` |
| `WORKER_ID` | `host:pid:random` | recorded on leases and attempt rows (1 to 128 printable chars) |
| `WORKER_CONCURRENCY` / `WORKER_BATCH_SIZE` | `16` / `50` | max in-flight requests per worker; max leases per poll (never more than free slots) |
| `WORKER_POLL_MIN_MS` / `WORKER_POLL_MAX_MS` | `100` / `2000` | idle polling backoff bounds (min ≤ max enforced) |
| `WORKER_LEASE_S` / `WORKER_LEASE_RENEW_INTERVAL_S` | `60` / `15` | lease length and renewal cadence (renewal ≤ half the lease, enforced) |
| `WORKER_RECLAIM_INTERVAL_S` | `10` | how often expired leases are reaped |
| `WORKER_DRAIN_TIMEOUT_S` | `25` | `SIGTERM` grace for in-flight attempts before leases are released |
| `WORKER_REFILL_LINGER_MS` | `20` | when saturated, wait up to this long for a quarter of the slots to free before leasing again (0 = lease per freed slot) |
| `WORKER_COMPLETE_RETRIES` | `6` | retries of a completion write after a transient DB error (restart, failover, pool timeout); fenced by the lease token, so idempotent. 0 = leave it to lease expiry + reaper |
| `WORKER_COMPLETE_RETRY_BASE_MS` / `WORKER_COMPLETE_RETRY_MAX_MS` | `100` / `2000` | exponential backoff of those retries (base ≤ max enforced) |
| `WORKER_COMPLETE_BATCH_MAX` / `WORKER_COMPLETE_BATCH_LINGER_MS` | `32` / `5` | group commit: finished attempts per completion transaction (1 to 500; `1` = one transaction per attempt) / the most extra wait for a batch to fill (0 to 1000) |
| `WORKER_HEARTBEAT_FILE` / `WORKER_HEARTBEAT_MAX_AGE_S` | `/tmp/hookrelay-worker.heartbeat` / `60` | liveness file for `--healthcheck` |
| `DELIVERY_CONNECT_TIMEOUT_MS` | `5000` | TCP + TLS handshake deadline (the endpoint's `timeout_ms` bounds the whole request) |
| `DELIVERY_RESPONSE_MAX_BYTES` | `4096` | response bytes read and kept in the attempt log (0 to 65536) |
| `DELIVERY_MAX_ATTEMPTS` / `DELIVERY_MAX_AGE_S` | `8` / `259200` | per run (a replay starts a new run); then the delivery is `dead` |
| `DELIVERY_BACKOFF_BASE_S` / `DELIVERY_BACKOFF_MAX_S` | `5` / `3600` | full-jitter exponential backoff (base ≤ max enforced) |
| `DELIVERY_RETRY_AFTER_MAX_S` | `3600` | cap on a receiver's `Retry-After` |
| `DELIVERY_ALLOWED_PORTS` | `80,443,8080,8443` | SSRF guard port allow-list |
| `DELIVERY_ALLOW_PRIVATE_NETWORKS` | `false` | loopback / RFC 1918 / CGNAT receivers for development; **refused in production** |
| `ENDPOINT_DEFAULT_MAX_IN_FLIGHT` / `ENDPOINT_MAX_IN_FLIGHT` | `10` / `100` | in-flight attempts per endpoint across all workers when unset / the most a tenant may set (default ≤ max enforced) |
| `ENDPOINT_MAX_RATE_LIMIT_PER_S` | `1000` | highest `rate_limit_per_s` a tenant may set (unset = unlimited) |
| `BREAKER_FAILURE_THRESHOLD` | `5` | consecutive availability failures that open an endpoint's breaker |
| `BREAKER_COOLDOWN_BASE_S` / `BREAKER_COOLDOWN_MAX_S` | `30` / `600` | open duration `base × 2^trips`, capped (base ≤ max enforced) |
| `ENDPOINT_AUTO_DISABLE_AFTER_S` | `259200` | failing this long → endpoint disabled + `endpoint.disabled` notification (`0` = never) |
| `REPLAY_RATE_LIMIT_PER_MINUTE` | `30` | replay requests per tenant per minute (`429` + `Retry-After` beyond) |
| `REPLAY_MAX_DELIVERIES` / `REPLAY_MAX_WINDOW_S` | `1000` / `604800` | deliveries per replay request (`has_more`) / widest bulk window |
| `NOTIFICATION_POLL_INTERVAL_S` / `NOTIFICATION_MAX_ATTEMPTS` | `5` / `10` | relay dispatch cadence / attempts before a notification is `abandoned` |
| `NOTIFICATION_RETENTION_S` | `604800` | dispatched / abandoned notifications and replay audit rows are purged after this |
| `ENDPOINT_SECRET_ROTATION_OVERLAP_S` / `ENDPOINT_SECRET_MAX_OVERLAP_S` | `86400` / `604800` | default dual-signing overlap after a secret rotation / the most a tenant may ask for (default ≤ max enforced) |
| `ENDPOINT_HEALTH_DEFAULT_WINDOW_S` / `ENDPOINT_HEALTH_MAX_WINDOW_S` | `86400` / `604800` | health summary look-back default / cap (default ≤ max enforced) |
| `ENDPOINT_HEALTH_MAX_ATTEMPTS` | `10000` | newest attempts aggregated per health request (`sampled: true` beyond) |
| `EVENT_PAYLOAD_RETENTION_DAYS` | `0` | drop event payloads (and stored response bodies) this long after the event once every delivery is settled; must be `< EVENT_RETENTION_DAYS`; `0` = keep until the event expires |
| `RETENTION_BATCH_SIZE` / `RETENTION_MAX_BATCHES` / `RETENTION_BATCH_PAUSE_MS` | `500` / `200` / `50` | rows per retention transaction / batches per task per run / pause between batches |

### Observability (Milestone 7)

| Variable | Default | Notes |
|---|---|---|
| `METRICS_ENABLED` | `true` | Serve `GET /metrics` (Prometheus text exposition). |
| `METRICS_TOKEN` | unset | Optional bearer token for `/metrics`. Unset = scrapeable without auth (private scrape network). Never exposes tenant payloads. |
| `METRICS_PORT` | unset | Side-car scrape HTTP port for relay/worker (`0`/unset = off). API always uses `/metrics` on its main port. |

Queue gauges are refreshed from Postgres by the relay (every housekeeping interval) and by the API
on every `/metrics` scrape: `hookrelay_outbox_pending`, `hookrelay_outbox_lag_seconds`,
`hookrelay_deliveries_pending`, `hookrelay_deliveries_delivering` and (M9b) `hookrelay_deliveries_due`,
the pending deliveries a worker could lease now (due, endpoint breaker not cooling down), which
is the worker autoscaling signal.
| `OTEL_EXPORTER_OTLP_ENDPOINT` / `TRACING_OTLP_ENDPOINT` | unset | OTLP HTTP base or `/v1/traces` URL. Tracing is **off** when unset. |
| `TRACING_ENABLED` | auto | Force on/off; default on when an OTLP endpoint is set. |
| `TRACING_SAMPLE_RATIO` | `1` | Parent-based trace-id ratio sampler. |
| `DASHBOARD_DEFAULT_WINDOW_S` / `DASHBOARD_MAX_WINDOW_S` | `3600` / `604800` | `GET /v1/dashboard` look-back window bounds. |
| `DASHBOARD_MAX_ATTEMPTS` | `50000` | Cap on attempts sampled per dashboard request. |

Compose also starts Prometheus (`:9090`), Grafana (`:3000`, admin/`EXAMPLE-local-only-not-for-prod`) and Jaeger all-in-one (`:16686`, OTLP `:4318`). Scrape config and a starter dashboard live under `deploy/`.

## API

Interactive docs: `/docs` (Swagger UI), `/redoc`, schema at `/openapi.json` (disable with
`DOCS_ENABLED=false`).

| Method | Path | Purpose | Responses |
| --- | --- | --- | --- |
| `GET` | `/health` | Liveness: process is up (no dependency checks) | `200 {"status":"ok","service","version"}` |
| `GET` | `/ready` | Readiness: Postgres reachable within `READINESS_TIMEOUT_S` and not draining | `200` / `503` with per-check status and latency |
| `GET` | `/metrics` | Prometheus scrape (unauthenticated by default; optional `METRICS_TOKEN` bearer). Never includes tenant payloads. | `200` text exposition / `401` / `404` when disabled |

**Operator API** (`Authorization: Bearer $OPERATOR_TOKEN`):

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/admin/tenants` | Create a tenant (`409 tenant_slug_taken`) |
| `GET` | `/admin/tenants` | List tenants (cursor pagination, `?status=` filter) |
| `GET` | `/admin/tenants/{tenant_id}` | Get a tenant |
| `POST` | `/admin/tenants/{tenant_id}/suspend` / `resume` | Suspend (all its keys get `403 tenant_suspended`) / reactivate; idempotent |
| `POST` | `/admin/tenants/{tenant_id}/api-keys` | Issue a key for a tenant (bootstraps the first admin key) |

**Tenant API** (`Authorization: Bearer hrk_...`; minimum role in the last column):

| Method | Path | Purpose | Role |
| --- | --- | --- | --- |
| `GET` | `/v1/me` | The calling key and its tenant | reader |
| `GET` / `POST` | `/v1/api-keys` | List / create keys (secret shown once) | admin |
| `GET` | `/v1/api-keys/{key_id}` | Get a key (never the secret) | admin |
| `POST` | `/v1/api-keys/{key_id}/rotate` | Mint a successor; old key works for `API_KEY_ROTATION_OVERLAP_S` | admin |
| `POST` | `/v1/api-keys/{key_id}/revoke` | Revoke now (idempotent) | admin |
| `GET` | `/v1/endpoints` | List endpoints (`?enabled=`), header values masked below admin | reader |
| `POST` | `/v1/endpoints` | Create; returns `signing_secret` once, `Location`, `ETag` | admin |
| `GET` | `/v1/endpoints/{endpoint_id}` | Get (`ETag` header) | reader |
| `PATCH` | `/v1/endpoints/{endpoint_id}` | Partial update, **requires `If-Match`**; incl. `max_in_flight` / `rate_limit_per_s` (`null` = default / unlimited); re-enabling an auto-disabled endpoint resets its breaker | admin |
| `DELETE` | `/v1/endpoints/{endpoint_id}` | Soft delete + remove subscriptions (`If-Match` optional) | admin |
| `GET` | `/v1/endpoints/{endpoint_id}/secret` | Reveal the signing secret (audited, `no-store`) | admin |
| `POST` | `/v1/endpoints/{endpoint_id}/secret/rotate` | Rotate the signing secret, **requires `If-Match`**; body optional: `overlap_s` (dual signing, `0` = cutover), `secret` (bring your own `whsec_`); returns the new secret once, `previous_secret_expires_at`, `ETag` | admin |
| `DELETE` | `/v1/endpoints/{endpoint_id}/secret/previous` | End the overlap now: stop signing with the previous secret (idempotent, `If-Match` optional) | admin |
| `GET` | `/v1/endpoints/{endpoint_id}/health` | Health over `?window_s=` (default `ENDPOINT_HEALTH_DEFAULT_WINDOW_S`): attempts by outcome, `success_rate`, p50 / p95 / p99 latency, `errors_by_class`, `last_error`, `last_success_at`, breaker, backlog | reader |
| `GET` | `/v1/dashboard` | Tenant dashboard over `?window_s=`: throughput/min, success rate, latency percentiles, backlog, top endpoints | reader |
| `GET` / `POST` | `/v1/event-types` | List (`?include_archived=`) / register | reader / publisher |
| `GET` / `PATCH` | `/v1/event-types/{name}` | Get / update description or archive (`If-Match`) | reader / publisher |
| `GET` / `POST` | `/v1/endpoints/{endpoint_id}/subscriptions` | List / subscribe to `invoice.paid`, `invoice.*` or `*` | reader / admin |
| `DELETE` | `/v1/endpoints/{endpoint_id}/subscriptions/{subscription_id}` | Unsubscribe | admin |
| `GET` | `/v1/subscriptions/match?event_type=` | Fan-out preview: enabled endpoints + matched patterns | reader |
| `POST` | `/v1/events` | Publish an event (`202`, `Location`); optional `Idempotency-Key` header | publisher |
| `GET` | `/v1/events` | List events, newest first (`?event_type=`, `?created_after=`, `?created_before=`), without payloads | reader |
| `GET` | `/v1/events/{event_id}` | Get an event with its payload and `payload_size` | reader |
| `GET` | `/v1/events/{event_id}/deliveries` | Deliveries created for the event (status, attempts, next attempt, last result) | reader |
| `GET` | `/v1/deliveries` | Search the delivery log, newest first: `?status=` (repeatable), `?event_type=`, `?endpoint_id=`, `?created_after=` (inclusive), `?created_before=` (exclusive); keyset cursor | reader |
| `GET` | `/v1/deliveries/{delivery_id}` | One delivery: status, `attempt_count`, `next_attempt_at`, `last_status_code`, `last_error_class` | reader |
| `GET` | `/v1/deliveries/{delivery_id}/attempts` | Attempt log, newest first: outcome, status code, error class, latency, masked request headers, truncated response | reader |
| `GET` | `/v1/endpoints/{endpoint_id}/circuit` | Breaker state, failure streak, `open_until`, trips, in-flight vs. effective `max_in_flight`, `rate_limit_per_s`, `auto_disabled_at` | reader |
| `POST` | `/v1/deliveries/{delivery_id}/replay` | Replay one `succeeded` / `failed` / `dead` delivery (`202`, the delivery); optional `Idempotency-Key` | publisher |
| `POST` | `/v1/events/{event_id}/replay` | Replay an event's deliveries (body optional: `statuses`, default `failed` + `dead`; `endpoint_id`) | publisher |
| `POST` | `/v1/replays` | Bulk replay: `created_after` / `created_before` (required, bounded), `statuses`, `endpoint_id`, `event_type`; `{"replayed", "has_more"}` | admin |
| `GET` | `/v1/replays` | Replay audit trail, newest first (scope, filters, count, key) | reader |

List responses share one shape: `{"data": [...], "next_cursor": "...", "has_more": true}`; pass
`?cursor=<next_cursor>&limit=<1..100>` for the next page.

Example: creating an endpoint.

```http
POST /v1/endpoints
Authorization: Bearer hrk_3k9x0q7m2p1a_...
Content-Type: application/json

{"url": "https://api.acme.example/webhooks/hookrelay", "description": "Billing service",
 "headers": {"X-Acme-Source": "hookrelay"}, "timeout_ms": 5000}
```

```http
HTTP/1.1 201 Created
Location: /v1/endpoints/0d3c8a6e-...
ETag: "1"
Cache-Control: no-store

{"id": "0d3c8a6e-...", "url": "https://api.acme.example/webhooks/hookrelay", "enabled": true,
 "timeout_ms": 5000, "version": 1, "signing_secret": "whsec_...", ...}
```

Example: publishing an event.

```http
POST /v1/events
Authorization: Bearer hrk_3k9x0q7m2p1a_...
Content-Type: application/json
Idempotency-Key: inv_1042-paid

{"event_type": "invoice.paid", "external_id": "inv_1042:paid",
 "occurred_at": "2026-11-01T09:30:00Z", "payload": {"invoice_id": "inv_1042", "amount": 4200}}
```

```http
HTTP/1.1 202 Accepted
Location: /v1/events/7b1e9a52-...

{"id": "7b1e9a52-...", "event_type": "invoice.paid", "external_id": "inv_1042:paid",
 "occurred_at": "2026-11-01T09:30:00Z", "created_at": "2026-11-01T09:30:00.120Z",
 "retain_until": "2026-12-01T09:30:00.120Z"}
```

Sending the same request again with the same key returns the same body and status plus
`Idempotent-Replayed: true`; a different body with that key gets `422 idempotency_key_mismatch`.

Example: replaying yesterday's dead letters for one endpoint after fixing it.

```http
POST /v1/replays
Authorization: Bearer hrk_3k9x0q7m2p1a_...
Content-Type: application/json
Idempotency-Key: replay-billing-2026-11-02

{"created_after": "2026-11-02T00:00:00Z", "created_before": "2026-11-03T00:00:00Z",
 "endpoint_id": "0d3c8a6e-...", "statuses": ["dead"]}
```

```http
HTTP/1.1 202 Accepted

{"id": "5f0a...", "scope": "filter", "filters": {"statuses": ["dead"], ...}, "replayed": 1000,
 "has_more": true, "created_at": "2026-11-03T09:00:00Z"}
```

`has_more: true` means the cap was hit; call again (with a new key) for the next batch.

Example: rotating an endpoint's signing secret with a one-day overlap.

```http
POST /v1/endpoints/0d3c8a6e-.../secret/rotate
Authorization: Bearer hrk_3k9x0q7m2p1a_...
If-Match: "3"
Content-Type: application/json

{"overlap_s": 86400}
```

```http
HTTP/1.1 200 OK
ETag: "4"
Cache-Control: no-store

{"endpoint_id": "0d3c8a6e-...", "signing_secret": "whsec_...", "rotated_at": "2026-11-04T10:00:00Z",
 "previous_secret_expires_at": "2026-11-05T10:00:00Z", "version": 4}
```

Until `previous_secret_expires_at` every attempt carries `webhook-signature: v1,<new> v1,<old>`.
Deploy the new secret to the receiver, then optionally `DELETE .../secret/previous`.

Example: yesterday's failed and dead `invoice.paid` deliveries.

```http
GET /v1/deliveries?status=failed&status=dead&event_type=invoice.paid&created_after=2026-11-03T00:00:00Z&created_before=2026-11-04T00:00:00Z&limit=50
Authorization: Bearer hrk_3k9x0q7m2p1a_...
```

Common error codes: `unauthorized`, `api_key_revoked`, `api_key_expired` (401); `insufficient_role`,
`tenant_suspended`, `operator_api_disabled` (403); `*_not_found` (404, e.g. `delivery_not_found`); `tenant_slug_taken`,
`endpoint_url_taken`, `event_type_exists`, `subscription_exists`, `api_key_already_rotated`,
`event_exists`, `idempotency_key_in_progress`, `delivery_not_replayable`, `endpoint_unavailable`, `payload_purged` (409); `precondition_failed` (412);
`request_too_large`, `payload_too_large` (413); `unsupported_media_type` (415);
`invalid_endpoint_url`, `timeout_too_large`, `unknown_event_type`, `event_type_archived`,
`occurred_at_in_future`, `idempotency_key_mismatch`, `max_in_flight_too_high`, `rate_limit_too_high`,
`replay_window_required`, `invalid_replay_window`, `replay_window_too_large`, `overlap_too_long`,
`invalid_signing_secret`, `invalid_time_range`, `window_too_large`, `validation_error` (422);
`precondition_required` (428); `replay_rate_limited` (429, with `Retry-After`); `secret_unavailable` (500). `invalid_idempotency_key` is a `400`.

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

## Verifying webhook signatures

Every delivery follows [Standard Webhooks](https://www.standardwebhooks.com/):

```http
POST /webhooks/hookrelay HTTP/1.1
Content-Type: application/json
User-Agent: hookrelay-webhooks/1.0
webhook-id: 7b1e9a52-6c1f-4f0e-9a51-2f3c8f1f4c11
webhook-timestamp: 1793594207
webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=

{"data":{"amount":4200,"invoice_id":"inv_1042"},"timestamp":"2026-11-02T09:30:00Z","type":"invoice.paid"}
```

The signature is `base64(HMAC-SHA256(secret, "{webhook-id}.{webhook-timestamp}.{raw body}"))`, where
`secret` is the base64 part of the endpoint's `whsec_...` signing secret. Receivers should verify the
**raw** bytes (never a re-serialised body), compare in constant time, reject timestamps more than
5 minutes away, accept any of several space-separated signatures (during a secret rotation each
attempt is signed with the new and the previous secret, so either verifies), and
dedupe on `webhook-id` (delivery is at-least-once). `hookrelay.core.signing` is dependency-free
(stdlib only) so receivers can vendor it:

```python
from fastapi import FastAPI, HTTPException, Request

from hookrelay.core.signing import WebhookVerificationError, verify

SECRET = "whsec_..."  # GET /v1/endpoints/{id}/secret (admin), keep it in your secret store
app = FastAPI()
seen: set[str] = set()  # use your database in production


@app.post("/webhooks/hookrelay")
async def receive(request: Request) -> dict[str, str]:
    body = await request.body()
    try:
        webhook_id = verify(SECRET, request.headers, body)
    except WebhookVerificationError as exc:
        raise HTTPException(status_code=400, detail="invalid signature") from exc
    if webhook_id not in seen:  # a retry of something already processed is acknowledged again
        seen.add(webhook_id)
        ...  # handle json.loads(body)["type"] / ["data"]
    return {"status": "ok"}
```

Answer `2xx` quickly and do slow work asynchronously: anything else, or no answer within the
endpoint's `timeout_ms`, is retried (`429` / `503` with `Retry-After` are honoured). Redirects are
not followed and count as failures, so point the endpoint at the final URL.

## Project layout

```
src/hookrelay/
  main.py               app factory + lifespan
  __main__.py           `hookrelay` / `python -m hookrelay` (uvicorn)
  healthcheck.py        container / Kubernetes probes, stdlib only: API liveness, `--heartbeat relay|worker`
  relay.py              fan-out relay process (`python -m hookrelay.relay` / `hookrelay-relay`)
  worker.py             delivery worker process (`python -m hookrelay.worker` / `hookrelay-worker`)
  retention.py          one bounded retention run (`python -m hookrelay.retention` / `hookrelay-retention`)
  mock_receiver.py      configurable webhook receiver for load / chaos tests (`hookrelay-mock-receiver`):
                        latency, jitter, failure / 429 / hang rates per endpoint, ack accounting, /stats
  redis_/               fail-soft redis.asyncio client, keys, coalescer, wakeup pub/sub, subscription
                        cache, Lua rate limiter, process runtime helpers (named redis_ so it does not
                        shadow the redis package)
  observability/        Prometheus metrics registry, OpenTelemetry setup + W3C propagation, gauge refresh
  api/
    deps.py             auth dependencies (operator token, API key → Principal, roles)
    contracts/          request/response models with OpenAPI examples
    routers/            thin HTTP layer: admin_tenants, api_keys, endpoints, event_types, subscriptions,
                        events, deliveries, replays, dashboard, me, health, metrics
    body.py             bounded body reading, strict JSON parsing, canonical JSON
    etag.py             ETag / If-Match parsing
  core/                 config, logging (redaction), errors, request context, security (keys, SecretBox),
                        signing (Standard Webhooks sign / verify, stdlib only)
  db/                   engine/session lifecycle, declarative base, models, integrity-error mapping
  services/             business logic (framework-free): tenants, api_keys, endpoints, endpoint_rules,
                        event_types, subscriptions, pagination, events, idempotency, fanout, housekeeping,
                        delivery_queue (lease / renew / complete / complete_many / reclaim / release
                        SQL), completion_batcher (group-commit flush policy), delivery_policy
                        (classification, backoff, Retry-After), ssrf (address vetting + guarded backend),
                        webhook_client (httpx), deliveries (read API), circuit_breaker (breaker state
                        machine + outcome recording), notifications (outbox + notifier), replays,
                        endpoint_secrets (rotation), endpoint_health (summary), dashboard (tenant
                        rollup), retention (payload purge + ended secret overlaps, batched runner);
                        deliveries also holds search
  deploy/               Prometheus scrape config, Grafana provisioning + starter dashboard,
                        pgbouncer/ (compose pgbouncer.ini + userlist, run-test-pooler.sh for tests / CI),
                        helm/hookrelay/ (the Helm chart) and helm/ci/ (validate.sh: lint + kubeconform +
                        guard rails; kind-smoke.sh + deps.yaml + values-kind.yaml; values-full.yaml)
alembic/                async env + versioned migrations (0001 tenants ... 0010 observability trace context)
loadtest/               k6 scripts (ingest.js, e2e.js, lib.js) + run-local.sh (starts the stack, runs k6,
                        optional CHAOS_CMD fault injection, collects summaries and logs)
docs/capacity.md        measured capacity plan (throughput per worker, scaling, pool sizing, storage, limits)
tests/receiver.py       raw-asyncio HTTP receiver + fake resolver used by delivery tests
tests/unit/             fast tests (fake database / mocked Redis): config, security, rules, pagination,
                        auth wiring audit, signing vectors, backoff / Retry-After, SSRF ranges, HTTP
                        client, worker helpers, Redis client / coalescer / cache / Lua bucket / wakeup
tests/integration/      real Postgres + Redis: migrations, schema constraints, full HTTP flows per
                        resource, idempotency races, concurrent relays and crash/replay, concurrent
                        workers, fencing, lease expiry, retries, SIGTERM drain, end-to-end signed
                        delivery, breaker lifecycle, in-flight / rate limits, fairness, auto-disable,
                        replays, secret rotation, delivery search + health, retention races, wake-up
                        short-circuit, subscription-cache hit/miss/invalidate, Redis RL + degraded mode,
                        dashboard + /metrics after a real delivery; chaos suite (worker SIGKILL, backend
                        storms, completion-write kill, slow / flaky / hanging receivers, drain in an outage)
```

## Load testing, chaos and capacity

Details: [loadtest/README.md](loadtest/README.md) (harness), [docs/capacity.md](docs/capacity.md)
(measured numbers and the capacity model), `tests/integration/test_chaos.py` (chaos suite).

```bash
# Mock receiver on its own (per-endpoint behaviour can also come from the endpoint URL's query string)
uv run hookrelay-mock-receiver --port 9000 --latency-ms 20 --jitter-ms 30 --failure-rate 0.02 --failure-status 500,503

# End to end against a local stack: 4 endpoints, realistic receivers, fail on any lost / double-acked delivery
export DATABASE_URL=postgresql+asyncpg://hookrelay:hookrelay@127.0.0.1:5432/hookrelay_load   # dedicated DB
WORKERS=2 bash loadtest/run-local.sh e2e.js -e RATE=75 -e DURATION=60s -e ENDPOINTS=4 \
  -e 'RECEIVER_QUERY=latency_ms=20&jitter_ms=30&failure_rate=0.02&failure_status=500,503'

# Chaos suite (real Postgres): worker SIGKILL, backend storms, completion-write kill, bad receivers
make test-chaos
```

Headline numbers from one shared 8-vCPU box running Postgres, Redis, k6 and every hookrelay process
together (so a floor, not a production figure; see the caveats in the capacity plan):

| What | Measured |
| --- | --- |
| Ingest, 1 API process | 200 req/s at p95 32.6 ms / p99 107.6 ms, 0 errors; saturates at ~230 req/s |
| Ingest, 4 API processes | 800 req/s at p95 58.9 ms / p99 110.6 ms, 0 errors; saturates at ~900 req/s |
| Delivery, 1 worker (50 ms receiver) | 265-274/s at concurrency 32, 368/s at 64, 430/s at 128; CPU-bound at ~1 core (≈ 2 ms CPU per delivery) |
| Delivery, 2 workers × 64, 16 endpoints | 547/s (1.29× one 128-slot worker); 4 workers hit the shared box's CPU limit |
| Steady state, 2 workers × 32, 4 endpoints, 20-50 ms receivers, 2% 5xx | 300 deliveries/s with lag p50 98 ms / p95 180 ms / p99 477 ms; saturates at ~375/s |
| Correctness | 0 lost and 0 acknowledged twice in every completed run except one stalled-box outlier (8 duplicates, explained in the plan) |
| Real Postgres restart under load | 0 lost, 0 acknowledged twice, 0 API 500s; ~0.3-0.4% of ingest requests got `503` + `Retry-After` during the restart |

What the chaos suite proves on every CI run:

| Scenario | Fault | Invariant checked |
| --- | --- | --- |
| Worker crash | worker subprocess `SIGKILL`-ed while the receiver holds its 8 requests | the 8 are redelivered exactly once after lease expiry (`lease_expired` attempt + success), nothing acknowledged twice |
| Postgres restart / failover | every backend of the test database terminated 8 times during ingest + delivery to fast, flaky and slow receivers | 60 events (idempotent client retries), 180 deliveries all succeeded, both loops survive |
| Lost completion | backend killed between the receiver's 2xx and the completion write | completion retried, no redelivery; with retries off exactly one redelivery |
| Slow receiver | one endpoint at high latency next to a healthy one | the healthy endpoint is not starved |
| Bad receivers | flaky `5xx`, `429` + `Retry-After`, hanging past the timeout | every delivery converges to exactly one acknowledgement |
| Drain during outage | `SIGTERM` while Postgres is unreachable | leases are left for the reaper; nothing lost |
| Relay connection loss | `57P01` / `PendingRollbackError` during fan-out | batch retried on a fresh connection, no row parked as poison |

## Scaling notes

- On Kubernetes (M9b), API replicas follow CPU through the HPA and workers follow delivery demand
  through KEDA: `ceil((due + delivering) / (0.75 × WORKER_CONCURRENCY))`, clamped to
  `[minReplicaCount, maxReplicaCount]`, scale-down limited to 2 pods a minute after a 5-minute
  stabilisation window (each removed worker drains for up to 25 s). Every new replica adds
  `DB_POOL_SIZE + DB_MAX_OVERFLOW` client connections, so with autoscaling on, run PgBouncer and
  size `maxReplicaCount` against its `max_client_conn`, not against Postgres. The measured
  per-worker throughput in [docs/capacity.md](docs/capacity.md) is the number to pick
  `deliveriesPerReplica` from.
- The API is stateless. Scale it horizontally behind a load balancer. Without a pooler Postgres sees
  `processes × (DB_POOL_SIZE + DB_MAX_OVERFLOW)` connections; with PgBouncer in transaction mode
  (compose default, `DB_PGBOUNCER=true`) that is only the client side, and Postgres sees at most
  `max_db_connections` (40 in `deploy/pgbouncer/pgbouncer.ini`: `default_pool_size` 20 + reserve 5,
  with headroom for migrations and admin sessions). See the connection budget in
  [docs/capacity.md](docs/capacity.md#connection-budget).
- Completion writes are group-committed (M9): one transaction per batch of up to
  `WORKER_COMPLETE_BATCH_MAX` attempts instead of one per attempt. In the backlog-drain benchmark
  (1 worker × 128 slots, 50 ms receivers) that took one worker from 388.5 to 759.6 deliveries/s and
  commits per delivery from ≈ 1.08 to ≈ 0.08; behind PgBouncer the same run did 618.5/s
  ([capacity plan](docs/capacity.md#11-milestone-9-batched-completions--pgbouncer)).
- Relays scale independently of the API: `SKIP LOCKED` hands each replica a disjoint batch, and the
  claim query is served by a partial index over pending outbox rows (`WHERE processed_at IS NULL`),
  so its cost tracks the backlog, not table size. One fan-out statement per batch keeps round trips
  at O(batches), not O(events × endpoints). Adaptive polling keeps idle relays to a few queries per
  second; Redis wake-ups (M6) cut idle latency when `WAKEUP_ENABLED` — Postgres polling remains the
  fallback when Redis is down.
- Ingest is a handful of indexed writes in one transaction (event, outbox row, idempotency row).
  Idempotency keys live in Postgres so the guarantee survives a Redis outage; expired keys are purged
  in batches by the relay, and the table is keyed `(tenant_id, key)`.
- Delivery workers scale independently of the API and relays: `SKIP LOCKED` leasing hands each worker a
  disjoint set (tests run 5 workers over 40 deliveries: every delivery sent exactly once, one attempt
  row each). The lease query is served by the partial `ix_deliveries_due` index (`WHERE status =
  'pending'`) and the reaper by `ix_deliveries_lease_expiry` (`WHERE status = 'delivering'`), so both
  cost O(due rows). Throughput per worker is `min(WORKER_CONCURRENCY / receiver latency, CPU limit)`: M8 measured
  the CPU limit at ~265-430 deliveries/s per worker process on the benchmark box (concurrency 32 → 128,
  50 ms receivers), so raise concurrency for slow receivers and add worker processes once a worker
  sits near one core, provided there are many more active endpoints than workers (the per-endpoint
  planner row is the unit of parallelism). Each worker holds one Postgres connection per lease /
  complete transaction, never during the HTTP call, so the default pool (10 + 5 overflow) carried a
  128-slot worker; larger pools measured no gain. See [docs/capacity.md](docs/capacity.md).
- One slow or broken endpoint is isolated: it holds at most `max_in_flight` slots across the whole
  fleet, its open breaker stops leasing entirely, and least-recently-leased ordering hands free slots
  to other endpoints first. The eligibility query scans `endpoint_states` rows with due work (one row
  per endpoint, locked `SKIP LOCKED`, so concurrent workers plan for disjoint endpoints) and counts
  in-flight / due rows through the partial `ix_deliveries_endpoint_delivering` and
  `ix_deliveries_endpoint_due` indexes; a fleet with tens of thousands of active endpoints would
  move the due set into a per-endpoint queue table (Redis already holds wake / RL / cache).
- The breaker row is a hot spot only for a single very busy endpoint; since M9 a completion batch
  updates it once per run of successes instead of once per attempt (failures are still applied one
  by one, in order). Redis counters are the next step if it shows up in `pg_stat_activity`.
- Bulk replay touches at most `REPLAY_MAX_DELIVERIES` rows per transaction via
  `ix_deliveries_tenant_created`, and replay rate limiting is a count over
  `ix_replay_requests_tenant_created` in the last minute.
- Authentication costs one indexed lookup (`api_keys.prefix` unique) joined to `tenants`, plus at most one
  throttled `last_used_at` write per key per minute, so hot keys do not turn reads into write load.
  A short-TTL in-process or Redis cache of `prefix → (hash, role, tenant status)` is the next step if
  auth shows up in profiles; revocation would then need explicit invalidation.
- Every list query is served by a `(tenant_id, created_at, id)` index (partial `WHERE deleted_at IS NULL`
  for endpoints), so pagination stays O(page size) no matter how many rows a tenant has.
- The fan-out lookup (`/v1/subscriptions/match` and the relay's fan-out query) uses `(tenant_id, event_type_pattern)` for
  exact matches; wildcard rows per tenant are few, and the resolved set is cached in Redis with
  explicit tenant invalidation (SQL fallback on miss).
- Delivery search walks one `(tenant_id, <filter>, created_at, id)` index in order, so a page costs
  O(page size) for single filters; a combination with a rare second filter can scan more of the
  first index (bounded by the time range). The health summary is an index-only scan over at most
  `ENDPOINT_HEALTH_MAX_ATTEMPTS` rows of `ix_delivery_attempts_endpoint_created` plus a capped
  backlog count; at very high attempt rates a per-minute rollup (M7) replaces it.
- Retention cost is O(batch) per transaction: the candidate scan uses the partial
  `ix_events_payload_unpurged` index, runners split the work with `SKIP LOCKED`, and the batch budget
  and pause keep it from competing with deliveries. Purging payloads keeps `events` rows small
  (TOASTed `jsonb` is most of their size) long before the rows themselves expire.
- Hot tables (`deliveries`, `delivery_attempts`) are designed for time-based partitioning (planned in
  M9; at the measured ~2.1 KB per delivery + attempt and ~1.4 KB per event, 300 deliveries/s is on the
  order of 65 GB/day before payload retention), which would turn expiry into dropping partitions. Redis only accelerates (wake-ups, subscription cache,
  rate-limit tokens). Losing it degrades latency / hot-path contention, not correctness or
  at-least-once delivery.

## Roadmap

See [ROADMAP.md](ROADMAP.md): Milestone 0 through Milestone 9, with a dated progress log.

## License

[MIT](LICENSE)
