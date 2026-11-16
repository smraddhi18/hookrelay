"""Milestone 9d partitioning measurements against a real Postgres (never extrapolated).

Builds a scratch database with 31 UTC days of the event graph (``N_PER_DAY`` events, one delivery
each, two attempts each) in the partitioned layout, copies the same rows into the layouts the
previous milestones had (attempts partitioned by their own ``created_at``; a plain ``deliveries``
table), and prints planning + execution time (``EXPLAIN ANALYZE`` medians) for: the endpoint
health window, the lease candidate pick, keyed vs id-only lookups; then times retiring the oldest
day child-first (DETACH and DROP per table) and, for comparison, the row-by-row DELETE of the
next day that the pre-0012 retention path ran.

    ADMIN_URL=postgresql://user:pw@localhost:5433/postgres N_PER_DAY=15000 \
        uv run python loadtest/partition_bench.py
"""

import asyncio, os, re, statistics, subprocess, sys, time
from datetime import UTC, date, datetime, timedelta
import asyncpg

N = int(os.environ.get("N_PER_DAY", "15000"))
DAYS = 30
ADMIN = os.environ.get("ADMIN_URL", "postgresql://hookrelay:hookrelay@localhost:5433/postgres")
NAME = "hr_m9d_bench"
URL = ADMIN.rsplit("/", 1)[0] + f"/{NAME}"


async def med(conn, sql, *args, runs=25):
    plan_t, exec_t = [], []
    for _ in range(runs):
        rows = await conn.fetch("EXPLAIN (ANALYZE, TIMING OFF, SUMMARY ON) " + sql, *args)
        txt = "\n".join(r[0] for r in rows)
        plan_t.append(float(re.search(r"Planning Time: ([\d.]+)", txt).group(1)))
        exec_t.append(float(re.search(r"Execution Time: ([\d.]+)", txt).group(1)))
    return statistics.median(plan_t), statistics.median(exec_t), min(exec_t), max(exec_t)


async def main():
    admin = await asyncpg.connect(ADMIN)
    await admin.execute(f"DROP DATABASE IF EXISTS {NAME} WITH (FORCE)")
    await admin.execute(f"CREATE DATABASE {NAME}")
    await admin.close()
    env = dict(os.environ, DATABASE_URL=URL.replace("postgresql://", "postgresql+asyncpg://"))
    subprocess.run(
        ["uv", "run", "alembic", "upgrade", "head"],
        check=True,
        env=env,
        cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."),
        capture_output=True,
    )
    c = await asyncpg.connect(URL, server_settings={"TimeZone": "UTC"})
    now = await c.fetchval("SELECT now()")
    today = now.date()
    for k in range(1, DAYS + 1):
        d = today - timedelta(days=k)
        lo, hi = (
            datetime.combine(d, datetime.min.time(), UTC),
            datetime.combine(d + timedelta(days=1), datetime.min.time(), UTC),
        )
        for t, _key in (("events", 1), ("deliveries", 1), ("delivery_attempts", 1)):
            await c.execute(
                f"CREATE TABLE {t}_p{d:%Y%m%d} PARTITION OF {t} FOR VALUES FROM ('{lo.isoformat()}') TO ('{hi.isoformat()}')"
            )
    tenant = await c.fetchval("INSERT INTO tenants (slug, name) VALUES ('b','b') RETURNING id")
    await c.execute("INSERT INTO event_types (tenant_id, name) VALUES ($1,'x.y')", tenant)
    eps = [
        await c.fetchval(
            "INSERT INTO endpoints (tenant_id, url, secret_ciphertext) VALUES ($1,$2,'v1.k1.ciphertext') RETURNING id",
            tenant,
            f"https://e{i}.example.com/",
        )
        for i in range(50)
    ]
    t0 = time.time()
    for k in range(DAYS, -1, -1):
        d = today - timedelta(days=k)
        start = datetime.combine(d, datetime.min.time(), UTC)
        span = min(86400.0, (now - start).total_seconds() - 1)
        status = "pending" if k == 0 else "succeeded"
        async with c.transaction():
            await c.execute(f"""CREATE TEMP TABLE ev ON COMMIT DROP AS SELECT gen_random_uuid() AS id, g AS i,
              '{start.isoformat()}'::timestamptz + (g::float8 / {N} * {span}) * interval '1 second' AS at
              FROM generate_series(1, {N}) g""")
            await c.execute(
                "INSERT INTO events (id, tenant_id, event_type, payload, payload_size, occurred_at, created_at, retain_until) "
                "SELECT id, $1, 'x.y', '{}', 2, at, at, at + interval '30 days' FROM ev",
                tenant,
            )
            await c.execute(
                f"""INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, event_created_at, status,
                 next_attempt_at, created_at, completed_at)
                 SELECT $1, id, ($2::uuid[])[1 + i % 50], at + interval '30 days', at, '{status}', at, at,
                 CASE WHEN '{status}' = 'succeeded' THEN at END FROM ev""",
                tenant,
                eps,
            )
            await c.execute(
                """INSERT INTO delivery_attempts (tenant_id, delivery_id, event_created_at, attempt_number, outcome,
                 status_code, latency_ms, error_class, request_url, worker_id, started_at, finished_at, created_at)
                 SELECT d.tenant_id, d.id, d.event_created_at, n, CASE WHEN n = 1 THEN 'retrying' ELSE 'succeeded' END,
                 CASE WHEN n = 1 THEN 503 ELSE 200 END, 20 + (random()*80)::int, CASE WHEN n = 1 THEN 'http_status' END, 'https://a.example.com/', 'w',
                 d.event_created_at + n * interval '1 minute', d.event_created_at + n * interval '1 minute',
                 d.event_created_at + n * interval '1 minute'
                 FROM deliveries d, generate_series(1, 2) n WHERE d.event_created_at >= $1 AND d.event_created_at < $2""",
                start,
                start + timedelta(days=1),
            )
    await c.execute("ANALYZE")
    counts = {
        t: await c.fetchval(f"SELECT count(*) FROM {t}")
        for t in ("events", "deliveries", "delivery_attempts")
    }
    parts = {
        t: await c.fetchval("SELECT count(*) FROM pg_inherits WHERE inhparent=$1::regclass", t)
        for t in counts
    }
    print(f"loaded {counts} in {time.time() - t0:.0f}s; partitions {parts}", flush=True)

    # Baselines: the same rows in the shapes 0011 / a plain table would have.
    await c.execute(
        "CREATE TABLE att_base (LIKE delivery_attempts INCLUDING DEFAULTS) PARTITION BY RANGE (created_at)"
    )
    for k in range(DAYS + 8, -8, -1):
        d = today - timedelta(days=k)
        lo, hi = (
            datetime.combine(d, datetime.min.time(), UTC),
            datetime.combine(d + timedelta(days=1), datetime.min.time(), UTC),
        )
        await c.execute(
            f"CREATE TABLE att_base_p{d:%Y%m%d} PARTITION OF att_base FOR VALUES FROM ('{lo.isoformat()}') TO ('{hi.isoformat()}')"
        )
    await c.execute("INSERT INTO att_base SELECT * FROM delivery_attempts")
    await c.execute(
        "CREATE INDEX ON att_base (endpoint_id, created_at) INCLUDE (outcome, latency_ms, error_class)"
    )
    await c.execute("CREATE TABLE del_plain (LIKE deliveries INCLUDING DEFAULTS)")
    await c.execute("INSERT INTO del_plain SELECT * FROM deliveries")
    await c.execute(
        "CREATE INDEX ON del_plain (endpoint_id, next_attempt_at, id) WHERE status = 'pending'"
    )
    await c.execute("ANALYZE")
    ep = eps[7]

    q_health = (
        "SELECT count(*), count(*) FILTER (WHERE outcome <> 'succeeded'), percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) "
        "FROM {t} WHERE endpoint_id = $1 AND created_at >= now() - interval '{w}'"
    )
    for w in ("1 hour", "24 hours"):
        for label, t in (
            ("0012 (attempts keyed by event day)", "delivery_attempts"),
            ("0011-style (keyed by attempt created_at)", "att_base"),
        ):
            p, e, lo_, hi_ = await med(c, q_health.format(t=t, w=w), ep)
            print(
                f"health window {w:9s} | {label:42s} | plan {p:7.2f} ms | exec median {e:7.2f} ms (min {lo_:.2f}, max {hi_:.2f})",
                flush=True,
            )
    q_lease = (
        "SELECT picked.id FROM unnest($1::uuid[]) AS a(endpoint_id) CROSS JOIN LATERAL ("
        "SELECT d.id FROM {t} d WHERE d.endpoint_id = a.endpoint_id AND d.status = 'pending' AND d.next_attempt_at <= now() "
        "ORDER BY d.next_attempt_at, d.id LIMIT 20) picked"
    )
    for label, t in (
        ("0012 deliveries (39 partitions)", "deliveries"),
        ("plain deliveries table", "del_plain"),
    ):
        p, e, lo_, hi_ = await med(c, q_lease.format(t=t), eps)
        print(
            f"lease pick (50 endpoints x 20) | {label:34s} | plan {p:7.2f} ms | exec median {e:7.2f} ms (min {lo_:.2f}, max {hi_:.2f})",
            flush=True,
        )
    q_hot = "SELECT * FROM {t} WHERE id = $1 {k}"
    some = await c.fetchrow(
        "SELECT id, event_created_at FROM deliveries WHERE event_created_at < now() - interval '10 days' LIMIT 1"
    )
    for label, sql, args in (
        (
            "id + key (hot paths)",
            "SELECT * FROM deliveries WHERE id = $1 AND event_created_at = $2",
            (some["id"], some["event_created_at"]),
        ),
        (
            "id only (probes every partition)",
            "SELECT * FROM deliveries WHERE id = $1",
            (some["id"],),
        ),
    ):
        p, e, lo_, hi_ = await med(c, sql, *args)
        print(f"delivery by {label:34s} | plan {p:7.2f} ms | exec median {e:7.3f} ms", flush=True)

    # Retention: drop the oldest day child-first; time every statement.
    oldest = today - timedelta(days=DAYS)
    ev_rows = await c.fetchval(f"SELECT count(*) FROM events_p{oldest:%Y%m%d}")
    for t in ("delivery_attempts", "deliveries", "events"):
        t1 = time.perf_counter()
        await c.execute(f"ALTER TABLE {t} DETACH PARTITION {t}_p{oldest:%Y%m%d}")
        t2 = time.perf_counter()
        await c.execute(f"DROP TABLE {t}_p{oldest:%Y%m%d}")
        t3 = time.perf_counter()
        print(
            f"retire {t:18s} day with {ev_rows} events: DETACH {1000 * (t2 - t1):8.1f} ms, DROP {1000 * (t3 - t2):7.1f} ms",
            flush=True,
        )
    # Row-by-row comparison: DELETE one more day of events with cascade (what 0011 still did).
    nxt = today - timedelta(days=DAYS - 1)
    lo = datetime.combine(nxt, datetime.min.time(), UTC)
    t1 = time.perf_counter()
    await c.execute(
        "DELETE FROM events WHERE created_at >= $1 AND created_at < $2", lo, lo + timedelta(days=1)
    )
    print(
        f"row-by-row DELETE of one day ({ev_rows} events cascading to deliveries + attempts): {1000 * (time.perf_counter() - t1):.0f} ms",
        flush=True,
    )
    await c.close()
    admin = await asyncpg.connect(ADMIN)
    await admin.execute(f"DROP DATABASE {NAME} WITH (FORCE)")
    await admin.close()


asyncio.run(main())
