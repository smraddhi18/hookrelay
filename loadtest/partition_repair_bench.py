"""Milestone 9e: how long a cascade-safe DEFAULT repair holds its transaction (real Postgres).

Builds a scratch database (migrated to head) with ``BG_DAYS`` past days of the event graph in
their own partitions (``BG_PER_DAY`` events each, so the parent tables are not empty) and then,
for each size in ``SIZES``, strands one future day of ``n`` events in the DEFAULT partitions
(one delivery, two attempts and a processed outbox row per event, so 5 rows per event), runs
``repair_day`` on it and prints the elapsed time of the one repair transaction, the rows moved
and the rows per second. Every row of the day is compared (``to_jsonb``) before and after.

    ADMIN_URL=postgresql://user:pw@localhost:5433/postgres SIZES=1000,10000 \
        uv run python loadtest/partition_repair_bench.py
"""

import asyncio, os, subprocess, time
from datetime import UTC, datetime, timedelta

import asyncpg

from hookrelay.db.partitioning import EVENTS
from hookrelay.services.partition_repair import repair_day
from hookrelay.services.partitions import PartitionPolicy, connect_kwargs

ADMIN = os.environ.get("ADMIN_URL", "postgresql://hookrelay:hookrelay@localhost:5433/postgres")
SIZES = [int(s) for s in os.environ.get("SIZES", "1000,10000").split(",")]
BG_DAYS = int(os.environ.get("BG_DAYS", "14"))
BG_PER_DAY = int(os.environ.get("BG_PER_DAY", "10000"))
NAME = "hr_m9e_bench"
URL = ADMIN.rsplit("/", 1)[0] + f"/{NAME}"


async def seed(c, tenant, eps, start, n):
    span = 86_000.0
    async with c.transaction():
        await c.execute(
            f"""CREATE TEMP TABLE ev ON COMMIT DROP AS SELECT gen_random_uuid() AS id, g AS i,
            '{start.isoformat()}'::timestamptz + (g::float8 / {n} * {span}) * interval '1 second' AS at
            FROM generate_series(1, {n}) g"""
        )
        await c.execute(
            "INSERT INTO events (id, tenant_id, event_type, payload, payload_size, occurred_at, "
            "created_at, retain_until, fanned_out_at) SELECT id, $1, 'x.y', '{\"k\": 1}', 8, at, at, "
            "at + interval '30 days', at FROM ev",
            tenant,
        )
        await c.execute(
            "INSERT INTO event_outbox (tenant_id, event_id, event_created_at, processed_at, attempts) "
            "SELECT $1, id, at, at, 1 FROM ev",
            tenant,
        )
        await c.execute(
            "INSERT INTO deliveries (tenant_id, event_id, endpoint_id, retain_until, event_created_at, "
            "status, next_attempt_at, created_at, completed_at) SELECT $1, id, ($2::uuid[])[1 + i % 50], "
            "at + interval '30 days', at, 'succeeded', at, at, at FROM ev",
            tenant,
            eps,
        )
        await c.execute(
            "INSERT INTO delivery_attempts (tenant_id, delivery_id, event_created_at, attempt_number, "
            "outcome, status_code, request_url, worker_id, started_at, finished_at) "
            "SELECT d.tenant_id, d.id, d.event_created_at, k, 'succeeded', 200, 'https://a.example.com/', "
            "'w', d.event_created_at, d.event_created_at FROM deliveries d JOIN ev ON ev.id = d.event_id, "
            "generate_series(1, 2) k"
        )


async def images(c, lo, hi):
    out = {}
    for t, key in (("events", "created_at"), ("deliveries", "event_created_at"),
                   ("delivery_attempts", "event_created_at"), ("event_outbox", "event_created_at")):
        out[t] = await c.fetchval(
            f"SELECT md5(string_agg(to_jsonb(t)::text, '' ORDER BY to_jsonb(t)::text)) FROM {t} t "
            f"WHERE {key} >= $1 AND {key} < $2", lo, hi)
    return out


async def main():
    admin = await asyncpg.connect(ADMIN)
    await admin.execute(f"DROP DATABASE IF EXISTS {NAME} WITH (FORCE)")
    await admin.execute(f"CREATE DATABASE {NAME}")
    await admin.close()
    env = dict(os.environ, DATABASE_URL=URL.replace("postgresql://", "postgresql+asyncpg://"))
    subprocess.run(["uv", "run", "alembic", "upgrade", "head"], check=True, env=env,
                   cwd=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."),
                   capture_output=True)
    policy = PartitionPolicy(premake_days=7, retention_days=30, repair_max_rows=10_000_000,
                             lock_timeout_ms=5_000)
    c = await asyncpg.connect(URL, **connect_kwargs(policy, application_name="repair-bench"))
    print("server", await c.fetchval("SHOW server_version"), flush=True)
    tenant = await c.fetchval("INSERT INTO tenants (slug, name) VALUES ('b','b') RETURNING id")
    await c.execute("INSERT INTO event_types (tenant_id, name) VALUES ($1,'x.y')", tenant)
    eps = [await c.fetchval(
        "INSERT INTO endpoints (tenant_id, url, secret_ciphertext) VALUES ($1,$2,'v1.k1.ciphertext') "
        "RETURNING id", tenant, f"https://e{i}.example.com/") for i in range(50)]
    today = (await c.fetchval("SELECT now()")).astimezone(UTC).date()
    for k in range(1, BG_DAYS + 1):
        d = today - timedelta(days=k)
        lo = datetime.combine(d, datetime.min.time(), UTC)
        for t in ("events", "deliveries", "delivery_attempts"):
            await c.execute(f"CREATE TABLE {t}_p{d:%Y%m%d} PARTITION OF {t} FOR VALUES FROM "
                            f"('{lo.isoformat()}') TO ('{(lo + timedelta(days=1)).isoformat()}')")
        await seed(c, tenant, eps, lo, BG_PER_DAY)
    print(f"background: {BG_DAYS} days x {BG_PER_DAY} events in their partitions", flush=True)
    for idx, n in enumerate(SIZES):
        day = today + timedelta(days=20 + idx)
        lo = datetime.combine(day, datetime.min.time(), UTC)
        await seed(c, tenant, eps, lo, n)
        await c.execute("ANALYZE")
        before = await images(c, lo, lo + timedelta(days=1))
        t0 = time.perf_counter()
        result = await repair_day(c, EVENTS, day, policy)
        wall = (time.perf_counter() - t0) * 1000
        after = await images(c, lo, lo + timedelta(days=1))
        homes = await c.fetchval("SELECT array_agg(DISTINCT tableoid::regclass::text) FROM events "
                                 "WHERE created_at >= $1 AND created_at < $2", lo, lo + timedelta(days=1))
        total = result.total_rows
        print(f"n={n} events: status={result.status} rows={result.rows} total={total} "
              f"repair={result.elapsed_ms} ms (wall {wall:.0f} ms) "
              f"{total / (wall / 1000):.0f} rows/s phases={result.phase_ms} identical={before == after} events_in={homes}",
              flush=True)
    await c.close()


asyncio.run(main())
