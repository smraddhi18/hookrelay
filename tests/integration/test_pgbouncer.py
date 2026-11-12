"""Milestone 9: hookrelay behind PgBouncer in transaction pooling mode.

Needs ``TEST_PGBOUNCER_URL``: a PgBouncer (``pool_mode = transaction``) whose ``*`` database
entry forwards every database name to the ``TEST_DATABASE_URL`` server, configured like
``deploy/pgbouncer/pgbouncer-test.ini`` (CI starts exactly that). Its
``max_prepared_statements = 0`` models the strictest pooler (PgBouncer before 1.21, or with
protocol-level prepared-statement support off), which is what ``DB_PGBOUNCER=true`` must
survive.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from hookrelay.core.config import Settings, normalize_database_url
from hookrelay.db.engine import Database
from hookrelay.mock_receiver import RunningReceiver
from tests.conftest import make_settings
from tests.integration.conftest import api_client
from tests.integration.test_chaos import (  # noqa: F401 - fixtures
    all_settled,
    assert_at_least_once,
    chaos_settings,
    mock,
    provision,
    publish_reliably,
    query,
    start_pipeline,
    wait_until,
)
from tests.integration.test_worker import db_url, worker_template  # noqa: F401

TEST_PGBOUNCER_URL = os.environ.get("TEST_PGBOUNCER_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not TEST_PGBOUNCER_URL, reason="TEST_PGBOUNCER_URL is not set"),
]


def through_pooler(database_url: str) -> str:
    """The same database, reached through PgBouncer instead of directly."""
    assert TEST_PGBOUNCER_URL
    pooler = make_url(normalize_database_url(TEST_PGBOUNCER_URL))
    direct = make_url(database_url)
    return direct.set(
        host=pooler.host, port=pooler.port, username=pooler.username, password=pooler.password
    ).render_as_string(hide_password=False)


async def _hammer(db: Database, *, clients: int = 30, rounds: int = 15) -> list[str]:
    """Many concurrent clients, many short transactions, a few distinct statements each."""
    errors: list[str] = []

    async def client(i: int) -> None:
        try:
            for r in range(rounds):
                async with db.session() as session:
                    total = await session.scalar(
                        text("SELECT CAST(:a AS int) + CAST(:b AS int)"), {"a": i, "b": r}
                    )
                    assert total == i + r
                    doc = await session.scalar(
                        text("SELECT CAST(:doc AS jsonb) -> 'k'"), {"doc": f'{{"k": {r}}}'}
                    )
                    assert doc == r
        except Exception as exc:
            errors.append(f"{type(exc).__name__}: {exc}"[:200])

    await asyncio.gather(*(client(i) for i in range(clients)))
    return errors


async def test_asyncpg_defaults_break_under_transaction_pooling(db_url: str) -> None:
    """Why DB_PGBOUNCER exists: asyncpg's per-connection statement names collide (or vanish)
    once PgBouncer hands one server connection to many clients."""
    engine = create_async_engine(
        through_pooler(db_url),
        pool_size=20,
        connect_args={"server_settings": {"application_name": "hookrelay-naive"}},
    )
    db = Database(engine)
    try:
        errors = await _hammer(db)
    finally:
        await db.dispose()
    assert errors, "expected prepared-statement collisions behind the transaction pooler"
    assert all("prepared statement" in e for e in errors), errors[:3]


async def test_the_default_startup_parameters_are_refused_by_pgbouncer(db_url: str) -> None:
    db = Database.from_settings(make_settings(database_url=through_pooler(db_url)))
    try:
        with pytest.raises(Exception, match="unsupported startup parameter"):
            async with db.session() as session:
                await session.execute(text("SELECT 1"))
    finally:
        await db.dispose()


async def test_pgbouncer_mode_survives_many_clients_on_few_server_connections(
    db_url: str,
) -> None:
    settings = make_settings(
        database_url=through_pooler(db_url), db_pgbouncer=True, db_pool_size=30
    )
    db = Database.from_settings(settings)
    try:
        assert await _hammer(db) == []
        await db.ping(2.0)
    finally:
        await db.dispose()
    # The pooler really multiplexed: far fewer server sessions than the 30 clients.
    name = make_url(db_url).database
    [row] = await query(
        db_url,
        "SELECT count(*) AS n FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
        name,
    )
    assert row["n"] <= 8


async def test_ingest_fan_out_and_batched_delivery_through_pgbouncer(
    mock: RunningReceiver,
    chaos_settings: Callable[..., Settings],
    db_url: str,
) -> None:
    """The whole write path behind the pooler: API ingest with Idempotency-Key (SET LOCAL
    lock_timeout), outbox relay, worker leasing (SKIP LOCKED) and group-committed completions,
    with 0 lost and 0 acknowledged twice."""
    pooled = through_pooler(db_url)
    async with api_client(
        pooled,
        db_pgbouncer=True,
        endpoint_allow_http=True,
        endpoint_allow_private_networks=True,
    ) as api:
        tenant, _ = await provision(
            api, mock, mock.url("/a"), mock.url("/b", failure_rate=0.2, failure_status=503)
        )
        settings = chaos_settings(db_pgbouncer=True).model_copy(
            update={"database_url": SecretStr(pooled)}
        )
        running = await start_pipeline(settings)
        try:
            await publish_reliably(api, tenant, 40, prefix="pgb")
            await wait_until(lambda: all_settled(db_url, 80), within_s=60)
        finally:
            await running.stop()

    semantics = await assert_at_least_once(db_url, mock.app)
    assert (semantics.deliveries, semantics.acked_duplicates) == (80, 0)
    stats = running.worker.stats
    assert (stats.loop_errors, stats.completion_fallbacks) == (0, 0)
    assert stats.completion_batches > 0
