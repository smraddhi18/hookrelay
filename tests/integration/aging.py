"""Test helper: make events (and everything hanging off them) look older.

``events.created_at`` is the partition key of the whole event graph (migration 0012) and part
of the composite foreign keys, so production code never updates it. Tests that need an old
event re-key the graph in one transaction with ``session_replication_role = replica`` (no FK
triggers; the test role is a superuser), moving rows between daily partitions exactly as a real
old row would sit.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

import asyncpg


async def age_events(
    db_url: str,
    created_ago: str = "3 days",
    *,
    retain_until_ago: str | None = None,
    only: Sequence[uuid.UUID | str] | None = None,
    except_: Sequence[uuid.UUID | str] | None = None,
) -> int:
    """Set ``created_at`` of the selected events (default: all) to ``now() - created_ago`` and
    carry the new key into their deliveries, attempts and outbox rows; optionally also set
    ``retain_until`` to ``now() - retain_until_ago``. Intervals are SQL interval literals
    (``"3 days"``). Returns the number of events re-keyed."""
    where, args = "true", []
    if only is not None:
        where, args = "id = ANY($1::uuid[])", [[uuid.UUID(str(i)) for i in only]]
    elif except_ is not None:
        where, args = "id <> ALL($1::uuid[])", [[uuid.UUID(str(i)) for i in except_]]
    retain = (
        f", retain_until = now() - interval '{retain_until_ago}'"
        if retain_until_ago is not None
        else ""
    )
    conn = await asyncpg.connect(db_url.replace("postgresql+asyncpg://", "postgresql://", 1))
    try:
        async with conn.transaction():
            await conn.execute("SET LOCAL session_replication_role = replica")
            await conn.execute(
                "CREATE TEMP TABLE aged ON COMMIT DROP AS SELECT id, created_at AS old, "  # noqa: S608
                f"now() - interval '{created_ago}' AS new FROM events WHERE {where}",
                *args,
            )
            await conn.execute(
                "UPDATE delivery_attempts AS t SET event_created_at = a.new "
                "FROM aged AS a JOIN deliveries AS d "
                "ON d.event_id = a.id AND d.event_created_at = a.old "
                "WHERE t.delivery_id = d.id AND t.event_created_at = a.old"
            )
            await conn.execute(
                "UPDATE deliveries AS d SET event_created_at = a.new FROM aged AS a "
                "WHERE d.event_id = a.id AND d.event_created_at = a.old"
            )
            await conn.execute(
                "UPDATE event_outbox AS o SET event_created_at = a.new FROM aged AS a "
                "WHERE o.event_id = a.id"
            )
            await conn.execute(
                f"UPDATE events AS e SET created_at = a.new{retain} FROM aged AS a "  # noqa: S608
                "WHERE e.id = a.id AND e.created_at = a.old"
            )
            return int(await conn.fetchval("SELECT count(*) FROM aged"))
    finally:
        await conn.close()
