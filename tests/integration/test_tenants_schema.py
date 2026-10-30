"""The baseline schema enforces its invariants in Postgres, not just in Python."""

from __future__ import annotations

import uuid

import asyncpg
import pytest


async def _insert(
    pg: asyncpg.Connection, slug: str, name: str = "Acme", **extra: str
) -> asyncpg.Record:
    cols = ["slug", "name", *extra]
    params = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    row = await pg.fetchrow(
        f"INSERT INTO tenants ({', '.join(cols)}) VALUES ({params}) RETURNING *",  # noqa: S608
        slug,
        name,
        *extra.values(),
    )
    assert row is not None
    return row


async def test_defaults(pg: asyncpg.Connection) -> None:
    row = await _insert(pg, "acme")
    assert isinstance(row["id"], uuid.UUID)
    assert row["status"] == "active"
    assert row["created_at"] is not None
    assert row["created_at"].tzinfo is not None
    assert row["updated_at"] == row["created_at"]


@pytest.mark.parametrize("slug", ["a", "ab", "acme-corp", "a1", "x" * 63])
async def test_valid_slugs(pg: asyncpg.Connection, slug: str) -> None:
    await _insert(pg, slug)


@pytest.mark.parametrize("slug", ["", "-acme", "acme-", "Acme", "ac me", "acme_corp", "x" * 64])
async def test_invalid_slugs_rejected(pg: asyncpg.Connection, slug: str) -> None:
    with pytest.raises((asyncpg.CheckViolationError, asyncpg.StringDataRightTruncationError)):
        await _insert(pg, slug)


async def test_slug_is_unique(pg: asyncpg.Connection) -> None:
    await _insert(pg, "dup")
    with pytest.raises(asyncpg.UniqueViolationError) as info:
        await _insert(pg, "dup")
    assert info.value.constraint_name == "uq_tenants_slug"


async def test_status_check(pg: asyncpg.Connection) -> None:
    await _insert(pg, "s1", status="suspended")
    with pytest.raises(asyncpg.CheckViolationError) as info:
        await _insert(pg, "s2", status="deleted")
    assert info.value.constraint_name == "ck_tenants_status_valid"


async def test_blank_name_rejected(pg: asyncpg.Connection) -> None:
    with pytest.raises(asyncpg.CheckViolationError) as info:
        await _insert(pg, "blank", name="   ")
    assert info.value.constraint_name == "ck_tenants_name_length"


async def test_updated_at_trigger_overrides_client_values(pg: asyncpg.Connection) -> None:
    row = await _insert(pg, "touch")
    after = await pg.fetchrow(
        "UPDATE tenants SET name = 'Renamed', updated_at = 'epoch' WHERE id = $1 "
        "RETURNING updated_at, now() AS db_now",
        row["id"],
    )
    assert after is not None
    # The BEFORE UPDATE trigger wins over whatever the writer sent (raw SQL included).
    assert after["updated_at"] == after["db_now"]
    assert after["updated_at"].year > 1970
