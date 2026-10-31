"""Keyset (cursor) pagination over ``(created_at DESC, id DESC)``.

Unlike ``OFFSET``, a keyset page costs the same at page 1 and page 10 000 (an index range
scan from the cursor) and never skips or repeats rows when new rows are inserted between
page fetches. The cursor is opaque base64url JSON holding only the last row's sort key; it
grants nothing by itself because every query is still filtered by the caller's tenant.
"""

from __future__ import annotations

import base64
import binascii
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from sqlalchemy import Select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import InstrumentedAttribute

from hookrelay.core.errors import BadRequestError

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 100


class Keyed(Protocol):
    @property
    def id(self) -> uuid.UUID: ...

    @property
    def created_at(self) -> datetime: ...


@dataclass(frozen=True, slots=True)
class Cursor:
    created_at: datetime
    id: uuid.UUID


@dataclass(frozen=True, slots=True)
class Page[T]:
    items: list[T]
    next_cursor: str | None

    @property
    def has_more(self) -> bool:
        return self.next_cursor is not None


def encode_cursor(created_at: datetime, row_id: uuid.UUID) -> str:
    raw = json.dumps({"c": created_at.isoformat(), "i": str(row_id)}, separators=(",", ":"))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_cursor(token: str) -> Cursor:
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
        data = json.loads(raw)
        created_at = datetime.fromisoformat(data["c"])
        row_id = uuid.UUID(data["i"])
    except (binascii.Error, ValueError, TypeError, KeyError, UnicodeDecodeError) as exc:
        raise BadRequestError("Malformed pagination cursor", code="invalid_cursor") from exc
    if created_at.tzinfo is None:
        raise BadRequestError("Malformed pagination cursor", code="invalid_cursor")
    return Cursor(created_at=created_at, id=row_id)


async def paginate[M: Keyed](
    session: AsyncSession,
    stmt: Select[M],
    *,
    created_col: InstrumentedAttribute[datetime],
    id_col: InstrumentedAttribute[uuid.UUID],
    cursor: str | None,
    limit: int,
) -> Page[M]:
    """Apply the keyset predicate + ordering to ``stmt`` and fetch one page (limit+1 probe)."""
    limit = max(1, min(limit, MAX_PAGE_SIZE))
    if cursor is not None:
        position = decode_cursor(cursor)
        # Row-value comparison matches the composite index order exactly.
        stmt = stmt.where(tuple_(created_col, id_col) < tuple_(position.created_at, position.id))
    stmt = stmt.order_by(created_col.desc(), id_col.desc()).limit(limit + 1)
    rows: list[Any] = list((await session.scalars(stmt)).all())
    has_more = len(rows) > limit
    items: list[M] = rows[:limit]
    next_cursor = encode_cursor(items[-1].created_at, items[-1].id) if has_more else None
    return Page(items=items, next_cursor=next_cursor)
