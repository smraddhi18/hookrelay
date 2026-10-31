"""One clock: the database's. Expiry and overlap windows are compared against ``now()`` of the
current transaction, so app servers with skewed clocks cannot disagree about key validity."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


async def db_now(session: AsyncSession) -> datetime:
    value: datetime | None = await session.scalar(select(func.now()))
    assert value is not None  # noqa: S101 - now() is never NULL
    return value
