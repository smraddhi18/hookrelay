"""FastAPI dependencies for database access."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession

from hookrelay.db.engine import Database


def get_database(request: Request) -> Database:
    database: Database = request.app.state.database
    return database


DatabaseDep = Annotated[Database, Depends(get_database)]


async def get_session(database: DatabaseDep) -> AsyncIterator[AsyncSession]:
    async with database.session() as session:
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_session)]
