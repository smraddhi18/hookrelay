"""Declarative base with a deterministic constraint naming convention.

Alembic autogenerate and hand-written migrations both depend on stable constraint names,
otherwise downgrades and later ``ALTER``s have to guess what Postgres auto-named things.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, FetchedValue, MetaData, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        # Maintained by a BEFORE UPDATE trigger (see migration 0001) so raw SQL updates are
        # covered too; FetchedValue tells the ORM to re-read it after an UPDATE.
        server_onupdate=FetchedValue(),
        nullable=False,
    )
