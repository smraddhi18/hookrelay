"""Tenant: the isolation boundary. Every endpoint, event and delivery will belong to one."""

from __future__ import annotations

import uuid
from enum import StrEnum

from sqlalchemy import CheckConstraint, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base, TimestampMixin

SLUG_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"


class TenantStatus(StrEnum):
    ACTIVE = "active"
    SUSPENDED = "suspended"


class Tenant(TimestampMixin, Base):
    __tablename__ = "tenants"
    __table_args__ = (
        CheckConstraint(f"slug ~ '{SLUG_PATTERN}'", name="slug_format"),
        CheckConstraint("status IN ('active', 'suspended')", name="status_valid"),
        CheckConstraint("length(btrim(name)) BETWEEN 1 AND 200", name="name_length"),
    )
    # Fetch server-generated values (id, timestamps, trigger-maintained updated_at) via
    # RETURNING on INSERT *and* UPDATE: async sessions cannot lazy-load expired attributes.
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    slug: Mapped[str] = mapped_column(String(63), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    # Stored as text + CHECK rather than a native ENUM: adding a status later is a cheap
    # constraint swap instead of an ALTER TYPE that cannot run inside a transaction block.
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default=text("'active'"))

    def __repr__(self) -> str:
        return f"Tenant(id={self.id!s}, slug={self.slug!r}, status={self.status!r})"
