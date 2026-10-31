"""Per-tenant registry of event type names (``invoice.paid``) that endpoints can subscribe to."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base, TimestampMixin

# Dot-separated lowercase segments, at most 8 of them: ``invoice.paid``, ``user.email_changed``.
SEGMENT = r"[a-z0-9][a-z0-9_-]*"
EVENT_TYPE_PATTERN = rf"^{SEGMENT}(\.{SEGMENT}){{0,7}}$"
MAX_EVENT_TYPE_LENGTH = 128


class EventType(TimestampMixin, Base):
    __tablename__ = "event_types"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_event_types_tenant_id_name"),
        UniqueConstraint("tenant_id", "id", name="uq_event_types_tenant_id_id"),
        CheckConstraint(f"name ~ '{EVENT_TYPE_PATTERN}'", name="name_format"),
        CheckConstraint("version >= 1", name="version_positive"),
        Index("ix_event_types_tenant_created", "tenant_id", "created_at", "id"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(MAX_EVENT_TYPE_LENGTH), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500))
    # Archived types stay resolvable (old subscriptions/deliveries) but accept no new subscriptions.
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("1"))

    def __repr__(self) -> str:
        return f"EventType(id={self.id!s}, name={self.name!r})"
