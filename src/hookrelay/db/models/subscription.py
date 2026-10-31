"""Subscriptions: endpoint ↔ event type pattern (``invoice.paid`` or wildcard ``invoice.*``)."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base
from hookrelay.db.models.event_type import SEGMENT

# ``*`` (everything), ``invoice.paid`` (exact) or ``invoice.*`` (every type under ``invoice.``).
SUBSCRIPTION_PATTERN = rf"^(\*|{SEGMENT}(\.{SEGMENT}){{0,7}}(\.\*)?)$"
MAX_PATTERN_LENGTH = 130


class Subscription(Base):
    """Immutable link row (create / delete only), hence no ``updated_at``/``version``."""

    __tablename__ = "subscriptions"
    __table_args__ = (
        # Composite FKs carry tenant_id, so a cross-tenant link is impossible even for raw SQL.
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["endpoints.tenant_id", "endpoints.id"],
            ondelete="CASCADE",
            name="fk_subscriptions_endpoint_same_tenant",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "event_type_id"],
            ["event_types.tenant_id", "event_types.id"],
            ondelete="RESTRICT",
            name="fk_subscriptions_event_type_same_tenant",
        ),
        UniqueConstraint(
            "endpoint_id", "event_type_pattern", name="uq_subscriptions_endpoint_pattern"
        ),
        CheckConstraint(f"event_type_pattern ~ '{SUBSCRIPTION_PATTERN}'", name="pattern_format"),
        # Exact patterns must reference a registered type; wildcards must not reference one.
        CheckConstraint(
            "(right(event_type_pattern, 1) = '*') = (event_type_id IS NULL)",
            name="exact_pattern_has_event_type",
        ),
        # Fan-out lookup for exact matches: WHERE tenant_id = $1 AND event_type_pattern = $2.
        Index("ix_subscriptions_tenant_pattern", "tenant_id", "event_type_pattern"),
        Index("ix_subscriptions_endpoint_created", "endpoint_id", "created_at", "id"),
        # FK-side index so RESTRICT checks on event_types do not scan; only exact rows have one.
        Index(
            "ix_subscriptions_event_type_id",
            "event_type_id",
            postgresql_where=text("event_type_id IS NOT NULL"),
        ),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    endpoint_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type_pattern: Mapped[str] = mapped_column(String(MAX_PATTERN_LENGTH), nullable=False)
    event_type_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    @property
    def is_wildcard(self) -> bool:
        return self.event_type_pattern.endswith("*")

    def __repr__(self) -> str:
        return f"Subscription(id={self.id!s}, pattern={self.event_type_pattern!r})"
