"""Deliveries: one row per (event, endpoint), created by the fan-out relay.

Milestone 3 workers lease ``pending`` rows whose ``next_attempt_at`` is due; the columns they
need (status, attempt_count, next_attempt_at, timestamps) exist from the start so the relay
and the workers agree on one schema.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base, TimestampMixin


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERING = "delivering"
    SUCCEEDED = "succeeded"
    DEAD = "dead"

    @property
    def is_terminal(self) -> bool:
        return self in {DeliveryStatus.SUCCEEDED, DeliveryStatus.DEAD}


_STATUSES = ", ".join(f"'{s.value}'" for s in DeliveryStatus)


class Delivery(TimestampMixin, Base):
    __tablename__ = "deliveries"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            ondelete="CASCADE",
            name="fk_deliveries_event_same_tenant",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["endpoints.tenant_id", "endpoints.id"],
            ondelete="CASCADE",
            name="fk_deliveries_endpoint_same_tenant",
        ),
        # Exactly one delivery per (event, endpoint): the relay inserts with ON CONFLICT DO
        # NOTHING, so a re-run after a crash, or two relays racing, can never duplicate.
        UniqueConstraint("event_id", "endpoint_id", name="uq_deliveries_event_endpoint"),
        CheckConstraint(f"status IN ({_STATUSES})", name="status_valid"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        CheckConstraint(
            "(status IN ('succeeded', 'dead')) = (completed_at IS NOT NULL)",
            name="completed_iff_terminal",
        ),
        # Worker queue (M3): due pending deliveries, oldest first.
        Index(
            "ix_deliveries_due",
            "next_attempt_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index("ix_deliveries_endpoint_created", "endpoint_id", "created_at", "id"),
        Index("ix_deliveries_tenant_created", "tenant_id", "created_at", "id"),
        Index("ix_deliveries_retain_until", "retain_until"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    endpoint_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    retain_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return f"Delivery(id={self.id!s}, status={self.status!r})"
