"""Operator notifications: a transactional outbox for "something needs a human" (Milestone 4).

A notification (for example ``endpoint.disabled``) is inserted in the same transaction as the
state change it reports, so it can neither be lost nor sent for a change that rolled back. The
relay dispatches pending rows through a pluggable :class:`~hookrelay.services.notifications.
Notifier` with retries; delivery is at-least-once.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base

MAX_NOTIFICATION_KIND_LENGTH = 64
MAX_NOTIFICATION_ERROR_LENGTH = 1_024


class NotificationStatus(StrEnum):
    PENDING = "pending"
    DISPATCHED = "dispatched"
    ABANDONED = "abandoned"  # the notifier kept failing for NOTIFICATION_MAX_ATTEMPTS


_STATUSES = ", ".join(f"'{s.value}'" for s in NotificationStatus)


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        CheckConstraint(f"status IN ({_STATUSES})", name="status_valid"),
        CheckConstraint("attempts >= 0", name="attempts_non_negative"),
        CheckConstraint(
            "(status = 'pending') = (completed_at IS NULL)", name="completed_iff_not_pending"
        ),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
        Index(
            "ix_notifications_due",
            "next_attempt_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_notifications_completed",
            "completed_at",
            postgresql_where=text("status <> 'pending'"),
        ),
        Index("ix_notifications_tenant_created", "tenant_id", "created_at", "id"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    # Not a FK: the notification must outlive a later endpoint deletion.
    endpoint_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    kind: Mapped[str] = mapped_column(String(MAX_NOTIFICATION_KIND_LENGTH), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'pending'")
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"Notification(id={self.id!s}, kind={self.kind!r}, status={self.status!r})"
