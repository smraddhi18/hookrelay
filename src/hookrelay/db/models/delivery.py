"""Deliveries: one row per (event, endpoint), created by the fan-out relay.

State machine (Milestone 3)::

    pending --lease--> delivering --2xx--> succeeded
       ^                   |
       |                   +--terminal error, attempts or age exhausted--> failed
       |                   |
       +--retryable error--+   (also: lease expired, or released on drain)

A worker owns a ``delivering`` row only while it holds the row's current ``lease_token``
(a fencing token minted on every lease): every state transition is
``UPDATE ... WHERE lease_token = :mine``, so a worker whose lease expired and was taken over
can never overwrite the new owner's result. ``dead`` is reserved for the Milestone 4
dead-letter queue; ``failed`` rows stay replayable.
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
    FAILED = "failed"
    DEAD = "dead"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset(
    {DeliveryStatus.SUCCEEDED, DeliveryStatus.FAILED, DeliveryStatus.DEAD}
)
MAX_WORKER_ID_LENGTH = 128
MAX_ERROR_CLASS_LENGTH = 32

_STATUSES = ", ".join(f"'{s.value}'" for s in DeliveryStatus)
_TERMINAL = ", ".join(f"'{s.value}'" for s in sorted(TERMINAL_STATUSES))


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
        # Target of the composite (tenant_id, delivery_id) FK from delivery_attempts.
        UniqueConstraint("tenant_id", "id", name="uq_deliveries_tenant_id_id"),
        CheckConstraint(f"status IN ({_STATUSES})", name="status_valid"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_non_negative"),
        CheckConstraint(
            f"(status IN ({_TERMINAL})) = (completed_at IS NOT NULL)",
            name="completed_iff_terminal",
        ),
        # A lease (token + expiry + owner) exists exactly while a worker is delivering.
        CheckConstraint(
            "(status = 'delivering') = (lease_token IS NOT NULL) "
            "AND (lease_token IS NULL) = (leased_until IS NULL) "
            "AND (lease_token IS NULL) = (leased_by IS NULL)",
            name="lease_iff_delivering",
        ),
        CheckConstraint(
            "last_status_code IS NULL OR last_status_code BETWEEN 100 AND 599",
            name="last_status_code_range",
        ),
        # Worker queue: due pending deliveries, oldest first.
        Index(
            "ix_deliveries_due",
            "next_attempt_at",
            postgresql_where=text("status = 'pending'"),
        ),
        # Lease reaper: in-flight deliveries whose lease ran out (crashed / partitioned worker).
        Index(
            "ix_deliveries_lease_expiry",
            "leased_until",
            postgresql_where=text("status = 'delivering'"),
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
    # Fencing token of the current lease; NULL unless status = 'delivering'.
    lease_token: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    leased_by: Mapped[str | None] = mapped_column(String(MAX_WORKER_ID_LENGTH))
    last_status_code: Mapped[int | None] = mapped_column(Integer)
    last_error_class: Mapped[str | None] = mapped_column(String(MAX_ERROR_CLASS_LENGTH))

    def __repr__(self) -> str:
        return f"Delivery(id={self.id!s}, status={self.status!r})"
