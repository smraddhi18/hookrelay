"""Ingested events and their transactional outbox rows.

An event row and its outbox row are written in the same transaction as the (optional)
idempotency record, so "accepted" always means "durably queued for fan-out" and there is no
dual write to a broker that could half-succeed.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base
from hookrelay.db.models.event_type import MAX_EVENT_TYPE_LENGTH

# Client-supplied event identifiers (``evt_123``, ``order:42:paid``): URL/header safe.
EXTERNAL_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$"
MAX_EXTERNAL_ID_LENGTH = 128
MAX_IDEMPOTENCY_KEY_LENGTH = 255


class Event(Base):
    """Immutable after insert except ``fanned_out_at`` (set once by the relay)."""

    __tablename__ = "events"
    __table_args__ = (
        # Target of the composite (tenant_id, event_id) FKs from event_outbox and deliveries.
        UniqueConstraint("tenant_id", "id", name="uq_events_tenant_id_id"),
        # The event type must be registered for this tenant. NO ACTION (not RESTRICT) so that
        # deleting a tenant, which cascades to both tables, is checked at statement end.
        ForeignKeyConstraint(
            ["tenant_id", "event_type"],
            ["event_types.tenant_id", "event_types.name"],
            name="fk_events_event_type_same_tenant",
        ),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="payload_object"),
        CheckConstraint("payload_size >= 2", name="payload_size_positive"),
        CheckConstraint(f"external_id ~ '{EXTERNAL_ID_PATTERN}'", name="external_id_format"),
        CheckConstraint("retain_until > created_at", name="retain_after_created"),
        # A client-supplied id is unique per tenant: a retried publish cannot create a twin.
        Index(
            "uq_events_tenant_external_id",
            "tenant_id",
            "external_id",
            unique=True,
            postgresql_where=text("external_id IS NOT NULL"),
        ),
        Index("ix_events_tenant_created", "tenant_id", "created_at", "id"),
        Index("ix_events_tenant_type_created", "tenant_id", "event_type", "created_at", "id"),
        # Retention sweeps (and later time partitioning) walk this index.
        Index("ix_events_retain_until", "retain_until"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(MAX_EVENT_TYPE_LENGTH), nullable=False)
    external_id: Mapped[str | None] = mapped_column(String(MAX_EXTERNAL_ID_LENGTH))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    payload_size: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_key: Mapped[str | None] = mapped_column(String(MAX_IDEMPOTENCY_KEY_LENGTH))
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    retain_until: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fanned_out_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"Event(id={self.id!s}, event_type={self.event_type!r})"


class OutboxEntry(Base):
    """One row per event, consumed by the fan-out relay (``FOR UPDATE SKIP LOCKED``)."""

    __tablename__ = "event_outbox"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            ondelete="CASCADE",
            name="fk_event_outbox_event_same_tenant",
        ),
        UniqueConstraint("event_id", name="uq_event_outbox_event_id"),
        CheckConstraint("attempts >= 0", name="attempts_non_negative"),
        # The relay's claim query: oldest due, unprocessed rows. Processed rows drop out of the
        # index, so it stays as small as the backlog no matter how much history is kept.
        Index(
            "ix_event_outbox_pending",
            "available_at",
            "id",
            postgresql_where=text("processed_at IS NULL"),
        ),
        Index(
            "ix_event_outbox_processed",
            "processed_at",
            postgresql_where=text("processed_at IS NOT NULL"),
        ),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_error: Mapped[str | None] = mapped_column(Text)

    def __repr__(self) -> str:
        return f"OutboxEntry(id={self.id}, event_id={self.event_id!s})"
