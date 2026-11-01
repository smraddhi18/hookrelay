"""Stored ``Idempotency-Key`` outcomes (tenant-scoped, expiring)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    LargeBinary,
    PrimaryKeyConstraint,
    SmallInteger,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base
from hookrelay.db.models.event import MAX_IDEMPOTENCY_KEY_LENGTH

FINGERPRINT_BYTES = 32  # SHA-256


class IdempotencyKey(Base):
    """The claim row is inserted first in the request's transaction and completed (response
    stored) before commit, so other transactions only ever see completed records; a concurrent
    request with the same key blocks on the uncommitted row instead of racing it."""

    __tablename__ = "idempotency_keys"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "key", name="pk_idempotency_keys"),
        ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            ondelete="CASCADE",
            name="fk_idempotency_keys_event_same_tenant",
        ),
        CheckConstraint(
            f"octet_length(fingerprint) = {FINGERPRINT_BYTES}", name="fingerprint_sha256"
        ),
        CheckConstraint("key ~ '^[\\x21-\\x7e]+$'", name="key_printable_ascii"),
        CheckConstraint("expires_at > created_at", name="expires_after_created"),
        CheckConstraint(
            "(response_status IS NULL) = (response_body IS NULL)", name="response_complete"
        ),
        CheckConstraint(
            "response_status IS NULL OR response_status BETWEEN 200 AND 299",
            name="response_status_success",
        ),
        Index("ix_idempotency_keys_expires_at", "expires_at"),
        Index("ix_idempotency_keys_event_id", "event_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    key: Mapped[str] = mapped_column(String(MAX_IDEMPOTENCY_KEY_LENGTH), nullable=False)
    request_method: Mapped[str] = mapped_column(String(10), nullable=False)
    request_path: Mapped[str] = mapped_column(String(255), nullable=False)
    fingerprint: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    response_status: Mapped[int | None] = mapped_column(SmallInteger)
    response_body: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    def __repr__(self) -> str:
        return f"IdempotencyKey(tenant_id={self.tenant_id!s}, key={self.key!r})"
