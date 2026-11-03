"""Replay requests: the audit trail of every replay, and the replay rate limiter's ledger.

Each accepted replay (one delivery, one event, or a filtered bulk replay) writes one row in the
same transaction that requeues the deliveries. Counting a tenant's rows in the last minute,
under a per-tenant advisory lock, is the Postgres-only rate limit for the replay API.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base


class ReplayScope(StrEnum):
    DELIVERY = "delivery"
    EVENT = "event"
    FILTER = "filter"


_SCOPES = ", ".join(f"'{s.value}'" for s in ReplayScope)


class ReplayRequest(Base):
    __tablename__ = "replay_requests"
    __table_args__ = (
        CheckConstraint(f"scope IN ({_SCOPES})", name="scope_valid"),
        CheckConstraint("replayed >= 0", name="replayed_non_negative"),
        CheckConstraint("jsonb_typeof(filters) = 'object'", name="filters_object"),
        Index("ix_replay_requests_tenant_created", "tenant_id", "created_at", "id"),
        Index("ix_replay_requests_created", "created_at"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    # Who asked (not a FK: the audit row outlives a revoked or deleted key).
    api_key_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    filters: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    replayed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    has_more: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return f"ReplayRequest(id={self.id!s}, scope={self.scope!r}, replayed={self.replayed})"
