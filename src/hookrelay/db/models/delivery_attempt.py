"""Delivery attempts: an append-only log of every HTTP attempt a worker made (or lost).

One row per attempt, written in the same transaction as the fenced state transition on the
parent delivery, so ``deliveries.attempt_count`` always equals the number of attempt rows once
the delivery is not in flight. Rows hold what an operator needs to debug a receiver (status,
latency, a truncated response body, an error class) and nothing secret: the signature header
and custom header values are redacted before they are stored.
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
    ForeignKeyConstraint,
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
from hookrelay.db.models.delivery import MAX_ERROR_CLASS_LENGTH, MAX_WORKER_ID_LENGTH

MAX_ATTEMPT_ERROR_LENGTH = 1_024
MAX_ATTEMPT_RESPONSE_BYTES = 65_536


class AttemptOutcome(StrEnum):
    SUCCEEDED = "succeeded"  # 2xx: the delivery is done
    RETRYING = "retrying"  # retryable failure, another attempt is scheduled
    FAILED = "failed"  # terminal failure, or retries / max age exhausted


_OUTCOMES = ", ".join(f"'{o.value}'" for o in AttemptOutcome)


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "delivery_id"],
            ["deliveries.tenant_id", "deliveries.id"],
            ondelete="CASCADE",
            name="fk_delivery_attempts_delivery_same_tenant",
        ),
        UniqueConstraint("delivery_id", "attempt_number", name="uq_delivery_attempts_number"),
        CheckConstraint(f"outcome IN ({_OUTCOMES})", name="outcome_valid"),
        CheckConstraint("attempt_number >= 1", name="attempt_number_positive"),
        CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599", name="status_code_range"
        ),
        CheckConstraint("latency_ms IS NULL OR latency_ms >= 0", name="latency_non_negative"),
        CheckConstraint("finished_at >= started_at", name="finished_after_started"),
        # Success is a 2xx and has no error class; every non-success names its error class.
        CheckConstraint(
            "(outcome = 'succeeded') = (error_class IS NULL)", name="error_class_iff_unsuccessful"
        ),
        CheckConstraint(
            "outcome <> 'succeeded' OR COALESCE(status_code BETWEEN 200 AND 299, false)",
            name="success_is_2xx",
        ),
        CheckConstraint(
            f"response_body IS NULL OR octet_length(response_body) <= {MAX_ATTEMPT_RESPONSE_BYTES}",
            name="response_body_bounded",
        ),
        CheckConstraint(
            f"error_message IS NULL OR length(error_message) <= {MAX_ATTEMPT_ERROR_LENGTH}",
            name="error_message_bounded",
        ),
        CheckConstraint("jsonb_typeof(request_headers) = 'object'", name="request_headers_object"),
        # GET /v1/deliveries/{id}/attempts: keyset pages within one delivery.
        Index("ix_delivery_attempts_delivery_created", "delivery_id", "created_at", "id"),
        Index("ix_delivery_attempts_tenant_created", "tenant_id", "created_at", "id"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    delivery_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    status_code: Mapped[int | None] = mapped_column(Integer)
    error_class: Mapped[str | None] = mapped_column(String(MAX_ERROR_CLASS_LENGTH))
    error_message: Mapped[str | None] = mapped_column(Text)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    request_url: Mapped[str] = mapped_column(Text, nullable=False)
    request_headers: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    response_body: Mapped[str | None] = mapped_column(Text)
    response_truncated: Mapped[bool] = mapped_column(nullable=False, server_default=text("false"))
    worker_id: Mapped[str] = mapped_column(String(MAX_WORKER_ID_LENGTH), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"DeliveryAttempt(delivery_id={self.delivery_id!s}, "
            f"attempt_number={self.attempt_number}, outcome={self.outcome!r})"
        )
