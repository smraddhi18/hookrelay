"""Per-endpoint delivery state: circuit breaker and rate-limit token bucket (Milestone 4).

One row per endpoint, created by an ``AFTER INSERT`` trigger on ``endpoints`` (so no code path
can create an endpoint without one) and removed with it by the cascading FK. The row is the
coordination point between workers: the lease query locks it (``FOR UPDATE SKIP LOCKED``) while
it decides how many of the endpoint's deliveries may start, and the completion path locks it to
record an outcome. Keeping this out of ``endpoints`` keeps high-churn worker writes off the
tenant-owned, ETag-versioned configuration row.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Double,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from hookrelay.db.base import Base, TimestampMixin


class BreakerState(StrEnum):
    CLOSED = "closed"  # deliveries flow (subject to the in-flight cap and rate limit)
    OPEN = "open"  # nothing is leased until open_until
    HALF_OPEN = "half_open"  # exactly one probe delivery is in flight


_STATES = ", ".join(f"'{s.value}'" for s in BreakerState)


class EndpointState(TimestampMixin, Base):
    __tablename__ = "endpoint_states"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["endpoints.tenant_id", "endpoints.id"],
            ondelete="CASCADE",
            name="fk_endpoint_states_endpoint_same_tenant",
        ),
        CheckConstraint(f"breaker_state IN ({_STATES})", name="breaker_state_valid"),
        CheckConstraint("consecutive_failures >= 0", name="consecutive_failures_non_negative"),
        CheckConstraint("trips >= 0", name="trips_non_negative"),
        # open_until is the cooldown deadline, so it exists exactly while the breaker is open.
        CheckConstraint(
            "(breaker_state = 'open') = (open_until IS NOT NULL)", name="open_until_iff_open"
        ),
        CheckConstraint(
            "(consecutive_failures = 0) = (failing_since IS NULL)",
            name="failing_since_iff_failures",
        ),
        CheckConstraint("rate_tokens IS NULL OR rate_tokens >= 0", name="rate_tokens_non_negative"),
    )
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    endpoint_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    breaker_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default=text("'closed'")
    )
    consecutive_failures: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default=text("0")
    )
    # Start of the current unbroken failure streak (drives auto-disable).
    failing_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    open_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Times the breaker opened since it was last closed (drives the exponential cooldown).
    trips: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Token bucket (NULL = full / no limit), refilled lazily at lease time.
    rate_tokens: Mapped[float | None] = mapped_column(Double)
    rate_refilled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Fairness: the lease query serves least-recently-leased endpoints first.
    last_leased_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"EndpointState(endpoint_id={self.endpoint_id!s}, state={self.breaker_state!r})"
