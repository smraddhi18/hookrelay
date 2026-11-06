"""Observability: persist W3C trace context on events for cross-process spans.

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-06

Adds nullable ``traceparent`` / ``tracestate`` on ``events`` so the ingest span can continue
through the fan-out relay and delivery worker (separate processes) and be injected on outbound
webhooks. No new tables; dashboard metrics are computed from existing attempt indexes.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("events", sa.Column("traceparent", sa.String(length=55)))
    op.add_column("events", sa.Column("tracestate", sa.String(length=512)))


def downgrade() -> None:
    op.drop_column("events", "tracestate")
    op.drop_column("events", "traceparent")
