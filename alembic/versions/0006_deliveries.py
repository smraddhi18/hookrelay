"""Deliveries: one row per (event, endpoint), created by the fan-out relay.

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-01

``uq_deliveries_event_endpoint`` is what makes fan-out exactly-once per (event, endpoint): the
relay inserts with ``ON CONFLICT DO NOTHING``. The partial ``ix_deliveries_due`` index is the
Milestone 3 worker queue.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deliveries",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "status", sa.String(length=16), server_default=sa.text("'pending'"), nullable=False
        ),
        sa.Column("attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("retain_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status IN ('succeeded', 'dead')) = (completed_at IS NOT NULL)",
            name=op.f("ck_deliveries_completed_iff_terminal"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'delivering', 'succeeded', 'dead')",
            name=op.f("ck_deliveries_status_valid"),
        ),
        sa.CheckConstraint(
            "attempt_count >= 0", name=op.f("ck_deliveries_attempt_count_non_negative")
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["endpoints.tenant_id", "endpoints.id"],
            name="fk_deliveries_endpoint_same_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            name="fk_deliveries_event_same_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_deliveries_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_deliveries")),
        sa.UniqueConstraint("event_id", "endpoint_id", name="uq_deliveries_event_endpoint"),
    )
    op.create_index(
        "ix_deliveries_due",
        "deliveries",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_deliveries_endpoint_created",
        "deliveries",
        ["endpoint_id", "created_at", "id"],
        unique=False,
    )
    op.create_index("ix_deliveries_retain_until", "deliveries", ["retain_until"], unique=False)
    op.create_index(
        "ix_deliveries_tenant_created",
        "deliveries",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )
    op.execute(
        """
        CREATE TRIGGER trg_deliveries_updated_at
        BEFORE UPDATE ON deliveries
        FOR EACH ROW EXECUTE FUNCTION hookrelay_set_updated_at()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_deliveries_updated_at ON deliveries")
    op.drop_index("ix_deliveries_tenant_created", table_name="deliveries")
    op.drop_index("ix_deliveries_retain_until", table_name="deliveries")
    op.drop_index("ix_deliveries_endpoint_created", table_name="deliveries")
    op.drop_index(
        "ix_deliveries_due", table_name="deliveries", postgresql_where=sa.text("status = 'pending'")
    )
    op.drop_table("deliveries")
