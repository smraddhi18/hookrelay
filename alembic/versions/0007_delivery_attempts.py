"""Delivery workers: lease columns on deliveries, the ``failed`` state and delivery_attempts.

Revision ID: 0007
Revises: 0006
Create Date: 2026-11-02

* ``deliveries`` gains the lease (``lease_token`` fencing token, ``leased_until``,
  ``leased_by``) and the last result (``last_status_code``, ``last_error_class``). The
  ``lease_iff_delivering`` CHECK makes "delivering without an owner" unrepresentable.
* ``failed`` becomes a terminal status (``dead`` stays reserved for the Milestone 4 DLQ).
* ``delivery_attempts`` is the append-only attempt log, tied to its delivery by a same-tenant
  composite FK.

Downgrade maps ``failed`` to ``dead`` and returns in-flight rows to ``pending`` so the 0006
constraints hold again.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LEASE_CHECK = (
    "(status = 'delivering') = (lease_token IS NOT NULL) "
    "AND (lease_token IS NULL) = (leased_until IS NULL) "
    "AND (lease_token IS NULL) = (leased_by IS NULL)"
)


def upgrade() -> None:
    op.add_column("deliveries", sa.Column("lease_token", postgresql.UUID(as_uuid=True)))
    op.add_column("deliveries", sa.Column("leased_until", sa.DateTime(timezone=True)))
    op.add_column("deliveries", sa.Column("leased_by", sa.String(length=128)))
    op.add_column("deliveries", sa.Column("last_status_code", sa.Integer()))
    op.add_column("deliveries", sa.Column("last_error_class", sa.String(length=32)))

    # Rows left 'delivering' by nothing (no worker existed before 0007) go back to the queue.
    op.execute("UPDATE deliveries SET status = 'pending' WHERE status = 'delivering'")

    op.drop_constraint(op.f("ck_deliveries_status_valid"), "deliveries", type_="check")
    op.drop_constraint(op.f("ck_deliveries_completed_iff_terminal"), "deliveries", type_="check")
    op.create_check_constraint(
        op.f("ck_deliveries_status_valid"),
        "deliveries",
        "status IN ('pending', 'delivering', 'succeeded', 'failed', 'dead')",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_completed_iff_terminal"),
        "deliveries",
        "(status IN ('dead', 'failed', 'succeeded')) = (completed_at IS NOT NULL)",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_lease_iff_delivering"), "deliveries", _LEASE_CHECK
    )
    op.create_check_constraint(
        op.f("ck_deliveries_last_status_code_range"),
        "deliveries",
        "last_status_code IS NULL OR last_status_code BETWEEN 100 AND 599",
    )
    op.create_unique_constraint("uq_deliveries_tenant_id_id", "deliveries", ["tenant_id", "id"])
    op.create_index(
        "ix_deliveries_lease_expiry",
        "deliveries",
        ["leased_until"],
        unique=False,
        postgresql_where=sa.text("status = 'delivering'"),
    )

    op.create_table(
        "delivery_attempts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("delivery_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("error_class", sa.String(length=32), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("request_url", sa.Text(), nullable=False),
        sa.Column(
            "request_headers",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("response_body", sa.Text(), nullable=True),
        sa.Column(
            "response_truncated", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("worker_id", sa.String(length=128), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "outcome IN ('succeeded', 'retrying', 'failed')",
            name=op.f("ck_delivery_attempts_outcome_valid"),
        ),
        sa.CheckConstraint(
            "attempt_number >= 1", name=op.f("ck_delivery_attempts_attempt_number_positive")
        ),
        sa.CheckConstraint(
            "status_code IS NULL OR status_code BETWEEN 100 AND 599",
            name=op.f("ck_delivery_attempts_status_code_range"),
        ),
        sa.CheckConstraint(
            "latency_ms IS NULL OR latency_ms >= 0",
            name=op.f("ck_delivery_attempts_latency_non_negative"),
        ),
        sa.CheckConstraint(
            "finished_at >= started_at", name=op.f("ck_delivery_attempts_finished_after_started")
        ),
        sa.CheckConstraint(
            "(outcome = 'succeeded') = (error_class IS NULL)",
            name=op.f("ck_delivery_attempts_error_class_iff_unsuccessful"),
        ),
        sa.CheckConstraint(
            "outcome <> 'succeeded' OR COALESCE(status_code BETWEEN 200 AND 299, false)",
            name=op.f("ck_delivery_attempts_success_is_2xx"),
        ),
        sa.CheckConstraint(
            "response_body IS NULL OR octet_length(response_body) <= 65536",
            name=op.f("ck_delivery_attempts_response_body_bounded"),
        ),
        sa.CheckConstraint(
            "error_message IS NULL OR length(error_message) <= 1024",
            name=op.f("ck_delivery_attempts_error_message_bounded"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(request_headers) = 'object'",
            name=op.f("ck_delivery_attempts_request_headers_object"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "delivery_id"],
            ["deliveries.tenant_id", "deliveries.id"],
            name="fk_delivery_attempts_delivery_same_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_delivery_attempts_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_delivery_attempts")),
        sa.UniqueConstraint("delivery_id", "attempt_number", name="uq_delivery_attempts_number"),
    )
    op.create_index(
        "ix_delivery_attempts_delivery_created",
        "delivery_attempts",
        ["delivery_id", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_delivery_attempts_tenant_created",
        "delivery_attempts",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_delivery_attempts_tenant_created", table_name="delivery_attempts")
    op.drop_index("ix_delivery_attempts_delivery_created", table_name="delivery_attempts")
    op.drop_table("delivery_attempts")

    op.drop_index(
        "ix_deliveries_lease_expiry",
        table_name="deliveries",
        postgresql_where=sa.text("status = 'delivering'"),
    )
    op.drop_constraint("uq_deliveries_tenant_id_id", "deliveries", type_="unique")
    op.drop_constraint(op.f("ck_deliveries_last_status_code_range"), "deliveries", type_="check")
    op.drop_constraint(op.f("ck_deliveries_lease_iff_delivering"), "deliveries", type_="check")
    op.drop_constraint(op.f("ck_deliveries_completed_iff_terminal"), "deliveries", type_="check")
    op.drop_constraint(op.f("ck_deliveries_status_valid"), "deliveries", type_="check")

    op.execute("UPDATE deliveries SET status = 'dead' WHERE status = 'failed'")
    op.execute("UPDATE deliveries SET status = 'pending' WHERE status = 'delivering'")

    op.create_check_constraint(
        op.f("ck_deliveries_status_valid"),
        "deliveries",
        "status IN ('pending', 'delivering', 'succeeded', 'dead')",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_completed_iff_terminal"),
        "deliveries",
        "(status IN ('succeeded', 'dead')) = (completed_at IS NOT NULL)",
    )
    op.drop_column("deliveries", "last_error_class")
    op.drop_column("deliveries", "last_status_code")
    op.drop_column("deliveries", "leased_by")
    op.drop_column("deliveries", "leased_until")
    op.drop_column("deliveries", "lease_token")
