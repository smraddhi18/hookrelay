"""Event type registry and endpoint subscriptions (exact + wildcard patterns).

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-31

Subscriptions reference endpoints and event types through composite (tenant_id, id) foreign
keys, so Postgres itself guarantees both sides of a link belong to the same tenant.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SEGMENT = r"[a-z0-9][a-z0-9_-]*"
EVENT_TYPE_PATTERN = rf"^{SEGMENT}(\.{SEGMENT}){{0,7}}$"
SUBSCRIPTION_PATTERN = rf"^(\*|{SEGMENT}(\.{SEGMENT}){{0,7}}(\.\*)?)$"


def upgrade() -> None:
    op.create_table(
        "event_types",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
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
            f"name ~ '{EVENT_TYPE_PATTERN}'", name=op.f("ck_event_types_name_format")
        ),
        sa.CheckConstraint("version >= 1", name=op.f("ck_event_types_version_positive")),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_event_types_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_event_types")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_event_types_tenant_id_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_event_types_tenant_id_name"),
    )
    op.create_index(
        "ix_event_types_tenant_created",
        "event_types",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )
    op.execute(
        """
        CREATE TRIGGER trg_event_types_updated_at
        BEFORE UPDATE ON event_types
        FOR EACH ROW EXECUTE FUNCTION hookrelay_set_updated_at()
        """
    )

    op.create_table(
        "subscriptions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type_pattern", sa.String(length=130), nullable=False),
        sa.Column("event_type_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            f"event_type_pattern ~ '{SUBSCRIPTION_PATTERN}'",
            name=op.f("ck_subscriptions_pattern_format"),
        ),
        sa.CheckConstraint(
            "(right(event_type_pattern, 1) = '*') = (event_type_id IS NULL)",
            name=op.f("ck_subscriptions_exact_pattern_has_event_type"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["endpoints.tenant_id", "endpoints.id"],
            name="fk_subscriptions_endpoint_same_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_type_id"],
            ["event_types.tenant_id", "event_types.id"],
            name="fk_subscriptions_event_type_same_tenant",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_subscriptions_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_subscriptions")),
        sa.UniqueConstraint(
            "endpoint_id", "event_type_pattern", name="uq_subscriptions_endpoint_pattern"
        ),
    )
    op.create_index(
        "ix_subscriptions_tenant_pattern",
        "subscriptions",
        ["tenant_id", "event_type_pattern"],
        unique=False,
    )
    op.create_index(
        "ix_subscriptions_endpoint_created",
        "subscriptions",
        ["endpoint_id", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_subscriptions_event_type_id",
        "subscriptions",
        ["event_type_id"],
        unique=False,
        postgresql_where=sa.text("event_type_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_subscriptions_event_type_id", table_name="subscriptions")
    op.drop_index("ix_subscriptions_endpoint_created", table_name="subscriptions")
    op.drop_index("ix_subscriptions_tenant_pattern", table_name="subscriptions")
    op.drop_table("subscriptions")
    op.execute("DROP TRIGGER IF EXISTS trg_event_types_updated_at ON event_types")
    op.drop_index("ix_event_types_tenant_created", table_name="event_types")
    op.drop_table("event_types")
