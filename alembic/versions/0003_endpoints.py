"""Endpoints: target URL, custom headers, timeout, encrypted signing secret, soft delete.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "endpoints",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("description", sa.String(length=500), nullable=True),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column(
            "headers",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("timeout_ms", sa.Integer(), server_default=sa.text("10000"), nullable=False),
        sa.Column("secret_ciphertext", sa.Text(), nullable=False),
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
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
            "url ~ '^https?://' AND length(url) <= 2048", name=op.f("ck_endpoints_url_format")
        ),
        sa.CheckConstraint(
            "timeout_ms BETWEEN 1000 AND 60000", name=op.f("ck_endpoints_timeout_range")
        ),
        sa.CheckConstraint(
            "jsonb_typeof(headers) = 'object'", name=op.f("ck_endpoints_headers_object")
        ),
        sa.CheckConstraint("version >= 1", name=op.f("ck_endpoints_version_positive")),
        sa.CheckConstraint(
            "deleted_at IS NULL OR NOT enabled", name=op.f("ck_endpoints_deleted_is_disabled")
        ),
        sa.CheckConstraint(
            "secret_ciphertext LIKE 'v1.%'", name=op.f("ck_endpoints_secret_encrypted")
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_endpoints_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_endpoints")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_endpoints_tenant_id_id"),
    )
    op.create_index(
        "uq_endpoints_tenant_url_live",
        "endpoints",
        ["tenant_id", "url"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.create_index(
        "ix_endpoints_tenant_created_live",
        "endpoints",
        ["tenant_id", "created_at", "id"],
        unique=False,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )
    op.execute(
        """
        CREATE TRIGGER trg_endpoints_updated_at
        BEFORE UPDATE ON endpoints
        FOR EACH ROW EXECUTE FUNCTION hookrelay_set_updated_at()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_endpoints_updated_at ON endpoints")
    op.drop_index("ix_endpoints_tenant_created_live", table_name="endpoints")
    op.drop_index("uq_endpoints_tenant_url_live", table_name="endpoints")
    op.drop_table("endpoints")
