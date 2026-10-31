"""Tenant API keys: hashed secrets, prefix lookup, roles, rotation lineage, revocation.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-31
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "api_keys",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("role", sa.String(length=16), nullable=False),
        sa.Column("prefix", sa.String(length=12), nullable=False),
        sa.Column("key_hash", sa.LargeBinary(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rotated_from_id", postgresql.UUID(as_uuid=True), nullable=True),
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
            "role IN ('admin', 'publisher', 'reader')", name=op.f("ck_api_keys_role_valid")
        ),
        sa.CheckConstraint("prefix ~ '^[a-z0-9]{12}$'", name=op.f("ck_api_keys_prefix_format")),
        sa.CheckConstraint("octet_length(key_hash) = 32", name=op.f("ck_api_keys_key_hash_sha256")),
        sa.CheckConstraint(
            "length(btrim(name)) BETWEEN 1 AND 100", name=op.f("ck_api_keys_name_length")
        ),
        sa.CheckConstraint(
            "expires_at IS NULL OR expires_at > created_at",
            name=op.f("ck_api_keys_expiry_after_creation"),
        ),
        sa.CheckConstraint(
            "rotated_from_id IS NULL OR rotated_from_id <> id",
            name=op.f("ck_api_keys_not_self_rotated"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_api_keys_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["rotated_from_id"],
            ["api_keys.id"],
            name=op.f("fk_api_keys_rotated_from_id_api_keys"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_api_keys")),
        sa.UniqueConstraint("prefix", name=op.f("uq_api_keys_prefix")),
    )
    op.create_index(
        "ix_api_keys_tenant_created", "api_keys", ["tenant_id", "created_at", "id"], unique=False
    )
    op.create_index(
        "uq_api_keys_rotated_from_id",
        "api_keys",
        ["rotated_from_id"],
        unique=True,
        postgresql_where=sa.text("rotated_from_id IS NOT NULL"),
    )
    op.execute(
        """
        CREATE TRIGGER trg_api_keys_updated_at
        BEFORE UPDATE ON api_keys
        FOR EACH ROW EXECUTE FUNCTION hookrelay_set_updated_at()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_api_keys_updated_at ON api_keys")
    op.drop_index("uq_api_keys_rotated_from_id", table_name="api_keys")
    op.drop_index("ix_api_keys_tenant_created", table_name="api_keys")
    op.drop_table("api_keys")
