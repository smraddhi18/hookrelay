"""Event ingestion: events, the transactional outbox and idempotency keys.

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-01

``POST /v1/events`` writes an ``events`` row, its ``event_outbox`` row and (with an
``Idempotency-Key``) the stored response in one transaction. The outbox's partial index on
unprocessed rows keeps the relay's claim query proportional to the backlog, not the history.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("external_id", sa.String(length=128), nullable=True),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("payload_size", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=True),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("retain_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fanned_out_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "external_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'",
            name=op.f("ck_events_external_id_format"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name=op.f("ck_events_payload_object")
        ),
        sa.CheckConstraint("payload_size >= 2", name=op.f("ck_events_payload_size_positive")),
        sa.CheckConstraint(
            "retain_until > created_at", name=op.f("ck_events_retain_after_created")
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_type"],
            ["event_types.tenant_id", "event_types.name"],
            name="fk_events_event_type_same_tenant",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_events_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_events")),
        sa.UniqueConstraint("tenant_id", "id", name="uq_events_tenant_id_id"),
    )
    op.create_index("ix_events_retain_until", "events", ["retain_until"], unique=False)
    op.create_index(
        "ix_events_tenant_created", "events", ["tenant_id", "created_at", "id"], unique=False
    )
    op.create_index(
        "ix_events_tenant_type_created",
        "events",
        ["tenant_id", "event_type", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "uq_events_tenant_external_id",
        "events",
        ["tenant_id", "external_id"],
        unique=True,
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )
    op.create_table(
        "event_outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_event_outbox_attempts_non_negative")),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            name="fk_event_outbox_event_same_tenant",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_event_outbox")),
        sa.UniqueConstraint("event_id", name="uq_event_outbox_event_id"),
    )
    op.create_index(
        "ix_event_outbox_pending",
        "event_outbox",
        ["available_at", "id"],
        unique=False,
        postgresql_where=sa.text("processed_at IS NULL"),
    )
    op.create_index(
        "ix_event_outbox_processed",
        "event_outbox",
        ["processed_at"],
        unique=False,
        postgresql_where=sa.text("processed_at IS NOT NULL"),
    )
    op.create_table(
        "idempotency_keys",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("request_method", sa.String(length=10), nullable=False),
        sa.Column("request_path", sa.String(length=255), nullable=False),
        sa.Column("fingerprint", sa.LargeBinary(), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("response_status", sa.SmallInteger(), nullable=True),
        sa.Column("response_body", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "key ~ '^[\\x21-\\x7e]+$'", name=op.f("ck_idempotency_keys_key_printable_ascii")
        ),
        sa.CheckConstraint(
            "(response_status IS NULL) = (response_body IS NULL)",
            name=op.f("ck_idempotency_keys_response_complete"),
        ),
        sa.CheckConstraint(
            "expires_at > created_at", name=op.f("ck_idempotency_keys_expires_after_created")
        ),
        sa.CheckConstraint(
            "octet_length(fingerprint) = 32", name=op.f("ck_idempotency_keys_fingerprint_sha256")
        ),
        sa.CheckConstraint(
            "response_status IS NULL OR response_status BETWEEN 200 AND 299",
            name=op.f("ck_idempotency_keys_response_status_success"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["events.tenant_id", "events.id"],
            name="fk_idempotency_keys_event_same_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_idempotency_keys_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "key", name="pk_idempotency_keys"),
    )
    op.create_index("ix_idempotency_keys_event_id", "idempotency_keys", ["event_id"], unique=False)
    op.create_index(
        "ix_idempotency_keys_expires_at", "idempotency_keys", ["expires_at"], unique=False
    )


def downgrade() -> None:
    op.drop_index("ix_idempotency_keys_expires_at", table_name="idempotency_keys")
    op.drop_index("ix_idempotency_keys_event_id", table_name="idempotency_keys")
    op.drop_table("idempotency_keys")
    op.drop_index(
        "ix_event_outbox_processed",
        table_name="event_outbox",
        postgresql_where=sa.text("processed_at IS NOT NULL"),
    )
    op.drop_index(
        "ix_event_outbox_pending",
        table_name="event_outbox",
        postgresql_where=sa.text("processed_at IS NULL"),
    )
    op.drop_table("event_outbox")
    op.drop_index(
        "uq_events_tenant_external_id",
        table_name="events",
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )
    op.drop_index("ix_events_tenant_type_created", table_name="events")
    op.drop_index("ix_events_tenant_created", table_name="events")
    op.drop_index("ix_events_retain_until", table_name="events")
    op.drop_table("events")
