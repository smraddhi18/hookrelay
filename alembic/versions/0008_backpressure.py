"""Back-pressure and failure isolation: endpoint limits, breaker state, DLQ, replays, notices.

Revision ID: 0008
Revises: 0007
Create Date: 2026-11-03

* ``endpoints`` gains ``rate_limit_per_s`` and ``max_in_flight`` (NULL = unlimited / deployment
  default) and ``auto_disabled_at`` (set when the breaker disabled it).
* ``endpoint_states`` holds the per-endpoint circuit breaker and token bucket. Existing
  endpoints are backfilled and an ``AFTER INSERT`` trigger on ``endpoints`` creates the row for
  every new one, so the lease query can rely on it with an inner join.
* ``deliveries`` gains ``attempt_offset`` / ``run_started_at`` (a replay starts a fresh retry
  budget) and ``replay_count``, plus two partial indexes for the per-endpoint lease.
* ``dead`` becomes the dead-letter state: existing ``failed`` rows whose last error was
  retryable (they failed by exhausting attempts or age) move to ``dead``; ``failed`` keeps
  meaning "permanent error".
* ``notifications`` (operator notification outbox) and ``replay_requests`` (replay audit and
  rate-limit ledger).

Everything is plain DDL/DML so ``alembic upgrade --sql`` works offline. Downgrade maps ``dead``
back to ``failed`` (0007's meaning of "terminal error") and drops the rest.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Error classes that are retried; a 'failed' row ending in one of these ran out of budget.
_EXHAUSTED_CLASSES = (
    "'timeout', 'connect_error', 'dns_error', 'tls_error', 'protocol_error', "
    "'secret_unavailable', 'lease_expired', 'max_age_exceeded'"
)


def _timestamps() -> list[sa.Column]:  # type: ignore[type-arg]
    return [
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
    ]


def upgrade() -> None:
    # endpoints: per-endpoint limits and the auto-disable marker.
    op.add_column("endpoints", sa.Column("rate_limit_per_s", sa.Integer()))
    op.add_column("endpoints", sa.Column("max_in_flight", sa.Integer()))
    op.add_column("endpoints", sa.Column("auto_disabled_at", sa.DateTime(timezone=True)))
    op.create_check_constraint(
        op.f("ck_endpoints_rate_limit_range"),
        "endpoints",
        "rate_limit_per_s IS NULL OR rate_limit_per_s BETWEEN 1 AND 10000",
    )
    op.create_check_constraint(
        op.f("ck_endpoints_max_in_flight_range"),
        "endpoints",
        "max_in_flight IS NULL OR max_in_flight BETWEEN 1 AND 1000",
    )
    op.create_check_constraint(
        op.f("ck_endpoints_auto_disabled_is_disabled"),
        "endpoints",
        "auto_disabled_at IS NULL OR NOT enabled",
    )

    # endpoint_states: circuit breaker + token bucket, one row per endpoint.
    op.create_table(
        "endpoint_states",
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "breaker_state",
            sa.String(length=16),
            server_default=sa.text("'closed'"),
            nullable=False,
        ),
        sa.Column(
            "consecutive_failures", sa.Integer(), server_default=sa.text("0"), nullable=False
        ),
        sa.Column("failing_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("open_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trips", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rate_tokens", sa.Double(), nullable=True),
        sa.Column("rate_refilled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_leased_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            "breaker_state IN ('closed', 'open', 'half_open')",
            name=op.f("ck_endpoint_states_breaker_state_valid"),
        ),
        sa.CheckConstraint(
            "consecutive_failures >= 0",
            name=op.f("ck_endpoint_states_consecutive_failures_non_negative"),
        ),
        sa.CheckConstraint("trips >= 0", name=op.f("ck_endpoint_states_trips_non_negative")),
        sa.CheckConstraint(
            "(breaker_state = 'open') = (open_until IS NOT NULL)",
            name=op.f("ck_endpoint_states_open_until_iff_open"),
        ),
        sa.CheckConstraint(
            "(consecutive_failures = 0) = (failing_since IS NULL)",
            name=op.f("ck_endpoint_states_failing_since_iff_failures"),
        ),
        sa.CheckConstraint(
            "rate_tokens IS NULL OR rate_tokens >= 0",
            name=op.f("ck_endpoint_states_rate_tokens_non_negative"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "endpoint_id"],
            ["endpoints.tenant_id", "endpoints.id"],
            name="fk_endpoint_states_endpoint_same_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_endpoint_states_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("endpoint_id", name=op.f("pk_endpoint_states")),
    )
    op.execute(
        """
        CREATE TRIGGER trg_endpoint_states_updated_at
        BEFORE UPDATE ON endpoint_states
        FOR EACH ROW EXECUTE FUNCTION hookrelay_set_updated_at()
        """
    )
    op.execute(
        """
        INSERT INTO endpoint_states (endpoint_id, tenant_id)
        SELECT id, tenant_id FROM endpoints
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION hookrelay_create_endpoint_state() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            INSERT INTO endpoint_states (endpoint_id, tenant_id) VALUES (NEW.id, NEW.tenant_id);
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_endpoints_create_state
        AFTER INSERT ON endpoints
        FOR EACH ROW EXECUTE FUNCTION hookrelay_create_endpoint_state()
        """
    )

    # deliveries: replay bookkeeping, per-endpoint lease indexes, the dead-letter state.
    op.add_column(
        "deliveries",
        sa.Column("attempt_offset", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    op.add_column(
        "deliveries",
        sa.Column(
            "run_started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.add_column(
        "deliveries",
        sa.Column("replay_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )
    # The first run of every existing delivery started when it was created.
    op.execute("UPDATE deliveries SET run_started_at = created_at")
    op.execute(
        f"""
        UPDATE deliveries SET status = 'dead'
        WHERE status = 'failed'
          AND (last_error_class IN ({_EXHAUSTED_CLASSES})
               OR (last_error_class = 'http_status'
                   AND (last_status_code >= 500 OR last_status_code IN (408, 409, 425, 429))))
        """  # noqa: S608 - interpolates a constant SQL fragment, never input
    )
    op.create_check_constraint(
        op.f("ck_deliveries_attempt_offset_range"),
        "deliveries",
        "attempt_offset >= 0 AND attempt_offset <= attempt_count",
    )
    op.create_check_constraint(
        op.f("ck_deliveries_replay_count_non_negative"), "deliveries", "replay_count >= 0"
    )
    op.create_index(
        "ix_deliveries_endpoint_due",
        "deliveries",
        ["endpoint_id", "next_attempt_at", "id"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_deliveries_endpoint_delivering",
        "deliveries",
        ["endpoint_id"],
        unique=False,
        postgresql_where=sa.text("status = 'delivering'"),
    )

    # notifications: operator notification outbox.
    op.create_table(
        "notifications",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "status", sa.String(length=16), server_default=sa.text("'pending'"), nullable=False
        ),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'dispatched', 'abandoned')",
            name=op.f("ck_notifications_status_valid"),
        ),
        sa.CheckConstraint("attempts >= 0", name=op.f("ck_notifications_attempts_non_negative")),
        sa.CheckConstraint(
            "(status = 'pending') = (completed_at IS NULL)",
            name=op.f("ck_notifications_completed_iff_not_pending"),
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'", name=op.f("ck_notifications_payload_object")
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_notifications_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_notifications")),
    )
    op.create_index(
        "ix_notifications_due",
        "notifications",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.create_index(
        "ix_notifications_completed",
        "notifications",
        ["completed_at"],
        unique=False,
        postgresql_where=sa.text("status <> 'pending'"),
    )
    op.create_index(
        "ix_notifications_tenant_created",
        "notifications",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )

    # replay_requests: replay audit trail and rate-limit ledger.
    op.create_table(
        "replay_requests",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("api_key_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("scope", sa.String(length=16), nullable=False),
        sa.Column(
            "filters",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("replayed", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("has_more", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "scope IN ('delivery', 'event', 'filter')", name=op.f("ck_replay_requests_scope_valid")
        ),
        sa.CheckConstraint("replayed >= 0", name=op.f("ck_replay_requests_replayed_non_negative")),
        sa.CheckConstraint(
            "jsonb_typeof(filters) = 'object'", name=op.f("ck_replay_requests_filters_object")
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name=op.f("fk_replay_requests_tenant_id_tenants"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_replay_requests")),
    )
    op.create_index(
        "ix_replay_requests_tenant_created",
        "replay_requests",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )
    op.create_index("ix_replay_requests_created", "replay_requests", ["created_at"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_replay_requests_created", table_name="replay_requests")
    op.drop_index("ix_replay_requests_tenant_created", table_name="replay_requests")
    op.drop_table("replay_requests")

    op.drop_index("ix_notifications_tenant_created", table_name="notifications")
    op.drop_index(
        "ix_notifications_completed",
        table_name="notifications",
        postgresql_where=sa.text("status <> 'pending'"),
    )
    op.drop_index(
        "ix_notifications_due",
        table_name="notifications",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.drop_table("notifications")

    op.drop_index(
        "ix_deliveries_endpoint_delivering",
        table_name="deliveries",
        postgresql_where=sa.text("status = 'delivering'"),
    )
    op.drop_index(
        "ix_deliveries_endpoint_due",
        table_name="deliveries",
        postgresql_where=sa.text("status = 'pending'"),
    )
    op.drop_constraint(op.f("ck_deliveries_replay_count_non_negative"), "deliveries", type_="check")
    op.drop_constraint(op.f("ck_deliveries_attempt_offset_range"), "deliveries", type_="check")
    # 0007 has a single terminal error state.
    op.execute("UPDATE deliveries SET status = 'failed' WHERE status = 'dead'")
    op.drop_column("deliveries", "replay_count")
    op.drop_column("deliveries", "run_started_at")
    op.drop_column("deliveries", "attempt_offset")

    op.execute("DROP TRIGGER IF EXISTS trg_endpoints_create_state ON endpoints")
    op.execute("DROP FUNCTION IF EXISTS hookrelay_create_endpoint_state()")
    op.execute("DROP TRIGGER IF EXISTS trg_endpoint_states_updated_at ON endpoint_states")
    op.drop_table("endpoint_states")

    op.drop_constraint(op.f("ck_endpoints_auto_disabled_is_disabled"), "endpoints", type_="check")
    op.drop_constraint(op.f("ck_endpoints_max_in_flight_range"), "endpoints", type_="check")
    op.drop_constraint(op.f("ck_endpoints_rate_limit_range"), "endpoints", type_="check")
    op.drop_column("endpoints", "auto_disabled_at")
    op.drop_column("endpoints", "max_in_flight")
    op.drop_column("endpoints", "rate_limit_per_s")
