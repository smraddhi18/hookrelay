"""Operability for tenants: secret rotation, delivery search, endpoint health, payload retention.

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-04

* ``endpoints`` gains ``previous_secret_ciphertext`` / ``previous_secret_expires_at`` (the
  dual-signing overlap window after a rotation; both set or both NULL) and
  ``secret_rotated_at``. The previous ciphertext is moved, not re-encrypted: it stays bound to
  the same tenant+endpoint AAD.
* ``deliveries`` gains ``event_type`` (denormalised from the immutable event by a ``BEFORE
  INSERT`` trigger, so writers cannot get it wrong) and two search indexes:
  ``(tenant_id, status, created_at, id)`` and ``(tenant_id, event_type, created_at, id)``.
* ``delivery_attempts`` gains ``endpoint_id`` (same trigger approach, from the parent delivery,
  with a same-tenant FK) and two indexes for the endpoint health summary: a covering
  ``(endpoint_id, created_at) INCLUDE (outcome, latency_ms, error_class)`` index and a partial
  index over unsuccessful attempts for "last error".
* ``events.payload`` becomes nullable and ``payload_purged_at`` records when the payload
  retention job dropped it (``payload IS NULL`` exactly when ``payload_purged_at`` is set),
  with a partial index over unpurged events for the purge scan.

Backfills are single statements: fine at this project's scale. A large deployment would batch
them and build the indexes ``CONCURRENTLY`` in a separate, non-transactional migration.
Downgrade restores purged payloads as ``{}`` (the data is gone; ``NOT NULL`` must hold again)
and drops everything else.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # endpoints: secret rotation with an overlap window.
    op.add_column("endpoints", sa.Column("previous_secret_ciphertext", sa.Text()))
    op.add_column("endpoints", sa.Column("previous_secret_expires_at", sa.DateTime(timezone=True)))
    op.add_column("endpoints", sa.Column("secret_rotated_at", sa.DateTime(timezone=True)))
    op.create_check_constraint(
        op.f("ck_endpoints_previous_secret_encrypted"),
        "endpoints",
        "previous_secret_ciphertext IS NULL OR previous_secret_ciphertext LIKE 'v1.%'",
    )
    op.create_check_constraint(
        op.f("ck_endpoints_previous_secret_iff_expiry"),
        "endpoints",
        "(previous_secret_ciphertext IS NULL) = (previous_secret_expires_at IS NULL)",
    )
    op.create_index(
        "ix_endpoints_previous_secret_expiry",
        "endpoints",
        ["previous_secret_expires_at"],
        unique=False,
        postgresql_where=sa.text("previous_secret_expires_at IS NOT NULL"),
    )

    # deliveries.event_type: always derived from the event.
    op.add_column("deliveries", sa.Column("event_type", sa.String(length=128)))
    op.execute(
        """
        UPDATE deliveries AS d SET event_type = ev.event_type
        FROM events AS ev
        WHERE ev.tenant_id = d.tenant_id AND ev.id = d.event_id
        """
    )
    op.alter_column("deliveries", "event_type", nullable=False)
    op.execute(
        """
        CREATE OR REPLACE FUNCTION hookrelay_delivery_event_type() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            found_type text;
        BEGIN
            -- Looked up by primary key only: a cross-tenant or unknown event_id is left to the
            -- composite FK to reject (with its own constraint name); the '' placeholder only
            -- gets past NOT NULL so that the FK check runs, and never commits.
            SELECT ev.event_type INTO found_type FROM events AS ev WHERE ev.id = NEW.event_id;
            NEW.event_type := COALESCE(found_type, '');
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_deliveries_event_type
        BEFORE INSERT ON deliveries
        FOR EACH ROW EXECUTE FUNCTION hookrelay_delivery_event_type()
        """
    )
    op.create_index(
        "ix_deliveries_tenant_status_created",
        "deliveries",
        ["tenant_id", "status", "created_at", "id"],
        unique=False,
    )
    op.create_index(
        "ix_deliveries_tenant_type_created",
        "deliveries",
        ["tenant_id", "event_type", "created_at", "id"],
        unique=False,
    )

    # delivery_attempts.endpoint_id: always derived from the parent delivery.
    op.add_column("delivery_attempts", sa.Column("endpoint_id", postgresql.UUID(as_uuid=True)))
    op.execute(
        """
        UPDATE delivery_attempts AS a SET endpoint_id = d.endpoint_id
        FROM deliveries AS d
        WHERE d.tenant_id = a.tenant_id AND d.id = a.delivery_id
        """
    )
    op.alter_column("delivery_attempts", "endpoint_id", nullable=False)
    op.create_foreign_key(
        "fk_delivery_attempts_endpoint_same_tenant",
        "delivery_attempts",
        "endpoints",
        ["tenant_id", "endpoint_id"],
        ["tenant_id", "id"],
        ondelete="CASCADE",
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION hookrelay_attempt_endpoint() RETURNS trigger
        LANGUAGE plpgsql AS $$
        DECLARE
            found_endpoint uuid;
        BEGIN
            -- As for deliveries.event_type: an unknown or cross-tenant delivery_id is rejected
            -- by the (older, so first-firing) delivery FK; the nil UUID never commits.
            SELECT d.endpoint_id INTO found_endpoint FROM deliveries AS d
            WHERE d.id = NEW.delivery_id;
            NEW.endpoint_id := COALESCE(found_endpoint, '00000000-0000-0000-0000-000000000000');
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_delivery_attempts_endpoint
        BEFORE INSERT ON delivery_attempts
        FOR EACH ROW EXECUTE FUNCTION hookrelay_attempt_endpoint()
        """
    )
    op.create_index(
        "ix_delivery_attempts_endpoint_created",
        "delivery_attempts",
        ["endpoint_id", "created_at"],
        unique=False,
        postgresql_include=["outcome", "latency_ms", "error_class"],
    )
    op.create_index(
        "ix_delivery_attempts_endpoint_errors",
        "delivery_attempts",
        ["endpoint_id", "created_at"],
        unique=False,
        postgresql_where=sa.text("outcome <> 'succeeded'"),
    )

    # events: payload retention.
    op.add_column("events", sa.Column("payload_purged_at", sa.DateTime(timezone=True)))
    op.alter_column("events", "payload", nullable=True)
    op.drop_constraint(op.f("ck_events_payload_object"), "events", type_="check")
    op.create_check_constraint(
        op.f("ck_events_payload_object"),
        "events",
        "payload IS NULL OR jsonb_typeof(payload) = 'object'",
    )
    op.create_check_constraint(
        op.f("ck_events_payload_iff_not_purged"),
        "events",
        "(payload IS NULL) = (payload_purged_at IS NOT NULL)",
    )
    op.create_index(
        "ix_events_payload_unpurged",
        "events",
        ["created_at", "id"],
        unique=False,
        postgresql_where=sa.text("payload_purged_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_events_payload_unpurged", table_name="events")
    op.drop_constraint(op.f("ck_events_payload_iff_not_purged"), "events", type_="check")
    op.drop_constraint(op.f("ck_events_payload_object"), "events", type_="check")
    # The purged data is gone; an empty object keeps 0008's NOT NULL + object constraints.
    op.execute("UPDATE events SET payload = '{}'::jsonb WHERE payload IS NULL")
    op.alter_column("events", "payload", nullable=False)
    op.create_check_constraint(
        op.f("ck_events_payload_object"), "events", "jsonb_typeof(payload) = 'object'"
    )
    op.drop_column("events", "payload_purged_at")

    op.drop_index("ix_delivery_attempts_endpoint_errors", table_name="delivery_attempts")
    op.drop_index("ix_delivery_attempts_endpoint_created", table_name="delivery_attempts")
    op.execute("DROP TRIGGER IF EXISTS trg_delivery_attempts_endpoint ON delivery_attempts")
    op.execute("DROP FUNCTION IF EXISTS hookrelay_attempt_endpoint()")
    op.drop_constraint(
        "fk_delivery_attempts_endpoint_same_tenant", "delivery_attempts", type_="foreignkey"
    )
    op.drop_column("delivery_attempts", "endpoint_id")

    op.drop_index("ix_deliveries_tenant_type_created", table_name="deliveries")
    op.drop_index("ix_deliveries_tenant_status_created", table_name="deliveries")
    op.execute("DROP TRIGGER IF EXISTS trg_deliveries_event_type ON deliveries")
    op.execute("DROP FUNCTION IF EXISTS hookrelay_delivery_event_type()")
    op.drop_column("deliveries", "event_type")

    op.drop_index("ix_endpoints_previous_secret_expiry", table_name="endpoints")
    op.drop_constraint(op.f("ck_endpoints_previous_secret_iff_expiry"), "endpoints", type_="check")
    op.drop_constraint(op.f("ck_endpoints_previous_secret_encrypted"), "endpoints", type_="check")
    op.drop_column("endpoints", "secret_rotated_at")
    op.drop_column("endpoints", "previous_secret_expires_at")
    op.drop_column("endpoints", "previous_secret_ciphertext")
