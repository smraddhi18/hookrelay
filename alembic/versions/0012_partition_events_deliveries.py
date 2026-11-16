"""Native range partitioning of events and deliveries, attempts re-keyed to the event's day.

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-15

Milestone 9c partitioned ``delivery_attempts`` by its own ``created_at``, but expiring an event
still ran a row-by-row ``DELETE`` that cascaded into most attempt rows before the partition
``DROP`` could do the job. This revision makes the *whole event graph* drop together.

Every table of the graph is range-partitioned by UTC day on the **event's** ``created_at``:

* ``events`` by ``created_at`` (primary key ``(id, created_at)``);
* ``deliveries`` by a new ``event_created_at`` (copied from the event at fan-out);
* ``delivery_attempts`` by the same ``event_created_at`` (copied from the delivery), replacing
  the 0011 key ``created_at``;
* ``event_outbox`` stays a plain table (short-lived, claimed with ``SKIP LOCKED``) but carries
  ``event_created_at`` so its composite FK and the relay's joins prune to one partition.

The composite foreign keys carry the key (``(tenant_id, event_id, event_created_at)`` ...), so
day ``D`` of the three tables holds exactly one connected component of the graph.
Retention is then ``DROP`` of the same day's partitions, child first (``delivery_attempts``,
``deliveries``, ``events``); Postgres itself refuses to detach a referenced events / deliveries
partition while a referencing row is left, so a wrong order fails loudly instead of orphaning.

Consequences, all deliberate:

* Postgres needs every unique key of a partitioned table to contain the partition key. Ids are
  random UUIDs, so ``(id, created_at)`` is unique in practice, and the per-event invariants
  become ``(event_id, endpoint_id, event_created_at)`` (one delivery per event and endpoint)
  and ``(delivery_id, attempt_number, event_created_at)``; they are all scoped to one event, so
  one partition, so still exact.
* ``uq_events_tenant_external_id`` cannot stay a global unique index. It moves to the new plain
  table ``event_external_ids`` (``PRIMARY KEY (tenant_id, external_id)``, ``expires_at`` =
  the event's ``retain_until``), claimed in the ingest transaction with an
  ``ON CONFLICT ... WHERE expires_at <= now()`` upsert, so an external id is reusable exactly
  when its event has expired, and stays unique across days.
* ``idempotency_keys`` loses its FK to ``events`` (it would need the key too and it only
  cascaded cleanup): a record is self-contained and expires by its own ``expires_at``.
* Writers must pass ``event_created_at`` for ``deliveries`` and ``delivery_attempts`` (a BEFORE
  ROW trigger cannot fill a partition key: the row is routed first); a wrong value is rejected
  by the composite FK, and the existing BEFORE INSERT triggers derive ``event_type`` /
  ``endpoint_id`` by primary key, so their lookups prune to one partition. ``event_outbox`` is a
  plain table: a BEFORE INSERT trigger fills its ``event_created_at`` when left NULL.

Upgrade (one transaction, ``ACCESS EXCLUSIVE`` on the old tables): rename, drop the old keys,
indexes and the FKs that point at them, create the partitioned tables (``LIKE`` the old ones, so
columns and CHECKs stay identical), one partition per UTC day that has events plus today ..
today + 7 plus ``DEFAULT`` for each table, copy the rows (deliveries / attempts joined to their
parent for the key), then add keys, indexes, FKs and triggers and drop the old tables. Like
0011 this is the simple in-migration copy: it holds writes for the duration; the online path
is in docs/partitioning.md.

Downgrade restores the 0011 layout (plain ``events`` / ``deliveries``, ``delivery_attempts``
partitioned by its own ``created_at``) and fails loudly rather than drop data if two live events
ended up sharing an ``external_id`` (possible after an expired claim was reclaimed while the old
row was still waiting for its partition drop).
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PREMAKE_DAYS = 7  # matches the PARTITION_PREMAKE_DAYS default
OLD = "_m0011"
NEW = "_m0012"


def _rename_with_partitions(table: str, suffix: str) -> None:
    """Rename ``table`` and its child partitions (their names are reused by the new tables)."""
    op.execute(
        f"""
        DO $$
        DECLARE
            part record;
        BEGIN
            FOR part IN
                SELECT c.relname FROM pg_inherits AS i JOIN pg_class AS c ON c.oid = i.inhrelid
                WHERE i.inhparent = '{table}'::regclass
            LOOP
                EXECUTE format(
                    'ALTER TABLE %I RENAME TO %I', part.relname, part.relname || '{suffix}'
                );
            END LOOP;
        END
        $$
        """
    )
    op.execute(f"ALTER TABLE {table} RENAME TO {table}{suffix}")


def _drop_keys_and_indexes(table: str) -> None:
    """Drop every PK / unique constraint (CASCADE: the FKs pointing at them) and index of
    ``table``: their names are schema-global and the new tables reuse them."""
    op.execute(
        f"""
        DO $$
        DECLARE
            item record;
        BEGIN
            FOR item IN
                SELECT conname FROM pg_constraint
                WHERE conrelid = '{table}'::regclass AND contype IN ('p', 'u')
            LOOP
                EXECUTE format('ALTER TABLE {table} DROP CONSTRAINT %I CASCADE', item.conname);
            END LOOP;
            FOR item IN
                SELECT indexrelid::regclass::text AS name FROM pg_index
                WHERE indrelid = '{table}'::regclass
            LOOP
                EXECUTE format('DROP INDEX IF EXISTS %s', item.name);
            END LOOP;
        END
        $$
        """
    )


def _create_partitions(table: str, days_sql: str) -> None:
    """One partition per UTC day in ``days_sql`` and today .. today + PREMAKE_DAYS, plus
    DEFAULT. A DO block rather than Python so `alembic upgrade --sql` stays a complete script."""
    op.execute(
        f"""
        DO $$
        DECLARE
            day date;
        BEGIN
            FOR day IN
                {days_sql}
                UNION
                SELECT generate_series(
                    (now() AT TIME ZONE 'UTC')::date,
                    (now() AT TIME ZONE 'UTC')::date + {PREMAKE_DAYS},
                    interval '1 day'
                )::date
                ORDER BY 1
            LOOP
                EXECUTE format(
                    'CREATE TABLE %I PARTITION OF {table} FOR VALUES FROM (%L) TO (%L)',
                    '{table}_p' || to_char(day, 'YYYYMMDD'),
                    (day::timestamp AT TIME ZONE 'UTC'),
                    ((day + 1)::timestamp AT TIME ZONE 'UTC')
                );
            END LOOP;
        END
        $$
        """
    )
    op.execute(f"CREATE TABLE {table}_default PARTITION OF {table} DEFAULT")


def _statements(*statements: str) -> None:
    for statement in statements:
        op.execute(statement)


# --- indexes shared by both layouts ------------------------------------------------------

EVENT_INDEXES = (
    "CREATE INDEX ix_events_payload_unpurged ON events (created_at, id) "
    "WHERE payload_purged_at IS NULL",
    "CREATE INDEX ix_events_retain_until ON events (retain_until)",
    "CREATE INDEX ix_events_tenant_created ON events (tenant_id, created_at, id)",
    "CREATE INDEX ix_events_tenant_type_created ON events (tenant_id, event_type, created_at, id)",
)
EVENT_FKS = (
    "ALTER TABLE events ADD CONSTRAINT fk_events_event_type_same_tenant "
    "FOREIGN KEY (tenant_id, event_type) REFERENCES event_types (tenant_id, name)",
    "ALTER TABLE events ADD CONSTRAINT fk_events_tenant_id_tenants "
    "FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE",
)
DELIVERY_INDEXES = (
    "CREATE INDEX ix_deliveries_due ON deliveries (next_attempt_at) WHERE status = 'pending'",
    "CREATE INDEX ix_deliveries_endpoint_created ON deliveries (endpoint_id, created_at, id)",
    "CREATE INDEX ix_deliveries_endpoint_delivering ON deliveries (endpoint_id) "
    "WHERE status = 'delivering'",
    "CREATE INDEX ix_deliveries_endpoint_due ON deliveries (endpoint_id, next_attempt_at, id) "
    "WHERE status = 'pending'",
    "CREATE INDEX ix_deliveries_lease_expiry ON deliveries (leased_until) "
    "WHERE status = 'delivering'",
    "CREATE INDEX ix_deliveries_retain_until ON deliveries (retain_until)",
    "CREATE INDEX ix_deliveries_tenant_created ON deliveries (tenant_id, created_at, id)",
    "CREATE INDEX ix_deliveries_tenant_status_created "
    "ON deliveries (tenant_id, status, created_at, id)",
    "CREATE INDEX ix_deliveries_tenant_type_created "
    "ON deliveries (tenant_id, event_type, created_at, id)",
)
DELIVERY_TRIGGERS = (
    "CREATE TRIGGER trg_deliveries_event_type BEFORE INSERT ON deliveries "
    "FOR EACH ROW EXECUTE FUNCTION hookrelay_delivery_event_type()",
    "CREATE TRIGGER trg_deliveries_updated_at BEFORE UPDATE ON deliveries "
    "FOR EACH ROW EXECUTE FUNCTION hookrelay_set_updated_at()",
)
ATTEMPT_INDEXES = (
    "CREATE INDEX ix_delivery_attempts_delivery_created "
    "ON delivery_attempts (delivery_id, created_at, id)",
    "CREATE INDEX ix_delivery_attempts_tenant_created "
    "ON delivery_attempts (tenant_id, created_at, id)",
    "CREATE INDEX ix_delivery_attempts_endpoint_created "
    "ON delivery_attempts (endpoint_id, created_at) INCLUDE (outcome, latency_ms, error_class)",
    "CREATE INDEX ix_delivery_attempts_endpoint_errors "
    "ON delivery_attempts (endpoint_id, created_at) WHERE outcome <> 'succeeded'",
)
ATTEMPT_TRIGGER = (
    "CREATE TRIGGER trg_delivery_attempts_endpoint BEFORE INSERT ON delivery_attempts "
    "FOR EACH ROW EXECUTE FUNCTION hookrelay_attempt_endpoint()"
)

# FK checks run as AFTER triggers in creation order: the delivery FK is created first so an
# unknown delivery is reported by it, not by the endpoint FK on the trigger's nil UUID.
ATTEMPT_ENDPOINT_AND_TENANT_FKS = (
    "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_endpoint_same_tenant "
    "FOREIGN KEY (tenant_id, endpoint_id) REFERENCES endpoints (tenant_id, id) "
    "ON DELETE CASCADE",
    "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_tenant_id_tenants "
    "FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE",
)
DELIVERY_ENDPOINT_AND_TENANT_FKS = (
    "ALTER TABLE deliveries ADD CONSTRAINT fk_deliveries_endpoint_same_tenant "
    "FOREIGN KEY (tenant_id, endpoint_id) REFERENCES endpoints (tenant_id, id) "
    "ON DELETE CASCADE",
    "ALTER TABLE deliveries ADD CONSTRAINT fk_deliveries_tenant_id_tenants "
    "FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE",
)

# --- trigger functions --------------------------------------------------------------------

EVENT_TYPE_FN_NEW = """
CREATE OR REPLACE FUNCTION hookrelay_delivery_event_type() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    found_type text;
BEGIN
    -- event_type is always copied from the (immutable) event, looked up by primary key
    -- (id, created_at): the writer passes event_created_at (it is the partition key, so it
    -- must be known before the row is routed) and the lookup prunes to one events partition.
    -- An unknown or cross-tenant event_id is left to the composite FK to reject (with its own
    -- constraint name); the '' placeholder only gets past NOT NULL so that the FK check runs,
    -- and never commits.
    SELECT ev.event_type INTO found_type
    FROM events AS ev WHERE ev.id = NEW.event_id AND ev.created_at = NEW.event_created_at;
    NEW.event_type := COALESCE(found_type, '');
    RETURN NEW;
END;
$$
"""
EVENT_TYPE_FN_OLD = """
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
ATTEMPT_ENDPOINT_FN_NEW = """
CREATE OR REPLACE FUNCTION hookrelay_attempt_endpoint() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    found_endpoint uuid;
BEGIN
    -- endpoint_id is always derived from the parent delivery, looked up by primary key
    -- (id, event_created_at): the writer passes event_created_at (the partition key) and the
    -- lookup prunes to one deliveries partition. An unknown or cross-tenant delivery_id is
    -- rejected by the (older, so first-firing) delivery FK; the nil UUID never commits.
    SELECT d.endpoint_id INTO found_endpoint
    FROM deliveries AS d
    WHERE d.id = NEW.delivery_id AND d.event_created_at = NEW.event_created_at;
    NEW.endpoint_id := COALESCE(found_endpoint, '00000000-0000-0000-0000-000000000000');
    RETURN NEW;
END;
$$
"""
ATTEMPT_ENDPOINT_FN_OLD = """
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
OUTBOX_FN = """
CREATE OR REPLACE FUNCTION hookrelay_outbox_event_created_at() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    found_created timestamptz;
BEGIN
    -- The ingest path passes event_created_at; a raw writer may leave it NULL. An unknown
    -- event is left to the composite FK (now() only gets past NOT NULL and never commits).
    IF NEW.event_created_at IS NULL THEN
        SELECT ev.created_at INTO found_created FROM events AS ev WHERE ev.id = NEW.event_id;
        NEW.event_created_at := COALESCE(found_created, now());
    END IF;
    RETURN NEW;
END;
$$
"""


def upgrade() -> None:
    for table in ("events", "deliveries", "event_outbox", "delivery_attempts"):
        op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    for table in ("events", "deliveries", "delivery_attempts"):
        _rename_with_partitions(table, OLD)
    for table in ("events", "deliveries", "delivery_attempts"):
        _drop_keys_and_indexes(table + OLD)
    # The outbox FK to events (its own constraint, not a key of an old table).
    op.execute(
        "ALTER TABLE event_outbox DROP CONSTRAINT IF EXISTS fk_event_outbox_event_same_tenant"
    )
    op.execute(
        "ALTER TABLE idempotency_keys "
        "DROP CONSTRAINT IF EXISTS fk_idempotency_keys_event_same_tenant"
    )

    keep = "INCLUDING DEFAULTS INCLUDING CONSTRAINTS"
    op.execute(f"CREATE TABLE events (LIKE events{OLD} {keep}) PARTITION BY RANGE (created_at)")
    op.execute(
        f"CREATE TABLE deliveries (LIKE deliveries{OLD} {keep}, "
        "event_created_at timestamptz NOT NULL) PARTITION BY RANGE (event_created_at)"
    )
    op.execute(
        f"CREATE TABLE delivery_attempts (LIKE delivery_attempts{OLD} {keep}, "
        "event_created_at timestamptz NOT NULL) PARTITION BY RANGE (event_created_at)"
    )
    event_days = f"SELECT DISTINCT (created_at AT TIME ZONE 'UTC')::date FROM events{OLD}"
    for table in ("events", "deliveries", "delivery_attempts"):
        _create_partitions(table, event_days)

    # Copy before any index exists: one bulk build per partition instead of row-by-row
    # maintenance. deliveries / attempts are joined to their parent for the partition key.
    _statements(
        f"INSERT INTO events SELECT * FROM events{OLD}",
        "INSERT INTO deliveries SELECT d.*, e.created_at "
        f"FROM deliveries{OLD} AS d JOIN events{OLD} AS e "
        "ON e.tenant_id = d.tenant_id AND e.id = d.event_id",
        "INSERT INTO delivery_attempts SELECT a.*, d.event_created_at "
        f"FROM delivery_attempts{OLD} AS a JOIN deliveries AS d "
        "ON d.tenant_id = a.tenant_id AND d.id = a.delivery_id",
    )

    # Keys, indexes, FKs, triggers.
    _statements(
        "ALTER TABLE events ADD CONSTRAINT pk_events PRIMARY KEY (id, created_at)",
        "ALTER TABLE events ADD CONSTRAINT uq_events_tenant_id_id "
        "UNIQUE (tenant_id, id, created_at)",
        *EVENT_INDEXES,
        *EVENT_FKS,
        "ALTER TABLE deliveries ADD CONSTRAINT pk_deliveries PRIMARY KEY (id, event_created_at)",
        "ALTER TABLE deliveries ADD CONSTRAINT uq_deliveries_event_endpoint "
        "UNIQUE (event_id, endpoint_id, event_created_at)",
        "ALTER TABLE deliveries ADD CONSTRAINT uq_deliveries_tenant_id_id "
        "UNIQUE (tenant_id, id, event_created_at)",
        *DELIVERY_INDEXES,
        "ALTER TABLE deliveries ADD CONSTRAINT fk_deliveries_event_same_tenant "
        "FOREIGN KEY (tenant_id, event_id, event_created_at) "
        "REFERENCES events (tenant_id, id, created_at) ON DELETE CASCADE",
        *DELIVERY_ENDPOINT_AND_TENANT_FKS,
        "ALTER TABLE delivery_attempts ADD CONSTRAINT pk_delivery_attempts "
        "PRIMARY KEY (id, event_created_at)",
        "ALTER TABLE delivery_attempts ADD CONSTRAINT uq_delivery_attempts_number "
        "UNIQUE (delivery_id, attempt_number, event_created_at)",
        *ATTEMPT_INDEXES,
        "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_delivery_same_tenant "
        "FOREIGN KEY (tenant_id, delivery_id, event_created_at) "
        "REFERENCES deliveries (tenant_id, id, event_created_at) ON DELETE CASCADE",
        *ATTEMPT_ENDPOINT_AND_TENANT_FKS,
        EVENT_TYPE_FN_NEW,
        ATTEMPT_ENDPOINT_FN_NEW,
        *DELIVERY_TRIGGERS,
        ATTEMPT_TRIGGER,
    )

    # event_outbox: carry the key, backfill from events, enforce with the composite FK.
    _statements(
        "ALTER TABLE event_outbox ADD COLUMN event_created_at timestamptz",
        "UPDATE event_outbox AS o SET event_created_at = e.created_at FROM events AS e "
        "WHERE e.tenant_id = o.tenant_id AND e.id = o.event_id",
        "ALTER TABLE event_outbox ALTER COLUMN event_created_at SET NOT NULL",
        "ALTER TABLE event_outbox ADD CONSTRAINT fk_event_outbox_event_same_tenant "
        "FOREIGN KEY (tenant_id, event_id, event_created_at) "
        "REFERENCES events (tenant_id, id, created_at) ON DELETE CASCADE",
        OUTBOX_FN,
        "CREATE TRIGGER trg_event_outbox_event_created_at BEFORE INSERT ON event_outbox "
        "FOR EACH ROW EXECUTE FUNCTION hookrelay_outbox_event_created_at()",
    )

    # External ids move out of the (now per-day unique) events indexes.
    _statements(
        """
        CREATE TABLE event_external_ids (
            tenant_id uuid NOT NULL,
            external_id varchar(128) NOT NULL,
            event_id uuid NOT NULL,
            event_created_at timestamptz NOT NULL,
            expires_at timestamptz NOT NULL,
            CONSTRAINT pk_event_external_ids PRIMARY KEY (tenant_id, external_id),
            CONSTRAINT ck_event_external_ids_external_id_format
                CHECK (external_id ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$'),
            CONSTRAINT fk_event_external_ids_tenant_id_tenants
                FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE
        )
        """,
        "CREATE INDEX ix_event_external_ids_expires_at ON event_external_ids (expires_at)",
        "INSERT INTO event_external_ids "
        "(tenant_id, external_id, event_id, event_created_at, expires_at) "
        "SELECT tenant_id, external_id, id, created_at, retain_until FROM events "
        "WHERE external_id IS NOT NULL",
    )

    for table in ("delivery_attempts", "deliveries", "events"):
        op.execute(f"DROP TABLE {table}{OLD}")


def downgrade() -> None:
    for table in ("events", "deliveries", "event_outbox", "delivery_attempts"):
        op.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
    for table in ("events", "deliveries", "delivery_attempts"):
        _rename_with_partitions(table, NEW)
    for table in ("events", "deliveries", "delivery_attempts"):
        _drop_keys_and_indexes(table + NEW)
    _statements(
        "ALTER TABLE event_outbox DROP CONSTRAINT IF EXISTS fk_event_outbox_event_same_tenant",
        "DROP TRIGGER IF EXISTS trg_event_outbox_event_created_at ON event_outbox",
        "DROP FUNCTION IF EXISTS hookrelay_outbox_event_created_at()",
        "ALTER TABLE event_outbox DROP COLUMN event_created_at",
    )

    keep = "INCLUDING DEFAULTS INCLUDING CONSTRAINTS"
    op.execute(f"CREATE TABLE events (LIKE events{NEW} {keep})")
    op.execute(f"CREATE TABLE deliveries (LIKE deliveries{NEW} {keep})")
    op.execute(
        f"CREATE TABLE delivery_attempts (LIKE delivery_attempts{NEW} {keep}) "
        "PARTITION BY RANGE (created_at)"
    )
    _create_partitions(
        "delivery_attempts",
        f"SELECT DISTINCT (created_at AT TIME ZONE 'UTC')::date FROM delivery_attempts{NEW}",
    )
    _statements(
        f"INSERT INTO events SELECT * FROM events{NEW}",
        f"INSERT INTO deliveries SELECT * FROM deliveries{NEW}",
        f"INSERT INTO delivery_attempts SELECT * FROM delivery_attempts{NEW}",
        "ALTER TABLE deliveries DROP COLUMN event_created_at",
        "ALTER TABLE delivery_attempts DROP COLUMN event_created_at",
    )

    _statements(
        "ALTER TABLE events ADD CONSTRAINT pk_events PRIMARY KEY (id)",
        "ALTER TABLE events ADD CONSTRAINT uq_events_tenant_id_id UNIQUE (tenant_id, id)",
        *EVENT_INDEXES,
        "CREATE UNIQUE INDEX uq_events_tenant_external_id ON events (tenant_id, external_id) "
        "WHERE external_id IS NOT NULL",
        *EVENT_FKS,
        "ALTER TABLE deliveries ADD CONSTRAINT pk_deliveries PRIMARY KEY (id)",
        "ALTER TABLE deliveries ADD CONSTRAINT uq_deliveries_event_endpoint "
        "UNIQUE (event_id, endpoint_id)",
        "ALTER TABLE deliveries ADD CONSTRAINT uq_deliveries_tenant_id_id UNIQUE (tenant_id, id)",
        *DELIVERY_INDEXES,
        "ALTER TABLE deliveries ADD CONSTRAINT fk_deliveries_event_same_tenant "
        "FOREIGN KEY (tenant_id, event_id) REFERENCES events (tenant_id, id) ON DELETE CASCADE",
        *DELIVERY_ENDPOINT_AND_TENANT_FKS,
        "ALTER TABLE delivery_attempts ADD CONSTRAINT pk_delivery_attempts "
        "PRIMARY KEY (id, created_at)",
        "ALTER TABLE delivery_attempts ADD CONSTRAINT uq_delivery_attempts_number "
        "UNIQUE (delivery_id, attempt_number, created_at)",
        *ATTEMPT_INDEXES,
        "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_delivery_same_tenant "
        "FOREIGN KEY (tenant_id, delivery_id) REFERENCES deliveries (tenant_id, id) "
        "ON DELETE CASCADE",
        *ATTEMPT_ENDPOINT_AND_TENANT_FKS,
        EVENT_TYPE_FN_OLD,
        ATTEMPT_ENDPOINT_FN_OLD,
        *DELIVERY_TRIGGERS,
        ATTEMPT_TRIGGER,
        # The outbox rows of events whose partition was dropped cannot exist (FK); the
        # idempotency FK cascaded dangling keys away before, so do the same once.
        "DELETE FROM idempotency_keys AS k WHERE k.event_id IS NOT NULL AND NOT EXISTS ("
        "SELECT 1 FROM events AS e WHERE e.tenant_id = k.tenant_id AND e.id = k.event_id)",
        "ALTER TABLE event_outbox ADD CONSTRAINT fk_event_outbox_event_same_tenant "
        "FOREIGN KEY (tenant_id, event_id) REFERENCES events (tenant_id, id) ON DELETE CASCADE",
        "ALTER TABLE idempotency_keys ADD CONSTRAINT fk_idempotency_keys_event_same_tenant "
        "FOREIGN KEY (tenant_id, event_id) REFERENCES events (tenant_id, id) ON DELETE CASCADE",
        "DROP TABLE event_external_ids",
    )

    for table in ("delivery_attempts", "deliveries", "events"):
        op.execute(f"DROP TABLE {table}{NEW}")
