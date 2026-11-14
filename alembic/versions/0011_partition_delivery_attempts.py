"""Native range partitioning of delivery_attempts by created_at (daily + DEFAULT partition).

Revision ID: 0011
Revises: 0010
Create Date: 2026-11-14

``delivery_attempts`` is the fastest-growing table (one row per HTTP attempt) and the only one
whose retention used to be row-by-row: expiring an event cascaded a ``DELETE`` into every
attempt. Partitioned by UTC day, retention becomes ``DETACH`` + ``DROP`` of whole partitions
(``hookrelay-partitions``; the event-retention cascade still deletes most rows first, see
docs/partitioning.md) and time-windowed reads (endpoint health, dashboard, tenant
listings) prune to the partitions they need.

Upgrade, in this revision's transaction (``ACCESS EXCLUSIVE`` on the old table throughout):

1. rename the old table to ``delivery_attempts_m0010`` and drop its indexes / trigger, which
   frees the schema-global index names (``pk_delivery_attempts`` ...);
2. create the partitioned parent with the same columns (same order) and CHECK constraints;
3. create one partition per UTC day that has rows, plus today .. today + 7 (the maintenance
   job keeps that horizon rolling), plus the ``DEFAULT`` partition;
4. copy the rows (before any index exists: one bulk build per partition instead of row-by-row
   index maintenance), then add the keys, indexes, FKs and the endpoint trigger;
5. drop the old table.

This is the simple, in-migration copy: right for the sizes this project runs at, but it holds
writes for the duration of the copy. The online path for a large production table (attach the
old table as a single legacy partition, or dual-write + batched backfill + rename) is in
docs/partitioning.md.

Keys: Postgres requires every unique constraint on a partitioned table to contain the
partition key, so the primary key becomes ``(id, created_at)`` and the per-delivery
attempt-number constraint becomes ``(delivery_id, attempt_number, created_at)``. Ids stay
random UUIDs (unique in practice); attempt numbers are assigned under the delivery's lease
fence, the constraint is now a per-partition backstop (see docs/partitioning.md).

Downgrade copies the rows back into an unpartitioned table with the 0010 keys; it fails loudly
(unique violation) instead of dropping rows if the weaker constraint ever let a duplicate
attempt number through.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PREMAKE_DAYS = 7  # matches the PARTITION_PREMAKE_DAYS default

COLUMNS = """
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    tenant_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    outcome varchar(16) NOT NULL,
    status_code integer,
    error_class varchar(32),
    error_message text,
    latency_ms integer,
    request_url text NOT NULL,
    request_headers jsonb DEFAULT '{}'::jsonb NOT NULL,
    response_body text,
    response_truncated boolean DEFAULT false NOT NULL,
    worker_id varchar(128) NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    next_attempt_at timestamptz,
    created_at timestamptz DEFAULT now() NOT NULL,
    endpoint_id uuid NOT NULL,
    CONSTRAINT ck_delivery_attempts_attempt_number_positive CHECK (attempt_number >= 1),
    CONSTRAINT ck_delivery_attempts_error_class_iff_unsuccessful
        CHECK ((outcome = 'succeeded') = (error_class IS NULL)),
    CONSTRAINT ck_delivery_attempts_error_message_bounded
        CHECK (error_message IS NULL OR length(error_message) <= 1024),
    CONSTRAINT ck_delivery_attempts_finished_after_started CHECK (finished_at >= started_at),
    CONSTRAINT ck_delivery_attempts_latency_non_negative
        CHECK (latency_ms IS NULL OR latency_ms >= 0),
    CONSTRAINT ck_delivery_attempts_outcome_valid
        CHECK (outcome IN ('succeeded', 'retrying', 'failed')),
    CONSTRAINT ck_delivery_attempts_request_headers_object
        CHECK (jsonb_typeof(request_headers) = 'object'),
    CONSTRAINT ck_delivery_attempts_response_body_bounded
        CHECK (response_body IS NULL OR octet_length(response_body) <= 65536),
    CONSTRAINT ck_delivery_attempts_status_code_range
        CHECK (status_code IS NULL OR status_code BETWEEN 100 AND 599),
    CONSTRAINT ck_delivery_attempts_success_is_2xx
        CHECK (outcome <> 'succeeded' OR COALESCE(status_code BETWEEN 200 AND 299, false))
"""

COLUMN_LIST = (
    "id, tenant_id, delivery_id, attempt_number, outcome, status_code, error_class, "
    "error_message, latency_ms, request_url, request_headers, response_body, "
    "response_truncated, worker_id, started_at, finished_at, next_attempt_at, created_at, "
    "endpoint_id"
)

INDEXES = (
    "CREATE INDEX ix_delivery_attempts_delivery_created "
    "ON delivery_attempts (delivery_id, created_at, id)",
    "CREATE INDEX ix_delivery_attempts_tenant_created "
    "ON delivery_attempts (tenant_id, created_at, id)",
    "CREATE INDEX ix_delivery_attempts_endpoint_created "
    "ON delivery_attempts (endpoint_id, created_at) INCLUDE (outcome, latency_ms, error_class)",
    "CREATE INDEX ix_delivery_attempts_endpoint_errors "
    "ON delivery_attempts (endpoint_id, created_at) WHERE outcome <> 'succeeded'",
)

# Order matters: FK checks run as AFTER triggers in creation order, and an unknown delivery
# must be reported by the delivery FK, not by the endpoint FK on the trigger's nil UUID.
FOREIGN_KEYS = (
    "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_delivery_same_tenant "
    "FOREIGN KEY (tenant_id, delivery_id) REFERENCES deliveries (tenant_id, id) "
    "ON DELETE CASCADE",
    "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_endpoint_same_tenant "
    "FOREIGN KEY (tenant_id, endpoint_id) REFERENCES endpoints (tenant_id, id) "
    "ON DELETE CASCADE",
    "ALTER TABLE delivery_attempts ADD CONSTRAINT fk_delivery_attempts_tenant_id_tenants "
    "FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE",
)

TRIGGER = (
    "CREATE TRIGGER trg_delivery_attempts_endpoint BEFORE INSERT ON delivery_attempts "
    "FOR EACH ROW EXECUTE FUNCTION hookrelay_attempt_endpoint()"
)

OLD_INDEXES = (
    "ix_delivery_attempts_delivery_created",
    "ix_delivery_attempts_tenant_created",
    "ix_delivery_attempts_endpoint_created",
    "ix_delivery_attempts_endpoint_errors",
)

# One partition per UTC day with rows, and today .. today + PREMAKE_DAYS. A DO block rather
# than Python so `alembic upgrade --sql` still emits a complete script.
CREATE_PARTITIONS = f"""
DO $$
DECLARE
    day date;
BEGIN
    FOR day IN
        SELECT DISTINCT (created_at AT TIME ZONE 'UTC')::date FROM delivery_attempts_m0010
        UNION
        SELECT generate_series(
            (now() AT TIME ZONE 'UTC')::date,
            (now() AT TIME ZONE 'UTC')::date + {PREMAKE_DAYS},
            interval '1 day'
        )::date
        ORDER BY 1
    LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF delivery_attempts FOR VALUES FROM (%L) TO (%L)',
            'delivery_attempts_p' || to_char(day, 'YYYYMMDD'),
            (day::timestamp AT TIME ZONE 'UTC'),
            ((day + 1)::timestamp AT TIME ZONE 'UTC')
        );
    END LOOP;
END
$$
"""


def _drop_keys_and_indexes(table: str) -> None:
    op.execute(f"DROP TRIGGER IF EXISTS trg_delivery_attempts_endpoint ON {table}")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS pk_delivery_attempts")
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS uq_delivery_attempts_number")
    for index in OLD_INDEXES:
        op.execute(f"DROP INDEX IF EXISTS {index}")


def _add_keys_indexes_fks(*, partitioned: bool) -> None:
    created = ", created_at" if partitioned else ""
    op.execute(
        f"ALTER TABLE delivery_attempts ADD CONSTRAINT pk_delivery_attempts "
        f"PRIMARY KEY (id{created})"
    )
    op.execute(
        f"ALTER TABLE delivery_attempts ADD CONSTRAINT uq_delivery_attempts_number "
        f"UNIQUE (delivery_id, attempt_number{created})"
    )
    for statement in (*INDEXES, *FOREIGN_KEYS, TRIGGER):
        op.execute(statement)


def upgrade() -> None:
    op.execute("LOCK TABLE delivery_attempts IN ACCESS EXCLUSIVE MODE")
    op.execute("ALTER TABLE delivery_attempts RENAME TO delivery_attempts_m0010")
    _drop_keys_and_indexes("delivery_attempts_m0010")
    op.execute(f"CREATE TABLE delivery_attempts ({COLUMNS}) PARTITION BY RANGE (created_at)")
    op.execute(CREATE_PARTITIONS)
    op.execute("CREATE TABLE delivery_attempts_default PARTITION OF delivery_attempts DEFAULT")
    op.execute(
        f"INSERT INTO delivery_attempts ({COLUMN_LIST}) "
        f"SELECT {COLUMN_LIST} FROM delivery_attempts_m0010"
    )
    _add_keys_indexes_fks(partitioned=True)
    op.execute("DROP TABLE delivery_attempts_m0010")


def downgrade() -> None:
    op.execute("LOCK TABLE delivery_attempts IN ACCESS EXCLUSIVE MODE")
    op.execute("ALTER TABLE delivery_attempts RENAME TO delivery_attempts_m0011")
    _drop_keys_and_indexes("delivery_attempts_m0011")
    op.execute(f"CREATE TABLE delivery_attempts ({COLUMNS})")
    op.execute(
        f"INSERT INTO delivery_attempts ({COLUMN_LIST}) "
        f"SELECT {COLUMN_LIST} FROM delivery_attempts_m0011"
    )
    _add_keys_indexes_fks(partitioned=False)
    # Drops every attached partition with it. Partitions the maintenance job detached but did
    # not get to drop are standalone tables by then and stay (it drops them on its next run).
    op.execute("DROP TABLE delivery_attempts_m0011")
