#!/usr/bin/env bash
# Start a throwaway PgBouncer for the integration tests and the k6 smoke behind a pooler
# (CI does exactly this). Transaction pooling, a "*" entry that forwards every database name
# (the tests create scratch databases) and max_prepared_statements = 0, the strictest pooler
# DB_PGBOUNCER=true must survive.
#
#   UPSTREAM_PORT=5432 bash deploy/pgbouncer/run-test-pooler.sh   # prints TEST_PGBOUNCER_URL
#
# Env: UPSTREAM_HOST (127.0.0.1) UPSTREAM_PORT (5432) LISTEN_PORT (6439) POOL_SIZE (8)
#      DB_USER / DB_PASSWORD (hookrelay / hookrelay, throwaway CI credentials)
#      PGBOUNCER (pgbouncer on PATH, else /usr/sbin/pgbouncer)  RUN_DIR (a new temp dir)
set -euo pipefail
UPSTREAM_HOST=${UPSTREAM_HOST:-127.0.0.1}
UPSTREAM_PORT=${UPSTREAM_PORT:-5432}
LISTEN_PORT=${LISTEN_PORT:-6439}
POOL_SIZE=${POOL_SIZE:-8}
DB_USER=${DB_USER:-hookrelay}
DB_PASSWORD=${DB_PASSWORD:-hookrelay}
PGBOUNCER=${PGBOUNCER:-$(command -v pgbouncer || echo /usr/sbin/pgbouncer)}
RUN_DIR=${RUN_DIR:-$(mktemp -d -t hookrelay-pgbouncer.XXXXXX)}
mkdir -p "$RUN_DIR"

printf '"%s" "%s"\n' "$DB_USER" "$DB_PASSWORD" >"$RUN_DIR/userlist.txt"
cat >"$RUN_DIR/pgbouncer.ini" <<INI
[databases]
* = host=$UPSTREAM_HOST port=$UPSTREAM_PORT connect_query='SET statement_timeout = 15000'

[pgbouncer]
listen_addr = 127.0.0.1
listen_port = $LISTEN_PORT
unix_socket_dir =
auth_type = scram-sha-256
auth_file = $RUN_DIR/userlist.txt
pool_mode = transaction
default_pool_size = $POOL_SIZE
max_client_conn = 2000
max_prepared_statements = 0
query_wait_timeout = 30
ignore_startup_parameters = extra_float_digits
logfile = $RUN_DIR/pgbouncer.log
pidfile = $RUN_DIR/pgbouncer.pid
INI

"$PGBOUNCER" -d "$RUN_DIR/pgbouncer.ini"
for _ in $(seq 1 50); do
  if PGPASSWORD=$DB_PASSWORD psql -h 127.0.0.1 -p "$LISTEN_PORT" -U "$DB_USER" -d postgres \
    -tAc 'SHOW statement_timeout' >"$RUN_DIR/check.txt" 2>/dev/null; then
    break
  fi
  sleep 0.2
done
grep -qx '15s' "$RUN_DIR/check.txt" || { cat "$RUN_DIR/pgbouncer.log" >&2; exit 1; }
echo "pgbouncer pid $(cat "$RUN_DIR/pgbouncer.pid") on 127.0.0.1:$LISTEN_PORT (logs: $RUN_DIR)" >&2
echo "TEST_PGBOUNCER_URL=postgresql://$DB_USER:$DB_PASSWORD@127.0.0.1:$LISTEN_PORT/postgres"
