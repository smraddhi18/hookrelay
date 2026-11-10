#!/usr/bin/env bash
# Run one k6 script against a freshly started local hookrelay stack (API, relay, workers and
# the mock receiver as processes on this machine; Postgres and optionally Redis already
# running), then stop everything.
#
#   DATABASE_URL=postgresql://hookrelay:hookrelay@127.0.0.1:5432/hookrelay_load \
#   bash loadtest/run-local.sh e2e.js -e RATE=200 -e DURATION=60s -e ENDPOINTS=4
#
# Env (all optional except DATABASE_URL):
#   DATABASE_URL          Postgres to migrate and use (use a dedicated database)
#   REDIS_URL             enables wake-ups / subscription cache / Redis rate limits
#   PORT                  API port (8000)           API_WORKERS   uvicorn processes (1)
#   RELAYS                relay processes (1)       WORKERS       worker processes (2)
#   WORKER_CONCURRENCY    per worker (32)           START_WORKERS_AFTER_S  start workers late
#                                                   (backlog-drain benchmark; default 0)
#   RECEIVER_PORT         mock receiver port (9000) RECEIVER_ARGS extra mock-receiver flags
#   DB_POOL_SIZE / DB_MAX_OVERFLOW                  per process (defaults from .env.example)
#   OUT_DIR               logs + k6 summary JSON (/tmp/hookrelay-loadtest)
#   CHAOS_CMD             optional fault to inject while k6 runs, e.g. a real Postgres restart:
#                         CHAOS_CMD='pg_ctl -D /path/to/data restart -m fast -w' (output: chaos.log)
#   CHAOS_AFTER_S         seconds after k6 starts before CHAOS_CMD runs (20)
#   K6                    k6 binary (k6 on PATH)
# Scripts: ingest.js, e2e.js (see loadtest/README.md). Exit status is k6's (thresholds).
set -euo pipefail

SCRIPT=${1:?usage: run-local.sh <ingest.js|e2e.js> [k6 args...]}
shift
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/.." && pwd)
: "${DATABASE_URL:?DATABASE_URL is required (a dedicated database)}"
PORT=${PORT:-8000}
API_WORKERS=${API_WORKERS:-1}
RELAYS=${RELAYS:-1}
WORKERS=${WORKERS:-2}
RECEIVER_PORT=${RECEIVER_PORT:-9000}
START_WORKERS_AFTER_S=${START_WORKERS_AFTER_S:-0}
OUT_DIR=${OUT_DIR:-/tmp/hookrelay-loadtest}
K6=${K6:-k6}
mkdir -p "$OUT_DIR"
OUT_DIR=$(cd "$OUT_DIR" && pwd)
cd "$ROOT"

rand() { python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("="))'; }
# Throwaway per-run credentials (never reuse outside this run).
export OPERATOR_TOKEN=${OPERATOR_TOKEN:-$(rand)}
export SECRETS_ENCRYPTION_KEY=${SECRETS_ENCRYPTION_KEY:-$(rand)}
# Development mode is the only one that may deliver to the loopback receiver (SSRF guard).
export APP_ENV=development LOG_FORMAT=json LOG_LEVEL=${LOG_LEVEL:-WARNING} DOCS_ENABLED=false
export DATABASE_URL
export ENDPOINT_ALLOW_HTTP=true ENDPOINT_ALLOW_PRIVATE_NETWORKS=true
export DELIVERY_ALLOW_PRIVATE_NETWORKS=true DELIVERY_ALLOWED_PORTS=$RECEIVER_PORT
export ENDPOINT_MAX_IN_FLIGHT=${ENDPOINT_MAX_IN_FLIGHT:-500}
export WORKER_CONCURRENCY=${WORKER_CONCURRENCY:-32}
export DELIVERY_BACKOFF_BASE_S=${DELIVERY_BACKOFF_BASE_S:-0.5}
export DELIVERY_BACKOFF_MAX_S=${DELIVERY_BACKOFF_MAX_S:-5}
export BREAKER_FAILURE_THRESHOLD=${BREAKER_FAILURE_THRESHOLD:-50}
export TRACING_ENABLED=false
[ -n "${REDIS_URL:-}" ] && export REDIS_URL

uv run --locked alembic upgrade head >"$OUT_DIR/migrate.log" 2>&1

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

# shellcheck disable=SC2086 # RECEIVER_ARGS is a list of flags
uv run --locked hookrelay-mock-receiver --port "$RECEIVER_PORT" --seed 1 ${RECEIVER_ARGS:-} \
  >"$OUT_DIR/receiver.log" 2>&1 &
pids+=($!)
uv run --locked uvicorn hookrelay.main:create_app --factory --host 127.0.0.1 --port "$PORT" \
  --workers "$API_WORKERS" --no-access-log --log-level warning >"$OUT_DIR/api.log" 2>&1 &
pids+=($!)
for i in $(seq 1 "$RELAYS"); do
  RELAY_HEARTBEAT_FILE="$OUT_DIR/relay-$i.hb" uv run --locked hookrelay-relay \
    >"$OUT_DIR/relay-$i.log" 2>&1 &
  pids+=($!)
done
start_workers() {
  for i in $(seq 1 "$WORKERS"); do
    WORKER_ID="lt-worker-$i" WORKER_HEARTBEAT_FILE="$OUT_DIR/worker-$i.hb" \
      uv run --locked hookrelay-worker >"$OUT_DIR/worker-$i.log" 2>&1 &
    pids+=($!)
  done
}

for _ in $(seq 1 150); do
  if curl -fsS "http://127.0.0.1:$PORT/ready" >/dev/null 2>&1 \
    && curl -fsS "http://127.0.0.1:$RECEIVER_PORT/health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done
curl -fsS "http://127.0.0.1:$PORT/ready" >/dev/null
curl -fsS "http://127.0.0.1:$RECEIVER_PORT/health" >/dev/null

if [ "$START_WORKERS_AFTER_S" = 0 ]; then
  start_workers
else
  # Backlog-drain benchmark: ingest first, then measure how fast the workers empty the queue.
  (
    trap 'kill -TERM $(jobs -p) 2>/dev/null; wait' TERM
    sleep "$START_WORKERS_AFTER_S" & wait $!
    start_workers
    wait
  ) &
  pids+=($!)
fi

if [ -n "${CHAOS_CMD:-}" ]; then
  # Fault injection for chaos runs (see docs/capacity.md): the stack must ride it out with no
  # lost and no twice-acknowledged deliveries, which e2e.js checks after the drain.
  (
    sleep "${CHAOS_AFTER_S:-20}"
    echo "chaos at $(date -u +%FT%TZ): $CHAOS_CMD"
    bash -c "$CHAOS_CMD"
    echo "chaos done at $(date -u +%FT%TZ): exit $?"
  ) >"$OUT_DIR/chaos.log" 2>&1 &
  pids+=($!)
fi

name=$(basename "$SCRIPT" .js)
set +e
"$K6" run --quiet -e BASE_URL="http://127.0.0.1:$PORT" -e OPERATOR_TOKEN="$OPERATOR_TOKEN" \
  -e RECEIVER_URL="http://127.0.0.1:$RECEIVER_PORT" --summary-export "$OUT_DIR/$name.json" \
  "$@" "$HERE/$(basename "$SCRIPT")"
status=$?
set -e
curl -fsS "http://127.0.0.1:$RECEIVER_PORT/stats" >"$OUT_DIR/receiver-stats.json" || true
echo "k6 exit: $status  summary: $OUT_DIR/$name.json  receiver: $OUT_DIR/receiver-stats.json  logs: $OUT_DIR/*.log"
exit $status
