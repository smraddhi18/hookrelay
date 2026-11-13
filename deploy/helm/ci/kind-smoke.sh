#!/usr/bin/env bash
# End-to-end smoke of the Helm chart on a kind cluster (CI "helm" job; runnable locally).
#
# Prerequisites: a kind cluster as the current kubectl context, the image loaded as
# hookrelay:kind (docker build -t hookrelay:kind . && kind load docker-image hookrelay:kind),
# and helm, kubectl, curl, jq, python3 and k6 on PATH.
#
#   bash deploy/helm/ci/kind-smoke.sh
#
# Steps: in-cluster Postgres / Redis / mock receiver -> Secret (existingSecret path) ->
# helm install --wait (pre-install migration Job) -> schema at head -> helm test -> /ready ->
# k6 e2e through port-forwards (no lost / duplicated deliveries) -> helm upgrade (pre-upgrade
# migration Job re-runs) -> worker SIGTERM drain logs worker.stopped.
# Env: NS (hookrelay-ci)  RELEASE (hookrelay)  OUT_DIR (./kind-smoke)  K6_ARGS (extra k6 args)
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
CHART="$ROOT/deploy/helm/hookrelay"
NS=${NS:-hookrelay-ci}
RELEASE=${RELEASE:-hookrelay}
OUT_DIR=${OUT_DIR:-$PWD/kind-smoke}
mkdir -p "$OUT_DIR"
k() { kubectl -n "$NS" "$@"; }

diagnostics() {
  echo "::group::kind smoke diagnostics"
  k get all,jobs,pdb,cm,secret -o wide || true
  k get events --sort-by=.lastTimestamp | tail -50 || true
  for pod in $(k get pods -o name 2>/dev/null); do
    echo "--- $pod"; k logs "$pod" --all-containers --tail=80 || true
  done
  helm -n "$NS" history "$RELEASE" || true
  echo "::endgroup::"
}
pids=()
cleanup() {
  status=$?
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  if [ "$status" != 0 ]; then diagnostics; fi
  exit "$status"
}
trap cleanup EXIT

echo "== dependencies"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -
k apply -f "$HERE/deps.yaml"
k rollout status statefulset/postgres --timeout=180s
k rollout status deployment/redis --timeout=120s
k rollout status deployment/mock-receiver --timeout=120s

echo "== secret (throwaway per-run credentials, never reused)"
rand() { python3 -c 'import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode().rstrip("="))'; }
OPERATOR_TOKEN=$(rand)
k create secret generic hookrelay-ci --dry-run=client -o yaml \
  --from-literal=DATABASE_URL=postgresql+asyncpg://hookrelay:hookrelay@postgres:5432/hookrelay \
  --from-literal=REDIS_URL=redis://redis:6379/0 \
  --from-literal=OPERATOR_TOKEN="$OPERATOR_TOKEN" \
  --from-literal=SECRETS_ENCRYPTION_KEY="$(rand)" | k apply -f -

echo "== helm install (pre-install migration hook, then --wait for every Deployment)"
helm install "$RELEASE" "$CHART" -n "$NS" -f "$HERE/values-kind.yaml" --wait --timeout 6m
for d in api relay worker; do k rollout status "deployment/$RELEASE-$d" --timeout=180s; done
# The hook Job is deleted on success; the schema is the proof it ran.
k exec "deploy/$RELEASE-api" -- alembic current 2>/dev/null | tee "$OUT_DIR/alembic-current.txt"
grep -q '(head)' "$OUT_DIR/alembic-current.txt"

echo "== helm test"
helm test "$RELEASE" -n "$NS" --logs --timeout 2m

echo "== port-forwards"
k port-forward "svc/$RELEASE" 8000:80 >"$OUT_DIR/pf-api.log" 2>&1 &
pids+=($!)
k port-forward svc/mock-receiver 9000:9000 >"$OUT_DIR/pf-receiver.log" 2>&1 &
pids+=($!)
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8000/ready >/dev/null 2>&1 \
    && curl -fsS http://127.0.0.1:9000/health >/dev/null 2>&1; then break; fi
  sleep 1
done
curl -fsS http://127.0.0.1:8000/ready | tee "$OUT_DIR/ready.json"; echo
test "$(jq -r .status "$OUT_DIR/ready.json")" = ready
test "$(jq -r .checks.postgres.status "$OUT_DIR/ready.json")" = up
test "$(jq -r .checks.redis.status "$OUT_DIR/ready.json")" = up

echo "== k6 e2e (workers deliver to the in-cluster receiver; k6 checks through port-forwards)"
# shellcheck disable=SC2086 # K6_ARGS is a list of flags
k6 run --quiet \
  -e BASE_URL=http://127.0.0.1:8000 -e OPERATOR_TOKEN="$OPERATOR_TOKEN" \
  -e RECEIVER_URL="http://mock-receiver.$NS.svc.cluster.local:9000" \
  -e RECEIVER_ADMIN_URL=http://127.0.0.1:9000 \
  -e RATE=10 -e DURATION=15s -e ENDPOINTS=2 \
  -e 'RECEIVER_QUERY=latency_ms=10&jitter_ms=20&failure_rate=0.05&failure_status=500,503' \
  -e P95_MS=2000 -e E2E_P95_MS=15000 -e DRAIN_TIMEOUT_S=120 ${K6_ARGS:-} \
  --summary-export "$OUT_DIR/e2e.json" "$ROOT/loadtest/e2e.js"
curl -fsS http://127.0.0.1:9000/stats >"$OUT_DIR/receiver-stats.json"
jq . "$OUT_DIR/receiver-stats.json"

echo "== helm upgrade (pre-upgrade migration hook re-runs; idempotent at head)"
helm upgrade "$RELEASE" "$CHART" -n "$NS" -f "$HERE/values-kind.yaml" \
  --set worker.replicas=3 --wait --timeout 6m
k rollout status "deployment/$RELEASE-worker" --timeout=180s
test "$(k get "deployment/$RELEASE-worker" -o jsonpath='{.status.readyReplicas}')" = 3
test "$(helm -n "$NS" history "$RELEASE" -o json | jq -r '.[-1].status')" = deployed

echo "== worker SIGTERM: drains and exits cleanly within the grace period"
POD=$(k get pods -l app.kubernetes.io/component=worker -o jsonpath='{.items[0].metadata.name}')
k logs -f "$POD" >"$OUT_DIR/worker-drain.log" 2>&1 &
pids+=($!)
sleep 2
k delete pod "$POD" --wait=true --timeout=90s
sleep 2
grep -q '"event": "worker.stopped"' "$OUT_DIR/worker-drain.log"
echo "kind smoke passed"
