#!/usr/bin/env bash
# Static checks for the Helm chart (CI "helm" job, also run by the local CI script):
#   helm lint --strict, helm template for each value set, kubeconform -strict against the
#   Kubernetes schemas (+ KEDA / Prometheus Operator CRD schemas), and guard-rail checks that
#   bad values make rendering fail.
#
#   bash deploy/helm/ci/validate.sh            # needs helm and kubeconform on PATH
# Env: KUBE_VERSION (1.34.0)  OUT_DIR (a temp dir; rendered manifests are kept there)
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CHART=$(cd "$HERE/../hookrelay" && pwd)
KUBE_VERSION=${KUBE_VERSION:-1.34.0}
OUT_DIR=${OUT_DIR:-$(mktemp -d -t hookrelay-helm.XXXXXX)}
mkdir -p "$OUT_DIR"
# Minimal dev/CI value set: the chart itself manages the Secret.
DEV=(--set secrets.values.DATABASE_URL=postgresql://hookrelay:hookrelay@postgres:5432/hookrelay)
CRDS='https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json'

render() { # name, helm args...
  local name=$1; shift
  helm lint --strict "$CHART" "$@" >"$OUT_DIR/$name.lint.txt" || { cat "$OUT_DIR/$name.lint.txt"; return 1; }
  helm template hookrelay "$CHART" --namespace hookrelay --kube-version "$KUBE_VERSION" "$@" >"$OUT_DIR/$name.yaml"
  kubeconform -strict -summary -kubernetes-version "$KUBE_VERSION" \
    -schema-location default -schema-location "$CRDS" "$OUT_DIR/$name.yaml"
  echo "ok: $name ($(grep -c '^kind:' "$OUT_DIR/$name.yaml") objects)"
}

must_fail() { # description, expected message, helm args...
  local what=$1 expect=$2; shift 2
  if out=$(helm template hookrelay "$CHART" "$@" 2>&1); then
    echo "expected rendering to fail: $what" >&2; return 1
  fi
  grep -Eq -- "$expect" <<<"$out" || { echo "wrong failure for $what: $out" >&2; return 1; }
  echo "ok: refuses $what"
}

render minimal "${DEV[@]}"
render kind -f "$HERE/values-kind.yaml"
render full -f "$HERE/values-full.yaml"
render no-optional "${DEV[@]}" --set migrations.enabled=false --set retention.enabled=false \
  --set partitions.enabled=false --set tests.enabled=false --set api.pdb.enabled=false --set relay.metricsPort=0 \
  --set worker.metricsPort=0 --set serviceAccount.create=false --set topologySpread.enabled=false

# The kind smoke's own dependencies (Postgres / Redis / mock receiver) are plain manifests.
kubeconform -strict -summary -kubernetes-version "$KUBE_VERSION" "$HERE/deps.yaml"
echo "ok: kind dependencies ($(grep -c '^kind:' "$HERE/deps.yaml") objects)"

must_fail "a secret in the ConfigMap" "is a secret|invalid propertyName" "${DEV[@]}" --set config.OPERATOR_TOKEN=x
must_fail "a credential-looking ConfigMap key" "looks like a credential" "${DEV[@]}" --set config.PGPASSWORD=x
must_fail "role-owned settings in config" "set it there instead" "${DEV[@]}" --set-string config.WORKER_DRAIN_TIMEOUT_S=60
must_fail "a worker grace period below drain + 10 s" "must be at least drainTimeoutSeconds" \
  "${DEV[@]}" --set worker.drainTimeoutSeconds=40 --set worker.terminationGracePeriodSeconds=45
must_fail "an API grace period below preStop + shutdown" "must exceed preStopSleepSeconds" \
  "${DEV[@]}" --set api.terminationGracePeriodSeconds=25
must_fail "no database configured" "secrets.existingSecret" 
must_fail "PgBouncer without a direct URL for migrations" "DATABASE_DIRECT_URL" "${DEV[@]}" \
  --set-string config.DB_PGBOUNCER=true
must_fail "unknown values (schema)" "additional properties" "${DEV[@]}" --set worker.concurency=8
must_fail "non-string config values (schema)" "config" "${DEV[@]}" --set config.DB_POOL_SIZE=10
echo "helm chart checks passed (manifests in $OUT_DIR)"
