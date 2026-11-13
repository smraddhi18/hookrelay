"""Milestone 9b: invariants of the rendered Helm chart (skipped when ``helm`` is not on PATH).

The chart duplicates a few facts the code owns (console scripts, env var names, metric names,
probe commands, shutdown budgets); these tests pin them together so a rename on either side
fails here instead of in a cluster.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import pytest

from hookrelay import healthcheck
from hookrelay.core.config import Settings
from hookrelay.observability.metrics import SERVICE_RELAY, build_metrics, render_metrics

yaml = pytest.importorskip("yaml")

ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "deploy" / "helm" / "hookrelay"
CI_VALUES = ROOT / "deploy" / "helm" / "ci"
HELM = shutil.which("helm")
pytestmark = pytest.mark.skipif(HELM is None, reason="helm is not installed")

FAKE_DB = "postgresql://hookrelay:fake-ci-password@postgres:5432/hookrelay"
FAKE_KEY = "ci-only-not-a-secret-EXAMPLE-key"
DEV = ["--set", f"secrets.values.DATABASE_URL={FAKE_DB}"]


def render(*args: str) -> list[dict[str, Any]]:
    assert HELM is not None
    out = subprocess.run(  # noqa: S603 - fixed binary, test-controlled arguments
        [HELM, "template", "hr", str(CHART), "--namespace", "hookrelay", *args],
        capture_output=True,
        text=True,
        check=True,
        timeout=60,
    )
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def render_fails(*args: str) -> str:
    assert HELM is not None
    out = subprocess.run(  # noqa: S603
        [HELM, "template", "hr", str(CHART), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    assert out.returncode != 0, out.stdout
    return out.stderr


def by_kind(docs: list[dict[str, Any]], kind: str) -> dict[str, dict[str, Any]]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def pod_spec(doc: dict[str, Any]) -> dict[str, Any]:
    if doc["kind"] == "CronJob":
        return dict(doc["spec"]["jobTemplate"]["spec"]["template"]["spec"])
    if doc["kind"] == "Pod":
        return dict(doc["spec"])
    return dict(doc["spec"]["template"]["spec"])


def env_of(container: dict[str, Any]) -> dict[str, str]:
    return {e["name"]: e.get("value", "") for e in container.get("env", [])}


@pytest.fixture(scope="module")
def full() -> list[dict[str, Any]]:
    return render("-f", str(CI_VALUES / "values-full.yaml"))


@pytest.fixture(scope="module")
def dev() -> list[dict[str, Any]]:
    return render(*DEV, "--set", f"secrets.values.SECRETS_ENCRYPTION_KEY={FAKE_KEY}")


def test_grace_periods_cover_the_shutdown_budgets(full: list[dict[str, Any]]) -> None:
    deps = by_kind(full, "Deployment")
    worker = pod_spec(deps["hr-hookrelay-worker"])
    drain = float(env_of(worker["containers"][0])["WORKER_DRAIN_TIMEOUT_S"])
    assert worker["terminationGracePeriodSeconds"] >= drain + 10

    api = pod_spec(deps["hr-hookrelay-api"])
    container = api["containers"][0]
    graceful = int(env_of(container)["API_GRACEFUL_SHUTDOWN_S"])
    sleep = int(container["lifecycle"]["preStop"]["exec"]["command"][1])
    assert api["terminationGracePeriodSeconds"] > sleep + graceful

    # The defaults match the code's own defaults (Settings), not just each other.
    defaults = Settings.model_fields
    dev_deps = by_kind(render(*DEV), "Deployment")
    dev_worker = env_of(pod_spec(dev_deps["hr-hookrelay-worker"])["containers"][0])
    assert float(dev_worker["WORKER_DRAIN_TIMEOUT_S"]) == defaults["worker_drain_timeout_s"].default
    dev_api = env_of(pod_spec(dev_deps["hr-hookrelay-api"])["containers"][0])
    assert int(dev_api["API_GRACEFUL_SHUTDOWN_S"]) == defaults["api_graceful_shutdown_s"].default


def test_migration_job_is_a_pre_install_and_pre_upgrade_hook(dev: list[dict[str, Any]]) -> None:
    hooks = {
        (d["kind"], d["metadata"]["name"]): d["metadata"].get("annotations", {})
        for d in dev
        if "helm.sh/hook" in d["metadata"].get("annotations", {})
    }
    job = hooks[("Job", "hr-hookrelay-migrate")]
    assert job["helm.sh/hook"] == "pre-install,pre-upgrade"
    assert "before-hook-creation" in job["helm.sh/hook-delete-policy"]
    # Its ServiceAccount and Secret are hooks too, created first (the chart's own do not
    # exist yet at pre-install).
    for kind in ("ServiceAccount", "Secret"):
        dep = hooks[(kind, "hr-hookrelay-migrate")]
        assert dep["helm.sh/hook"] == "pre-install,pre-upgrade"
        assert int(dep["helm.sh/hook-weight"]) < int(job["helm.sh/hook-weight"])
    spec = pod_spec(by_kind(dev, "Job")["hr-hookrelay-migrate"])
    container = spec["containers"][0]
    assert container["command"] == ["alembic", "upgrade", "head"]
    assert spec["serviceAccountName"] == "hr-hookrelay-migrate"
    assert container["envFrom"] == [{"secretRef": {"name": "hr-hookrelay-migrate"}}]
    # Settings are inlined (the ConfigMap does not exist yet either).
    assert "configMapRef" not in str(container["envFrom"])
    assert env_of(container)["APP_ENV"] == "production"
    hook_secret = by_kind(dev, "Secret")["hr-hookrelay-migrate"]["stringData"]
    assert set(hook_secret) == {"DATABASE_URL", "SECRETS_ENCRYPTION_KEY"}


def test_migration_job_uses_the_existing_secret(full: list[dict[str, Any]]) -> None:
    assert "hr-hookrelay-migrate" not in by_kind(full, "Secret")
    container = pod_spec(by_kind(full, "Job")["hr-hookrelay-migrate"])["containers"][0]
    assert container["envFrom"] == [{"secretRef": {"name": "hookrelay-prod"}}]
    assert env_of(container)["DB_PGBOUNCER"] == "true"


def test_no_secrets_in_the_configmap(dev: list[dict[str, Any]], full: list[dict[str, Any]]) -> None:
    for docs in (dev, full):
        data = next(iter(by_kind(docs, "ConfigMap").values()))["data"]
        assert not set(data) & {
            "DATABASE_URL",
            "DATABASE_DIRECT_URL",
            "REDIS_URL",
            "OPERATOR_TOKEN",
            "SECRETS_ENCRYPTION_KEY",
            "SECRETS_ENCRYPTION_KEY_PREVIOUS",
            "METRICS_TOKEN",
        }
        blob = str(data)
        assert FAKE_DB not in blob
        assert FAKE_KEY not in blob
    assert "hookrelay-prod" not in [d["metadata"]["name"] for d in full if d["kind"] == "Secret"]
    # Refused twice over: by values.schema.json and by the template guard rail.
    refused = render_fails(*DEV, "--set", "config.OPERATOR_TOKEN=x")
    assert "invalid propertyName" in refused or "is a secret" in refused


def test_commands_are_real_console_scripts(full: list[dict[str, Any]]) -> None:
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    commands = {
        doc["metadata"]["name"]: pod_spec(doc)["containers"][0]["command"][0]
        for doc in full
        if doc["kind"] in ("Deployment", "CronJob")
    }
    assert commands == {
        "hr-hookrelay-api": "hookrelay",
        "hr-hookrelay-relay": "hookrelay-relay",
        "hr-hookrelay-worker": "hookrelay-worker",
        "hr-hookrelay-retention": "hookrelay-retention",
    }
    assert set(commands.values()) <= set(scripts)


def test_probes(full: list[dict[str, Any]]) -> None:
    deps = by_kind(full, "Deployment")
    api = pod_spec(deps["hr-hookrelay-api"])["containers"][0]
    assert api["livenessProbe"]["httpGet"]["path"] == "/health"
    assert api["startupProbe"]["httpGet"]["path"] == "/health"
    assert api["readinessProbe"]["httpGet"]["path"] == "/ready"
    for role in ("relay", "worker"):
        container = pod_spec(deps[f"hr-hookrelay-{role}"])["containers"][0]
        env = env_of(container)
        for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
            cmd = container[probe]["exec"]["command"]
            assert cmd == ["python", "-m", "hookrelay.healthcheck", "--heartbeat", role]
            assert role in healthcheck.HEARTBEAT_DEFAULTS
        # The heartbeat lives on the writable /tmp emptyDir (root FS is read-only).
        assert env[f"{role.upper()}_HEARTBEAT_FILE"].startswith("/tmp/")  # noqa: S108
        assert int(env[f"{role.upper()}_HEARTBEAT_MAX_AGE_S"]) >= 5


def test_every_env_var_is_a_real_setting(full: list[dict[str, Any]]) -> None:
    """Settings ignores unknown env vars, so a typo in the chart would be silently dropped."""
    fields = {name.upper() for name in Settings.model_fields}
    names: set[str] = set(next(iter(by_kind(full, "ConfigMap").values()))["data"])
    for doc in full:
        if doc["kind"] in ("Deployment", "CronJob", "Job"):
            for container in pod_spec(doc)["containers"]:
                names |= set(env_of(container))
    assert names
    assert names <= fields, sorted(names - fields)


def test_pods_are_hardened(full: list[dict[str, Any]]) -> None:
    pods = [d for d in full if d["kind"] in ("Deployment", "CronJob", "Job", "Pod")]
    assert len(pods) == 6
    for doc in pods:
        spec = pod_spec(doc)
        assert spec["securityContext"]["runAsNonRoot"] is True
        assert spec["automountServiceAccountToken"] is False
        for container in spec["containers"]:
            sc = container["securityContext"]
            assert sc["readOnlyRootFilesystem"] is True
            assert sc["allowPrivilegeEscalation"] is False
            assert sc["capabilities"]["drop"] == ["ALL"]
            assert "limits" in container["resources"]
            if doc["kind"] != "Pod":
                assert {"name": "tmp", "mountPath": "/tmp"} in container["volumeMounts"]  # noqa: S108


def test_autoscalers_own_the_replica_counts(full: list[dict[str, Any]]) -> None:
    deps = by_kind(full, "Deployment")
    assert "replicas" not in deps["hr-hookrelay-api"]["spec"]
    assert "replicas" not in deps["hr-hookrelay-worker"]["spec"]
    assert deps["hr-hookrelay-relay"]["spec"]["replicas"] == 2
    hpa = by_kind(full, "HorizontalPodAutoscaler")["hr-hookrelay-api"]
    assert hpa["apiVersion"] == "autoscaling/v2"
    assert hpa["spec"]["scaleTargetRef"]["name"] == "hr-hookrelay-api"
    assert hpa["spec"]["metrics"][0]["resource"]["name"] == "cpu"
    static = by_kind(render(*DEV), "Deployment")
    assert static["hr-hookrelay-worker"]["spec"]["replicas"] == 2


def test_keda_queries_use_metrics_the_code_exports(full: list[dict[str, Any]]) -> None:
    scaled = by_kind(full, "ScaledObject")["hr-hookrelay-worker"]
    assert scaled["spec"]["scaleTargetRef"]["name"] == "hr-hookrelay-worker"
    assert scaled["spec"]["fallback"]["replicas"] >= 1
    triggers = {t["name"]: t for t in scaled["spec"]["triggers"]}
    assert set(triggers) == {"delivery-demand", "fanout-backlog"}
    demand = triggers["delivery-demand"]["metadata"]
    assert demand["ignoreNullValues"] == "false"
    assert int(demand["threshold"]) == 24  # 75% of worker.concurrency=32
    exported = render_metrics(build_metrics(SERVICE_RELAY))[0].decode()
    for trigger in triggers.values():
        query = trigger["metadata"]["query"]
        assert 'namespace="hookrelay"' in query
        for metric in re.findall(r"hookrelay_[a-z_]+", query):
            assert f"# TYPE {metric} gauge" in exported, metric


def test_pdbs_and_monitors(full: list[dict[str, Any]]) -> None:
    pdbs = by_kind(full, "PodDisruptionBudget")
    assert set(pdbs) == {"hr-hookrelay-api", "hr-hookrelay-relay", "hr-hookrelay-worker"}
    for name, pdb in pdbs.items():
        assert pdb["spec"]["maxUnavailable"] == 1
        assert pdb["spec"]["selector"]["matchLabels"]["app.kubernetes.io/component"] in name
    sm = by_kind(full, "ServiceMonitor")["hr-hookrelay-api"]
    assert sm["spec"]["endpoints"][0]["honorLabels"] is True
    pm = by_kind(full, "PodMonitor")["hr-hookrelay-background"]
    assert pm["spec"]["selector"]["matchExpressions"][0]["values"] == ["relay", "worker"]


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (
            [
                "--set",
                "worker.drainTimeoutSeconds=40",
                "--set",
                "worker.terminationGracePeriodSeconds=45",
            ],
            "must be at least drainTimeoutSeconds",
        ),
        (["--set", "api.terminationGracePeriodSeconds=20"], "must exceed preStopSleepSeconds"),
        (["--set-string", "config.DB_PGBOUNCER=true"], "DATABASE_DIRECT_URL"),
        (["--set-string", "config.METRICS_PORT=9"], "set it there instead"),
    ],
)
def test_guard_rails(args: list[str], message: str) -> None:
    assert message in render_fails(*DEV, *args)
