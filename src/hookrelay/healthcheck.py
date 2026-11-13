"""Container probes: ``python -m hookrelay.healthcheck [--heartbeat relay|worker]``.

Without arguments this probes the API's liveness (not readiness) on purpose: Docker/compose and
Kubernetes restart a container whose liveness check fails, and a database outage must not cause
every API container to be restarted.

``--heartbeat relay|worker`` checks the age of the heartbeat file the relay / worker touch on
every loop iteration (also on iterations whose database work failed, so a Postgres outage does
not get them killed; a wedged event loop does). It reads ``RELAY_HEARTBEAT_FILE`` /
``RELAY_HEARTBEAT_MAX_AGE_S`` (or the ``WORKER_`` pair) straight from the environment with the
same defaults as :class:`hookrelay.core.config.Settings` (a unit test pins them together).

Only the stdlib is imported: Kubernetes runs exec probes every few seconds in every pod, and
``python -m hookrelay.worker --healthcheck`` imports the whole worker (SQLAlchemy, httpx,
OpenTelemetry, pydantic settings): about 0.7 s of CPU per probe against about 0.05 s here,
which on a small CPU limit is enough to throttle the pod and time the probe out.
"""

from __future__ import annotations

import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from pathlib import Path

_OK = 200

# role -> (heartbeat file default, max age default); must match Settings (tested).
HEARTBEAT_DEFAULTS: dict[str, tuple[str, int]] = {
    "relay": ("/tmp/hookrelay-relay.heartbeat", 60),  # noqa: S108 - container-local
    "worker": ("/tmp/hookrelay-worker.heartbeat", 60),  # noqa: S108 - container-local
}
_USAGE = "usage: python -m hookrelay.healthcheck [--heartbeat relay|worker]"


def probe(url: str, timeout_s: float = 2.0) -> bool:
    if not url.startswith(("http://127.0.0.1", "http://localhost")):
        raise ValueError("healthcheck only probes the local process")
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:  # noqa: S310 - fixed local URL
            return bool(resp.status == _OK)
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return False


def heartbeat_is_fresh(path: str, max_age_s: float, *, now: float | None = None) -> bool:
    """True when ``path`` exists and was modified at most ``max_age_s`` seconds ago."""
    try:
        modified = Path(path).stat().st_mtime
    except OSError:
        return False
    return ((time.time() if now is None else now) - modified) <= max_age_s


def _env(name: str) -> str | None:
    # pydantic-settings matches env names case-insensitively; containers use upper case.
    value = os.environ.get(name)
    if value is None:
        value = os.environ.get(name.lower())
    return value.strip() if value is not None and value.strip() else None


def heartbeat_settings(role: str) -> tuple[str, float]:
    """(heartbeat file, max age seconds) for ``role`` from the environment or the defaults."""
    default_file, default_age = HEARTBEAT_DEFAULTS[role]
    prefix = role.upper()
    path = _env(f"{prefix}_HEARTBEAT_FILE") or default_file
    raw_age = _env(f"{prefix}_HEARTBEAT_MAX_AGE_S")
    try:
        max_age = float(raw_age) if raw_age is not None else float(default_age)
    except ValueError:
        max_age = float(default_age)
    return path, max_age


def check_heartbeat(role: str, *, now: float | None = None) -> bool:
    path, max_age = heartbeat_settings(role)
    return heartbeat_is_fresh(path, max_age, now=now)


def main(argv: Sequence[str] | None = None) -> int:
    args = list(argv or ())
    if not args:
        port = os.environ.get("PORT", "8000")
        return 0 if probe(f"http://127.0.0.1:{port}/health") else 1
    if len(args) == 2 and args[0] == "--heartbeat" and args[1] in HEARTBEAT_DEFAULTS:  # noqa: PLR2004
        return 0 if check_heartbeat(args[1]) else 1
    sys.stderr.write(_USAGE + "\n")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
