"""Container HEALTHCHECK: ``python -m hookrelay.healthcheck``.

Probes liveness (not readiness) on purpose: Docker/compose restart a container whose health
check fails, and a database outage must not cause every API container to be restarted.
Uses only the stdlib so the runtime image needs no curl/wget.
"""

from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request

_OK = 200


def probe(url: str, timeout_s: float = 2.0) -> bool:
    if not url.startswith(("http://127.0.0.1", "http://localhost")):
        raise ValueError("healthcheck only probes the local process")
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:  # noqa: S310 - fixed local URL
            return bool(resp.status == _OK)
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return False


def main() -> int:
    port = os.environ.get("PORT", "8000")
    return 0 if probe(f"http://127.0.0.1:{port}/health") else 1


if __name__ == "__main__":
    sys.exit(main())
