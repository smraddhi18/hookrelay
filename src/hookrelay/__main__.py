"""``python -m hookrelay`` / ``hookrelay``: run the API with uvicorn using validated settings."""

from __future__ import annotations

import uvicorn

from hookrelay.core.config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "hookrelay.main:create_app",
        factory=True,
        host=settings.host,
        port=settings.port,
        log_config=None,  # logging is configured by the app factory (structlog)
        access_log=False,  # replaced by the structured access log in RequestContextMiddleware
        proxy_headers=True,
        timeout_graceful_shutdown=20,
    )


if __name__ == "__main__":
    main()
