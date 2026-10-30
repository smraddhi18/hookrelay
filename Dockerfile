# Multi-stage build: uv resolves the locked dependency set in a builder stage; the runtime
# stage gets only the virtualenv, the migrations and a non-root user.
ARG PYTHON_IMAGE=python:3.12-slim-bookworm

FROM ghcr.io/astral-sh/uv:0.13.6 AS uv

# ---------------------------------------------------------------------------- builder
FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /uvx /bin/
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=/usr/local/bin/python3 \
    UV_PROJECT_ENVIRONMENT=/app/.venv
WORKDIR /app

# 1) third-party dependencies only: this layer is reused until uv.lock changes
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-dev --no-install-project

# 2) the project itself, installed non-editable so the runtime needs no source tree
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# ---------------------------------------------------------------------------- runtime
FROM ${PYTHON_IMAGE} AS runtime
LABEL org.opencontainers.image.title="hookrelay" \
      org.opencontainers.image.description="Multi-tenant webhook delivery platform" \
      org.opencontainers.image.source="https://github.com/smraddhi18/hookrelay" \
      org.opencontainers.image.licenses="MIT"

RUN groupadd --system --gid 10001 hookrelay \
 && useradd --system --uid 10001 --gid hookrelay --no-create-home --shell /usr/sbin/nologin hookrelay

WORKDIR /app
COPY --from=builder --chown=hookrelay:hookrelay /app/.venv /app/.venv
COPY --chown=hookrelay:hookrelay alembic.ini ./
COPY --chown=hookrelay:hookrelay alembic ./alembic

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_ENV=production \
    LOG_FORMAT=json \
    PORT=8000

USER 10001:10001
EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=15s --retries=3 \
    CMD ["python", "-m", "hookrelay.healthcheck"]

# `hookrelay` = uvicorn with the app factory, structlog logging and graceful shutdown.
# Migrations are a separate one-shot command: `alembic upgrade head`.
CMD ["hookrelay"]
