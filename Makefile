# Thin wrappers over the exact commands CI runs.
.PHONY: install lint format typecheck test test-unit test-integration test-chaos migrate migrations-roundtrip run relay worker mock-receiver loadtest-smoke check up down helm-check kind-smoke

install:
	uv sync --locked

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

typecheck:
	uv run mypy

test:
	uv run pytest

test-unit:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration

# Milestone 8: crash / Postgres-restart / slow-and-failing-receiver scenarios (needs TEST_DATABASE_URL).
test-chaos:
	uv run pytest tests/integration/test_chaos.py -v

migrate:
	uv run alembic upgrade head

migrations-roundtrip:
	uv run alembic upgrade head
	uv run alembic upgrade head
	uv run alembic check
	uv run alembic downgrade base
	uv run alembic upgrade head

run:
	uv run hookrelay

relay:
	uv run hookrelay-relay

worker:
	uv run hookrelay-worker

mock-receiver:
	uv run hookrelay-mock-receiver --port 9000

# Same k6 smoke as CI against a local stack (DATABASE_URL must point at a dedicated database).
loadtest-smoke:
	OUT_DIR=k6-ingest bash loadtest/run-local.sh ingest.js -e RATE=50 -e DURATION=10s
	OUT_DIR=k6-e2e bash loadtest/run-local.sh e2e.js -e RATE=25 -e DURATION=15s -e ENDPOINTS=2 \
	  -e 'RECEIVER_QUERY=latency_ms=10&jitter_ms=20&failure_rate=0.05&failure_status=500,503' \
	  -e E2E_P95_MS=10000 -e DRAIN_TIMEOUT_S=90

check: lint typecheck migrations-roundtrip test

up:
	docker compose up --build

down:
	docker compose down -v

# Milestone 9b: Helm chart static checks (helm + kubeconform on PATH) and the kind smoke
# (current kubectl context = a kind cluster with hookrelay:kind loaded; see README).
helm-check:
	bash deploy/helm/ci/validate.sh
	uv run pytest tests/unit/test_helm_chart.py -q

kind-smoke:
	bash deploy/helm/ci/kind-smoke.sh
