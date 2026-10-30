# Thin wrappers over the exact commands CI runs.
.PHONY: install lint format typecheck test test-unit test-integration migrate migrations-roundtrip run check up down

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

check: lint typecheck migrations-roundtrip test

up:
	docker compose up --build

down:
	docker compose down -v
