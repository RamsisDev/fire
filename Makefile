.PHONY: install fmt lint test run

install:
	uv sync --group dev

fmt:
	uv run ruff format .

lint:
	uv run ruff format --check .
	uv run ruff check .

test:
	uv run pytest

run:
	uv run python -m fire.apps.incident_engine
