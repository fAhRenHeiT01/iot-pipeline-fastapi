.PHONY: install lint format test test-unit test-integration run-producer run-api run-local-pipeline run-local-generator status-local-db reset-local-db clean

PYTHON ?= python
PIP ?= pip

install:
	$(PIP) install -e ".[dev]"

lint:
	ruff check .

format:
	ruff format .

test:
	pytest

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration

run-producer:
	fleet-producer start --rate 5 --duration 30

run-api:
	uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

run-local-pipeline:
	$(PYTHON) -m local_pipeline.main --source kafka

run-local-generator:
	$(PYTHON) -m local_pipeline.main --source generator

status-local-db:
	$(PYTHON) -m local_pipeline.main --status

reset-local-db:
	$(PYTHON) -m local_pipeline.main --reset

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .ruff_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
