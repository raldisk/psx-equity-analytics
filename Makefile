.DEFAULT_GOAL := help

PSX_DATA_ROOT ?= /opt/airflow/data

.PHONY: help install test test-integration lint format up down clean load-test

help:
	@echo "psx-analytics — targets:"
	@echo "  install          Install all dependencies (dev + stats extras)"
	@echo "  test             Run regression suite with coverage"
	@echo "  test-integration Run integration tests (requires running services)"
	@echo "  lint             Run pre-commit on all files"
	@echo "  format           Format with black + ruff"
	@echo "  up               Start Docker services (dev compose)"
	@echo "  down             Stop Docker services"
	@echo "  clean            Remove dbt/target, __pycache__, htmlcov"
	@echo "  load-test        Run Locust load test against serving layer"

install:
	pip install -e ".[dev,stats]"

test:
	python -m pytest tests/ \
	  --cov=scripts --cov=serving --cov=airflow/dags \
	  --cov-report=term-missing --cov-fail-under=80 \
	  -v

test-integration:
	python -m pytest tests/integration/ -v --tb=short

lint:
	pre-commit run --all-files

format:
	black scripts/ serving/ airflow/ tests/
	ruff --fix scripts/ serving/ airflow/ tests/

up:
	docker compose -f infra/docker/docker-compose.yml up -d

down:
	docker compose -f infra/docker/docker-compose.yml down

clean:
	rm -rf dbt/target dbt/dbt_packages htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

load-test:
	locust -f tests/load/locustfile.py --host=http://localhost:$(PSX_API_PORT) \
	  --headless -u 50 -r 5 -t 60s
