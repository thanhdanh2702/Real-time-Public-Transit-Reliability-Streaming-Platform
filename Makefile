SHELL := /usr/bin/env bash

.DEFAULT_GOAL := help

.PHONY: help bootstrap up down ps logs tools-up airflow-up apps-up topics smoke lint format test dbt-parse config-check

help:
	@echo "TransitPulse local commands"
	@echo "  make bootstrap    Prepare .env and start core infrastructure"
	@echo "  make up           Start Kafka, Spark, PostgreSQL, and topic initialization"
	@echo "  make down         Stop services and preserve volumes"
	@echo "  make tools-up     Start optional Kafka UI"
	@echo "  make airflow-up   Initialize and start optional Airflow services"
	@echo "  make apps-up      Start optional application containers after implementation"
	@echo "  make topics       Reconcile Kafka topics"
	@echo "  make smoke        Run infrastructure smoke checks"
	@echo "  make config-check Validate YAML, JSON, TOML, and Compose configuration"
	@echo "  make lint         Run Ruff checks"
	@echo "  make format       Format Python sources with Ruff"
	@echo "  make test         Run tests when test modules exist"

bootstrap:
	@./scripts/bootstrap-local.sh

up:
	docker compose up -d postgres kafka kafka-init spark-master spark-worker

down:
	docker compose down --remove-orphans

ps:
	docker compose ps

logs:
	docker compose logs -f postgres kafka spark-master spark-worker

tools-up:
	docker compose --profile tools up -d kafka-ui

airflow-up:
	docker compose --profile orchestration up airflow-init
	docker compose --profile orchestration up -d airflow-api-server airflow-scheduler

apps-up:
	docker compose --profile apps up -d producer dashboard

topics:
	docker compose run --rm kafka-init

smoke:
	@./scripts/smoke-test.sh

config-check:
	docker compose config --quiet
	python -c "import json, pathlib; [json.load(path.open()) for path in pathlib.Path('contracts').glob('*.json')]"
	python -c "import pathlib, yaml; [yaml.safe_load(path.read_text()) for path in pathlib.Path('configs').glob('*.yaml')]"
	python -c "import tomllib, pathlib; tomllib.loads(pathlib.Path('pyproject.toml').read_text())"

lint:
	python -m ruff check producer spark dashboard airflow tests

format:
	python -m ruff format producer spark dashboard airflow tests

test:
	@if find producer spark dashboard airflow tests -name 'test_*.py' -print -quit | grep -q .; then python -m pytest; else echo "No tests implemented yet."; fi

dbt-parse:
	dbt parse --project-dir dbt --profiles-dir dbt
