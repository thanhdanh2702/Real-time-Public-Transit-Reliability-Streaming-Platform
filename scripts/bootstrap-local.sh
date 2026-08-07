#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required but was not found." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required." >&2
  exit 1
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from .env.example. Review local passwords before continuing."
fi

mkdir -p airflow/logs replay/data checkpoints

docker compose config --quiet
docker compose up -d postgres kafka kafka-init spark-master spark-worker

echo "Core infrastructure started. Run 'make smoke' after the services become healthy."
