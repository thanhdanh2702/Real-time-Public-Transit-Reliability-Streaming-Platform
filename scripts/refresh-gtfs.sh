#!/usr/bin/env bash
# Load the project's trusted local settings, refresh raw GTFS, then validate dbt.
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

gtfs_python="${GTFS_PYTHON:-$project_root/.venv/bin/python}"
gtfs_dbt="${GTFS_DBT:-$project_root/.venv/bin/dbt}"

for executable in "$gtfs_python" "$gtfs_dbt"; do
  if ! command -v "$executable" >/dev/null 2>&1; then
    echo "Missing executable: $executable. Install project extras: .[gtfs,dbt]" >&2
    exit 1
  fi
done

"$gtfs_python" -m scripts.refresh_gtfs_static "$@"
"$gtfs_dbt" build --project-dir dbt --profiles-dir dbt \
  --select path:models/staging path:models/intermediate
