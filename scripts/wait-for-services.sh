#!/usr/bin/env bash
set -euo pipefail

host="${1:-localhost}"
port="${2:-5432}"
timeout_seconds="${3:-60}"

deadline=$((SECONDS + timeout_seconds))
until (echo >"/dev/tcp/${host}/${port}") >/dev/null 2>&1; do
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for ${host}:${port}." >&2
    exit 1
  fi
  sleep 1
done

echo "${host}:${port} is reachable."
