#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

required_services=(postgres kafka spark-master spark-worker)
running_services="$(docker compose ps --status running --services)"

for service in "${required_services[@]}"; do
  if ! grep -qx "$service" <<<"$running_services"; then
    echo "Service is not running: $service" >&2
    exit 1
  fi
done

docker compose exec -T postgres \
  pg_isready -U "${POSTGRES_USER:-transitpulse_admin}" -d "${POSTGRES_DB:-transitpulse}"

topics="$(docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)"

expected_topics=(
  transit.vehicle_positions.v1
  transit.trip_updates.v1
  transit.service_alerts.v1
  transit.operational_alerts.v1
  transit.dead_letter.v1
)

for topic in "${expected_topics[@]}"; do
  if ! grep -qx "$topic" <<<"$topics"; then
    echo "Kafka topic is missing: $topic" >&2
    exit 1
  fi
done

echo "TransitPulse infrastructure smoke test passed."
