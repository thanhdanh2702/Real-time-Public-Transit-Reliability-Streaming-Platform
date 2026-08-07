#!/usr/bin/env bash
set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
partitions="${KAFKA_TOPIC_PARTITIONS:-3}"
replication_factor="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
standard_retention="${KAFKA_RETENTION_MS:-604800000}"
dlq_retention="${KAFKA_DLQ_RETENTION_MS:-2592000000}"
kafka_bin="/opt/kafka/bin"

declare -A topics=(
  [transit.vehicle_positions.v1]="$standard_retention"
  [transit.trip_updates.v1]="$standard_retention"
  [transit.service_alerts.v1]="$standard_retention"
  [transit.operational_alerts.v1]="$standard_retention"
  [transit.dead_letter.v1]="$dlq_retention"
)

for topic in "${!topics[@]}"; do
  "$kafka_bin/kafka-topics.sh" \
    --bootstrap-server "$bootstrap_servers" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor "$replication_factor" \
    --config "retention.ms=${topics[$topic]}" \
    --config cleanup.policy=delete

  "$kafka_bin/kafka-configs.sh" \
    --bootstrap-server "$bootstrap_servers" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "retention.ms=${topics[$topic]},cleanup.policy=delete"
done

"$kafka_bin/kafka-topics.sh" --bootstrap-server "$bootstrap_servers" --list
