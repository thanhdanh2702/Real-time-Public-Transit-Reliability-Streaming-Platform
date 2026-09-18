# TripUpdate flow test evidence

## Scope

The journey is: as a data engineer, I want TripUpdate events to be parsed, validated,
deduplicated, flattened, and persisted so that each valid stop prediction is queryable in
PostgreSQL.

## Guarantees

| Guarantee | Test | Result |
|---|---|---|
| Kafka bytes are parsed while nested TripUpdate data and Kafka metadata are preserved | `test_parse_trip_update_preserves_nested_event_and_kafka_metadata` | PASS |
| Valid events, including null direction and empty stop lists, are separated from invalid direction values, missing stop references, and negative stop sequences | `test_trip_update_quality_splits_valid_and_invalid_events` | PASS |
| Each stop update becomes one flat row with the expected trip and delay fields | `test_transform_trip_update_explodes_and_flattens_stop_updates` | PASS |
| Duplicate event IDs are removed across micro-batches using shared event-time logic | `test_events_are_deduplicated_across_micro_batches` | PASS |
| The main job wires Kafka parsing through the PostgreSQL sink and always stops Spark on failure | `test_main_writes_trip_stop_updates_to_postgres`, `test_main_stops_spark_when_stream_setup_fails` | PASS |

## Evidence

- RED: orchestration test failed because the job still used the console sink; quality test failed
  because invalid nested stop references reached the valid output.
- GREEN: focused TripUpdate suite passed (`5 passed`), and the repository regression suite passed
  (`40 passed`).
- Ruff and Docker Compose configuration checks passed.
- Runtime smoke test on 2026-09-18 passed: the Spark application used 2/2 worker cores and
  2048/2048 MB, then persisted 17,704 stop-update rows for 1,293 trips from live Kafka data.
- After the smoke test, producer, Kafka, Spark master, and Spark worker were stopped. PostgreSQL
  remained healthy and retained all 17,704 TripUpdate rows.

The sink uses a dedicated checkpoint and PostgreSQL's `(event_id, stop_update_index)` primary key
with `ON CONFLICT DO NOTHING`, so replayed micro-batches do not create duplicate rows.

An event with an empty `stop_time_updates` list is contract-valid but produces no row in the
stop-grain table. If canceled trips must remain queryable even without stop predictions, add a
separate trip-level staging table rather than inventing a synthetic stop row.
