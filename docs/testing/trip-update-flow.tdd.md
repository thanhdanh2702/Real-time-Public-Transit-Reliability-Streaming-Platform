# TripUpdate flow test evidence

## Scope

No source plan was provided. The journey was derived from the implemented flow: as a data
engineer, I want TripUpdate events to be parsed, validated, deduplicated, and flattened so that
each valid stop prediction is ready for downstream processing.

## Guarantees

| Guarantee | Test | Result |
|---|---|---|
| Kafka bytes are parsed while nested TripUpdate data and Kafka metadata are preserved | `test_parse_trip_update_preserves_nested_event_and_kafka_metadata` | PASS |
| Valid events, including null direction and empty stop lists, are separated from invalid direction values | `test_trip_update_quality_splits_valid_and_invalid_events` | PASS |
| Each stop update becomes one flat row with the expected trip and delay fields | `test_transform_trip_update_explodes_and_flattens_stop_updates` | PASS |
| Duplicate event IDs are removed across micro-batches using shared event-time logic | `test_events_are_deduplicated_across_micro_batches` | PASS |

## Evidence

- Baseline: `7 passed` before adding TripUpdate regression tests.
- Final Spark suite: `12 passed in 6.67s`.
- Target module coverage: `47/47 statements`, `100%`.
- Ruff check and format check: passed.

The tests covered existing production behavior and passed on their first execution, so no
production fix or RED/GREEN implementation cycle was required. Live Kafka connectivity,
console output, and `main.py` orchestration remain smoke-test responsibilities.
