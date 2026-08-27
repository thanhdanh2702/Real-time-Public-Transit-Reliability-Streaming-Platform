# ServiceAlert quality fix evidence

## Scope

No source plan was provided. The derived journey is: as a data engineer, I want ServiceAlert
events to be parsed and validated so that malformed nested alert values do not enter downstream
processing.

## Evidence

| Guarantee | Test | Result |
|---|---|---|
| Alert payload and Kafka metadata survive parsing | `test_parse_service_alert_preserves_nested_event_and_kafka_metadata` | PASS |
| Null/0/1 directions and non-negative route types are enforced | `test_service_alert_quality_splits_invalid_nested_values` | PASS |
| Equal active-period boundaries are accepted and reversed periods are rejected | `test_service_alert_quality_splits_invalid_nested_values` | PASS |

- RED: test collection failed because `service_alert_quality` and `split_service_alert` did not exist.
- GREEN: targeted ServiceAlert tests passed: `2 passed`.
- Regression: complete Spark suite passed: `14 passed in 8.61s`.
- Coverage for `parsing.py` and `data_quality.py`: `52/52 statements`, `100%`.
- Ruff check and format check for the complete `spark` directory: passed.

Live Kafka ingestion and the ServiceAlert job `main.py` remain smoke-test responsibilities.
