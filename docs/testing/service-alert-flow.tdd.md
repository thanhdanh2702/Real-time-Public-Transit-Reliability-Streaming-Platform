# ServiceAlert Spark flow evidence

## Scope

No source plan was provided. The derived journey is: as a data engineer, I want valid MBTA
ServiceAlert events to flow from Kafka through parsing, quality checks, event-time handling,
deduplication, and flattening so that affected entities are ready for a downstream sink.

## Guarantees

| Guarantee | Test | Result |
|---|---|---|
| Runtime configuration is passed to the Kafka source and console sink | `test_main_connects_service_alert_stream` | PASS |
| The processing stages are connected in the intended order and the query waits for termination | `test_main_connects_service_alert_stream` | PASS |
| Spark is stopped when stream setup fails | `test_main_stops_spark_when_stream_setup_fails` | PASS |
| Each informed entity becomes one flat row while alert periods and lineage are retained | `test_transform_service_alert_explodes_and_flattens_informed_entities` | PASS |

## Evidence

- RED: the two orchestration tests failed because `service_alert/main.py` did not expose the
  required pipeline dependencies.
- GREEN: targeted ServiceAlert tests passed: `5 passed in 5.99s`.
- Regression: all Spark unit tests passed: `16 passed in 7.26s`.
- Coverage for `spark.jobs.service_alert`: `34/35 statements`, `97%`.
- Ruff check and format check for the complete `spark` directory: passed.
- The local Spark test command sets `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to the project
  Python 3.12 virtual environment because the system `python3` is Python 3.14.

Live Kafka connectivity and console output remain smoke-test responsibilities.
