# TripUpdate/bunching implementation evidence — 2026-09-09

## Scope and journey

The user selected one Spark job, without a processed Kafka topic. The journey is to consume
raw TripUpdate events once, assemble snapshots split across micro-batches, detect close trip
pairs, confirm repeated observations, enforce cooldown and print contract-valid active alerts.

## RED → GREEN

- New snapshot/restart tests initially failed with `ModuleNotFoundError` for the missing pipeline.
- A repeated-stop test failed with headway **30**, expected **90**. Detector now keeps each
  trip's next eligible visit at a stop before pairing trips.
- A filtering test returned `a, train, future`, expected only `a`. The query now applies the
  bus allowlist and bounds feed time relative to ingestion time.
- All these tests pass after implementation. No Git commit or push was performed in this task.

## Verification

Commands run from the repository root, with both `PYSPARK_PYTHON` and
`PYSPARK_DRIVER_PYTHON` set to `/Users/thanhdanh/transitpulse/.venv/bin/python`:

```bash
.venv/bin/python -m pytest -q -o addopts='' --tb=short \
  --cov=spark.jobs.bunching_detector \
  --cov=spark.jobs.route_reliability.main \
  --cov=spark.common.sources.kafka_source --cov-report=term-missing
```

Result: **62 passed in 16.73s**, changed-flow coverage **203/206 statements (99%)**.

| Guarantee | Test target | Result |
|---|---|---|
| Snapshot split across batches survives processor restart | `test_split_snapshot_restart_confirmation_cooldown_and_retry` | PASS |
| Replayed batch returns the same outbox and does not confirm twice | Same test; `test_batch_limit_and_console_failure_retry` | PASS |
| Two observations confirm; cooldown suppresses and later permits output | `test_split_snapshot_restart_confirmation_cooldown_and_retry` | PASS |
| Duplicate snapshot cannot confirm; oversized state transaction rolls back | `test_duplicate_snapshot_does_not_confirm_and_limits_rollback` | PASS |
| Late rows and stale predictions do not create extra observations | `test_late_snapshot_missing_observation_and_stale_prediction` | PASS |
| Missing/changed state or incompatible settings fails visibly | `test_empty_batch_settings_change_missing_state_and_old_retry` | PASS |
| 0/120/121-second boundaries, routes, directions, stops, canceled/skipped trips | `test_detector_boundaries_and_pair_isolation` | PASS |
| Null vehicle ID is permitted; identical known vehicle IDs cannot form a pair | Same test | PASS |
| Raw Kafka-shaped input → quality → persistent state → detector → console, with query restart | `spark/tests/streaming/test_bunching_stream.py` | PASS |
| Alert matches JSON contract, including date-time formats | `test_split_snapshot_restart_confirmation_cooldown_and_retry` | PASS |
| Both entry points launch the same job and Spark closes after failure | `test_bunching_main.py` | PASS |

Also passed:

- `.venv/bin/ruff check producer spark dashboard airflow tests`
- Ruff format check for all changed Python files.
- `.venv/bin/mypy --follow-imports=silent spark/jobs/bunching_detector spark/jobs/route_reliability/main.py spark/common/sources/kafka_source.py`
- `docker compose config --quiet`
- `git diff --check`

## Console smoke evidence and limits

The deterministic streaming fixture test was also run with `-s`. Output included:

```text
Batch: 0
Bunching alerts: 0 (showing at most 20)
Batch: 1
Bunching alerts: 0 (showing at most 20)
Batch: 2
Bunching alerts: 1 (showing at most 20)
```

The emitted alert contained route `1`, leading trip `a`, following trip `b`, observed headway
`90`, threshold `120`, and distinct first/last observation times. This is a real local Spark
Structured Streaming test over files shaped like Kafka rows, not a live Kafka/MBTA test.
Docker image build/download and live API ingestion were not run. See the
[runbook](trip-update-bunching-runbook.md) for deployment and local-state limitations.

Remaining uncovered lines: validation of an empty route allowlist, the candidate-count safety
cap, and the direct `__main__` guard in route_reliability (the called function itself is tested).
Line coverage is not a claim of exhaustive business-rule correctness or production scale.
