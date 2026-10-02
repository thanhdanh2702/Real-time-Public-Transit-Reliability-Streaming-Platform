# Dashboard overhaul TDD record

## Scope

The dashboard overhaul changed metric semantics, query grain, page behavior, Kafka/Spark
observability, and Airflow orchestration. Tests were added before or alongside the smallest
implementation changes for each behavior.

## RED cases captured

- weighted late rate and zero/null denominators;
- P90 at observation-sample grain;
- equal-length previous periods;
- minimum eligible sample threshold;
- visible time-series gaps and volume context;
- MBTA bus-only vehicle scope;
- live route/direction filtering and selection persistence;
- alert unknown/stale states;
- Kafka end offset minus Spark processed offset;
- retained DLQ count;
- Spark progress parsing and persistence;
- Airflow 3 DAG configuration and internal Execution API settings.

## GREEN evidence

At completion, run:

```bash
.venv/bin/python -m pytest dashboard/tests -q
.venv/bin/python -m pytest airflow/tests/test_dbt_refresh.py -q
docker compose run --rm --no-deps --entrypoint python3 \
  spark-master -m pytest spark/tests/unit -q
.venv/bin/ruff check dashboard spark airflow
```

Spark unit tests are intentionally verified inside the Spark image. This prevents false
failures caused by a host Python version differing from the Python interpreter used by
Spark workers.

## Runtime evidence

- Dashboard and Airflow images built successfully.
- Migration 007 applied without deleting existing data.
- The Airflow DAG loaded with no import errors.
- A DAG test completed all selected dbt models and tests.
- A task dispatched by Airflow `LocalExecutor` completed through the Airflow 3 Execution
  API and recorded a successful dbt run.
- Subsequent scheduled runs completed successfully with `max_active_runs=1`.
- A finite three-job smoke test wrote real progress for all applications, including Kafka
  source offsets and batch timing.
- Overview, Route Reliability, Live Operations, and Pipeline Health were inspected in the
  running Streamlit UI, including a narrow viewport.

## Regression boundary

The smoke test used separate checkpoint directories after existing checkpoints referenced
Kafka offsets no longer retained by the broker. Existing checkpoints and offsets were not
deleted or reset. Producer and Spark smoke services were stopped after verification.
