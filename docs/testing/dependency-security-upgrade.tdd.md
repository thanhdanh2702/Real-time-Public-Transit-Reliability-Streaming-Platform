# Dependency security upgrade evidence

## Goal

Keep local development and runtime images above the patched versions reported by
`pip-audit` for `urllib3` and `virtualenv`.

## RED evidence

Before the dependency constraints were updated, the following command failed:

```text
uvx pip-audit --path .venv/lib/python3.12/site-packages
```

It reported `urllib3 2.7.0` and `virtualenv 21.7.3` as vulnerable. Inspection also
found `urllib3 2.5.0` in the Airflow image.

## Guarantees

| What is guaranteed | Validation |
|---|---|
| Producer and dashboard installations require `urllib3>=2.8,<3` | Inspect `pyproject.toml` and rebuild both extras |
| Developer installations require `urllib3>=2.8,<3` and `virtualenv>=21.7.13,<22` | Install `.[dev]` and rerun `pip-audit` |
| Airflow installations require `urllib3>=2.8,<3` | Rebuild the Airflow image and inspect the installed package |
| Airflow and dbt retain compatible protobuf and OpenTelemetry versions | Pin `protobuf<6.32` and the Prometheus exporter matching OpenTelemetry SDK 1.45, then run `python -m pip check` |

## Known gap

The repository does not currently commit a Python lock file. The lower bounds prevent
installation of the affected versions, while exact transitive versions may still vary
between builds.

The full Airflow image contains a large provider set, and a complete requirements-file
audit did not finish in a reasonable local smoke-test window. The packages addressed by
this change were verified directly, the local environment audit is clean, and the rebuilt
Airflow image passes `pip check`.

## GREEN evidence

```text
uvx pip-audit --path .venv/lib/python3.12/site-packages
No known vulnerabilities found

docker run --rm --entrypoint python transitpulse-airflow:local -m pip check
No broken requirements found.

PYSPARK_PYTHON=.venv/bin/python PYSPARK_DRIVER_PYTHON=.venv/bin/python \
  .venv/bin/python -m pytest spark/tests/unit -q
57 passed

.venv/bin/python -m pytest airflow/tests dashboard/tests producer/tests -q
78 passed
```

The recreated Airflow services loaded `refresh_transitpulse_dashboard_marts`, `dbt debug`
connected successfully to PostgreSQL, and the first scheduled refresh completed with
status `success`.
