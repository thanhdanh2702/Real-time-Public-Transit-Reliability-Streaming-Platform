# Dashboard database helper: test evidence

The user journey is to read PostgreSQL mart queries from dashboard pages through one reusable database helper.

| Guarantee | Test | Result |
| --- | --- | --- |
| Environment configuration creates a reusable PostgreSQL engine, including passwords with special characters | `test_database_engine_uses_environment_variables` | Pass |
| A missing password fails clearly | `test_database_engine_requires_credentials` | Pass |
| Query parameters are bound rather than interpolated into SQL | `test_read_dataframe_binds_parameters` | Pass |
| Query results are returned as a DataFrame | `test_read_dataframe_returns_query_rows` | Pass |

RED: `.venv/bin/python -m pytest dashboard/tests/test_queries.py -q -o addopts=` failed with four missing-function errors before implementation (`7adc721`).

GREEN: The same target passed after implementation (`57e63bf`): 4 passed. With `--cov=dashboard.queries.database --cov-report=term-missing`, coverage was 100%. Ruff check and format check passed.

The query tests use an in-memory SQLite database to verify SQL binding and DataFrame output. A live query against `mart.vehicle_latest_state` remains unverified because the Docker daemon was not running during this work.
