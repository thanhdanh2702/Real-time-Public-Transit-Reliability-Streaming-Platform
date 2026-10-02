# Route Reliability schema fix: test evidence

The Trip samples query and the dbt mart now share the same prediction timestamp contract. Database failures are also shown as concise Streamlit errors instead of raw tracebacks.

| Guarantee | Evidence |
| --- | --- |
| The mart exposes the arrival/departure prediction used to calculate delay | `assert_trip_monitoring_samples` |
| Displayed delay equals prediction timestamp minus scheduled timestamp | `assert_trip_monitoring_samples` |
| Pandas-wrapped database failures are handled in both page query stages | `test_route_page_hides_pandas_database_error_in_filters`, `test_route_page_hides_pandas_database_error_in_trip_detail` |
| The dashboard can query and display real PostgreSQL trip samples | Container query and browser verification with the 24-hour filter |

## RED

- `dbt test --select assert_trip_monitoring_samples` failed because `mart.fct_trip_monitoring_samples` did not expose `delay_prediction_timestamp`.
- The two Streamlit page tests failed with an uncaught `pandas.errors.DatabaseError`.
- Checkpoint commit: `b6b871e`.

## GREEN

- `dbt build`: 75 passed, 0 errors.
- Full Python suite: 149 passed, 36 skipped.
- Dashboard suite: 41 passed with 99% coverage.
- Ruff check and format check passed.
- The rebuilt dashboard image returned 371 bus routes and rendered the Trip samples table without a traceback.

The default 60-minute window can legitimately be empty when the table-materialized dbt marts are stale. Selecting 24 hours confirms the historical samples; a later scheduled dbt refresh should address mart freshness separately.
