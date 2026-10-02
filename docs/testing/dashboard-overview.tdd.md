# Overview dashboard: test evidence

The intended user can see current MBTA bus activity, predicted lateness in the last 60 minutes, and whether each source is fresh.

| Guarantee | Test | Result |
| --- | --- | --- |
| Fresh vehicles and active alerts are counted from the appropriate mart flags | `test_overview_counts_only_fresh_vehicles_and_alerts` | Pass |
| Route counts are summed per five-minute bucket and respect the time window | `test_overview_health_trend_sums_counts_at_bucket_grain` | Pass |
| Overall late rate is recomputed from summed late and eligible counts | `test_reliability_rate_uses_total_late_and_eligible_counts` | Pass |
| Missing and zero-denominator data are not shown as a measured late rate | `test_reliability_rate_is_unknown_without_eligible_samples`, `test_overview_page_marks_missing_sources_as_unavailable` | Pass |
| Stale alert feed does not display an active-alert count | `test_overview_page_does_not_count_alerts_from_stale_feed` | Pass |
| Chart time is displayed in the configured MBTA timezone | `test_predicted_late_chart_uses_project_local_time` | Pass |
| Streamlit entrypoint and populated Overview page render | `test_dashboard_entrypoint_opens`, `test_overview_page_shows_current_metrics` | Pass |

RED: Before implementation, three new query tests failed on missing functions and the metrics test failed at import (`3736a0d`). The page tests rendered no metrics (`17963be`). The chart test observed UTC instead of local time (`123fddc`).

GREEN: `.venv/bin/python -m pytest dashboard/tests -q -o addopts= --cov=dashboard --cov-report=term-missing` passed 14 tests with 100% coverage of the dashboard files. Ruff check/format and CI YAML parse passed. Query tests use in-memory SQLite and page tests use Streamlit AppTest. Two Python 3.12 SQLite datetime-adapter deprecation warnings remain in the test environment.

Live PostgreSQL and browser preview were not verified because the Docker daemon was unavailable. The `route_health_5m` mart is a dbt table; its displayed values update only after a dbt build. The alert mart cannot establish that a newer empty source snapshot occurred until feed-run metadata is stored.
