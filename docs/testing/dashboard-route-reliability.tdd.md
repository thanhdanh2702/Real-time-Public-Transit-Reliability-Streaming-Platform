# Route Reliability dashboard: test evidence

The page filters MBTA bus trip samples by time, route, and direction, then shows weighted predicted-late metrics, a five-minute trend, route comparison, and a drilldown into sampled trips.

| Guarantee | Evidence |
| --- | --- |
| Query filters and bound parameters apply consistently to trend, comparison, freshness, and detail rows | `test_route_filters_apply_to_trend_comparison_and_latest_bucket`, `test_trip_details_respect_filters_and_bus_scope` |
| Late rate is calculated from summed late and eligible counts; coverage from summed eligible and observed counts | `test_route_queries_use_bus_mart_and_weighted_rates`, `test_reliability_rate_uses_total_late_and_eligible_counts` |
| UI handles populated, empty, stale, filtered, and database-error states | `dashboard/tests/test_route_reliability_page.py` |
| Comparison chart keeps distinct route IDs even when route names match | `test_route_comparison_keeps_routes_distinct_when_names_repeat` |
| Trip detail retains the GTFS arrival/departure basis and falls back to route ID if the short name is absent | `test_trip_details_respect_filters_and_bus_scope`, `test_route_page_shows_metrics_comparison_and_trip_detail` |

RED: Query and page tests failed before implementation; separate tests caught the missing prediction basis, duplicate chart labels, database-error state, and missing route-name fallback.

GREEN: `.venv/bin/python -m pytest dashboard/tests -q -o addopts= --cov=dashboard --cov-report=term-missing` passed 23 tests with 99% dashboard coverage. Ruff check and format check passed. Eight SQLite datetime-adapter deprecation warnings remain under Python 3.12.

Query tests use in-memory SQLite and page tests use Streamlit AppTest. Live PostgreSQL and browser behavior remain unverified while the Docker daemon is unavailable. The page's dbt mart values refresh after `dbt build`, even though the Streamlit page reruns every 30 seconds.
