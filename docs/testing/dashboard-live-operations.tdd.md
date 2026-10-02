# Live Operations dashboard: test evidence

The page shows fresh MBTA bus positions on a route-filtered map and current service alerts from the latest stored non-empty feed snapshot. It refreshes every 30 seconds without creating another Kafka or Spark flow.

| Guarantee | Evidence |
| --- | --- |
| Only fresh bus positions with usable coordinates appear; route values are bound parameters | `test_live_vehicles_use_fresh_bus_positions_and_bound_route_filter` |
| The page detects result limits instead of silently implying a complete map or alert list | `test_live_vehicles_fetch_one_extra_row_to_detect_truncation`, page tests |
| Alerts respect freshness and active periods; unscoped alerts are labeled, not assumed to affect a selected route | `test_alert_query_keeps_unknown_route_scope_and_uses_bound_values`, `test_live_page_handles_missing_names_without_inventing_alert_route` |
| Stale feeds and fresh-but-empty selections have distinct UI states | `test_live_page_does_not_show_old_positions_or_stale_alert_count`, `test_live_page_distinguishes_fresh_empty_selection_from_stale_feed` |
| Database errors do not reveal connection details in the page | `test_live_page_reports_database_failure_without_exposing_credentials`, `test_live_page_catches_database_errors_wrapped_by_pandas` |
| PostgreSQL accepts the unfiltered queries without an untyped NULL route parameter | `test_all_routes_omits_untyped_null_parameter` and a Docker PostgreSQL query smoke test |

RED: The initial tests failed because the map, query, and page behavior did not exist. A real PostgreSQL smoke test then exposed an ambiguous NULL parameter in the all-routes query; a regression test reproduced it before the fix. A second regression test covered database errors wrapped by pandas.

GREEN: `dashboard/tests` passed 39 tests with 99% dashboard coverage. The full repository suite passed 147 tests with 36 skips when PySpark workers were explicitly pointed at the project's Python 3.12 environment. Ruff check and format check passed. The dashboard container passed its health check, all four Live Operations queries ran against Docker PostgreSQL, and the page rendered in a browser.

The stored MBTA data was stale during the browser check, so the page correctly showed no vehicle markers and an unknown alert count. Rendering with fresh live data was covered by Streamlit AppTest but not observed in the browser. The current alert mart cannot observe a newer empty source snapshot; the page and README disclose this limitation. The pre-existing local virtual environment has an `urllib3` advisory reported by `pip-audit`; the rebuilt dashboard image installed `urllib3 2.8.0`.
