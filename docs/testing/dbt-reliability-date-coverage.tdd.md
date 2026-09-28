# dbt reliability and date coverage — TDD evidence

## User journeys

- As a dashboard user, I want route reliability to use every calculable next-stop
  prediction so that departure-only updates are not silently excluded.
- As an analyst, I want the date dimension to cover retained realtime history so
  that refreshing the current GTFS feed does not break historical date joins.

## RED evidence

- `assert_route_health_uses_all_calculable_delays` returned 409 failing route
  buckets because the aggregate only considered arrival-based predictions.
- `dim_dates_covers_static_and_realtime_history` returned only the GTFS date and
  missed the earlier vehicle date and later TripUpdate date from its fixture.

## GREEN evidence

`dbt build --project-dir dbt --profiles-dir dbt` completed with `PASS=75`,
`WARN=0`, and `ERROR=0` across 21 models, 53 data tests, and one unit test.

On the current bus data, metric coverage changed from 561 of 1,487 observed
samples (37.73%) to 1,487 of 1,487 samples (100%). The fact model still prefers
arrival predictions and only uses departure as its existing fallback.

## Test specification

| Guarantee | Test | Type | Result |
|---|---|---|---|
| Route-health counts, percentages, average, and p90 use all calculable delays | `assert_route_health_uses_all_calculable_delays.sql` | Data/integration | PASS |
| Date spine spans static GTFS plus vehicle, TripUpdate, and alert event dates | `dim_dates_covers_static_and_realtime_history` | Unit | PASS |
| Existing grains, relationships, accepted values, and metric bounds remain valid | Full `dbt build` | Regression | PASS |

## Known gaps

- Current GTFS rows are not versioned against historical realtime events; that
  requires the deferred feed-snapshot design.
- Source freshness and incremental materializations remain separate follow-up work.
