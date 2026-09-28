# dbt Intermediate Models — TDD Evidence

## Source and user journey

Journeys were derived during this implementation. An analytics consumer needs
realtime vehicle, trip-update, and alert records enriched with the current GTFS
reference feed without losing unmatched realtime records or changing their grain.

## RED evidence

Before the four missing models were implemented, the following command was run:

```text
.venv/bin/dbt --warn-error test --no-partial-parse --project-dir dbt --profiles-dir dbt --select test_type:data
```

It failed during compilation because
`int_vehicle_positions_enriched` did not exist. The same tests also referenced the
other missing intermediate models.

## GREEN evidence

```text
.venv/bin/dbt run --project-dir dbt --profiles-dir dbt --select path:models/intermediate
```

Result: `PASS=5 WARN=0 ERROR=0`.

```text
.venv/bin/dbt test --project-dir dbt --profiles-dir dbt --select int_gtfs_trip_stop_schedule int_vehicle_positions_enriched int_trip_stop_updates_enriched int_service_alert_entities_enriched int_service_alert_periods
```

Result: `PASS=19 WARN=0 ERROR=0`.

## Test specification

| Guarantee | Test | Type | Result |
|---|---|---|---|
| Enrichment preserves the row count of each base relation | `assert_intermediate_preserves_base_rows.sql` | Integration | PASS |
| Every intermediate model preserves its declared grain | `assert_intermediate_grains.sql` | Integration | PASS |
| Alert-period expansion produces one row per period and retains empty arrays | `assert_service_alert_periods_expansion.sql` | Integration | PASS |
| Required identifiers and timestamps are non-null | `intermediate.yml` | Schema/data | PASS |
| Vehicle event identifiers remain unique | `intermediate.yml` | Schema/data | PASS |

## Known gaps

- SQL line coverage is not applicable to dbt data tests; behavioral coverage is
  represented by the 19 executed tests above.
- The current static feed matches only part of the realtime history. This is kept
  visible through `has_gtfs_*_match` flags instead of dropping records.
- No Git checkpoint commits were created because the branch already contained
  uncommitted user work before this implementation.
