# PostgreSQL staging schemas — TDD evidence

## Journey

Store flattened TripUpdate stops and ServiceAlert informed entities as idempotent relational
rows, while preserving Kafka lineage and alert active periods.

## RED → GREEN

The transform tests initially failed because `stop_update_index` and
`informed_entity_index` did not exist. After using `posexplode`, both keys are stable within
their source event; TripUpdate retains full lineage and ServiceAlert serializes
`active_periods` as JSON for the PostgreSQL `JSONB` column.

| Guarantee | Evidence | Result |
|---|---|---|
| Each trip stop has `(event_id, stop_update_index)` identity | `test_trip_update_transforms.py` | PASS |
| Each informed entity has `(event_id, informed_entity_index)` identity | `test_service_alert_transforms.py` | PASS |
| Alert periods are valid JSON before the sink | `test_service_alert_transforms.py` | PASS |
| Migrations 001–004 execute and create all three staging tables | PostgreSQL 18 temporary instance | PASS |

Targeted result: `6 passed`. Full result: `36 passed`, total repository coverage `88%`.
Ruff check and format check passed. The PostgreSQL sink and sink integration test remain a
separate next step; no data was written to the project's persistent PostgreSQL volume.
