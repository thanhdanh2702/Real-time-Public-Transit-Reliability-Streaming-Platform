# Bunching detector fix evidence

## Scope

No source plan was provided. The derived journey is: as a data engineer, I want two trips on
the same route, direction, stop, and feed snapshot to retain their leading vehicle and arrival
time so that their headway can be calculated correctly.

## Guarantees

| Guarantee | Test | Result |
|---|---|---|
| A pair of trips arriving 90 seconds apart is returned as a bunching candidate | `test_detect_bunching_candidates_returns_close_trip_pair` | PASS |
| The candidate retains the leading trip and vehicle IDs | `test_detect_bunching_candidates_returns_close_trip_pair` | PASS |
| The leading predicted arrival exists and produces the correct headway | `test_detect_bunching_candidates_returns_close_trip_pair` | PASS |

## Evidence

- RED: Spark raised `UNRESOLVED_COLUMN` because `leading_predicted_arrival` was not created and
  `leading_vehicle_id` was overwritten.
- GREEN: the targeted detector test passed: `1 passed in 6.10s`.
- Regression: all Spark unit tests passed: `17 passed in 8.10s`.
- Coverage for `detector.py`: `12/12 statements`, `100%`.
- Ruff check and format check for the detector and its test: passed.

Confirmation observations, cooldown state, alert construction, and live streaming integration
remain follow-up work.
