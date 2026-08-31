from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from spark.jobs.bunching_detector.detector import detect_bunching_candidates


def test_detect_bunching_candidates_returns_close_trip_pair(
    spark_session: SparkSession,
) -> None:
    feed_timestamp = datetime(2026, 8, 31, 10, 0, tzinfo=UTC)
    base_row = {
        "feed_timestamp": feed_timestamp,
        "event_timestamp": feed_timestamp,
        "route_id": "1",
        "direction_id": 0,
        "stop_id": "stop-1",
        "trip_schedule_relationship": "SCHEDULED",
        "stop_schedule_relationship": "SCHEDULED",
    }
    input_df = spark_session.createDataFrame(
        [
            {
                **base_row,
                "trip_id": "trip-a",
                "vehicle_id": "vehicle-a",
                "predicted_arrival": feed_timestamp + timedelta(minutes=5),
            },
            {
                **base_row,
                "trip_id": "trip-b",
                "vehicle_id": "vehicle-b",
                "predicted_arrival": feed_timestamp + timedelta(minutes=6, seconds=30),
            },
        ]
    )

    candidate = detect_bunching_candidates(input_df).first()

    assert candidate is not None
    assert candidate.leading_trip_id == "trip-a"
    assert candidate.following_trip_id == "trip-b"
    assert candidate.leading_vehicle_id == "vehicle-a"
    assert candidate.headway_seconds == 90
