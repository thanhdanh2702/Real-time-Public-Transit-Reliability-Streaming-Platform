import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pyspark.sql import functions as F

from spark.common.schemas.trip_update import TRIP_UPDATE_SCHEMA
from spark.jobs.bunching_detector.detector import detect_bunching_candidates
from spark.jobs.bunching_detector.pipeline import (
    BunchingProcessor,
    BunchingSettings,
    prepare_trip_updates,
)
from spark.jobs.route_reliability.transform import transform_trip_update

FIXTURE = Path(__file__).parents[1] / "fixtures/trip_update_valid.json"
CONTRACT = Path(__file__).parents[3] / "contracts/operational_alert_v1.json"


def event(trip, second, arrival=90):
    item = json.loads(FIXTURE.read_text())
    timestamp = f"2026-08-15T09:{45 + second // 60:02d}:{second % 60:02d}+00:00"
    item.update(
        trip_id=trip,
        vehicle_id=f"vehicle-{trip}",
        feed_timestamp=timestamp,
        source_timestamp=timestamp,
        event_id=f"{trip}:{second}",
    )
    item["payload"]["stop_time_updates"] = [
        {
            "stop_id": "stop-1",
            "stop_sequence": 1,
            "predicted_arrival": f"2026-08-15T10:{arrival // 60:02d}:{arrival % 60:02d}+00:00",
            "schedule_relationship": "SCHEDULED",
        }
    ]
    return item


def frame(spark, items):
    raw = spark.createDataFrame([(json.dumps(item),) for item in items], "value string")
    return (
        raw.select(F.from_json("value", TRIP_UPDATE_SCHEMA).alias("event"))
        .select("event.*")
        .withColumn("kafka_partition", F.lit(0))
        .withColumn("kafka_offset", F.lit(0).cast("long"))
        .withColumn("kafka_timestamp", F.col("feed_timestamp"))
    )


def test_split_snapshot_restart_confirmation_cooldown_and_retry(spark_session, tmp_path):
    output = []
    settings = BunchingSettings(snapshot_lateness_seconds=10, cooldown_seconds=60)
    processor = BunchingProcessor(tmp_path, settings, output.append)
    processor(frame(spark_session, [event("a", 0, 0)]), 0)
    # Same snapshot arrives in a different micro-batch; restart must preserve it.
    processor = BunchingProcessor(tmp_path, settings, output.append)
    processor(frame(spark_session, [event("b", 0), event("a", 30, 0), event("b", 30)]), 1)
    assert output[-1] == []  # Only the first observation has closed.
    processor(frame(spark_session, [event("a", 60, 0), event("b", 60)]), 2)
    alerts = output[-1]
    assert len(alerts) == 1
    alert = json.loads(alerts[0])
    Draft202012Validator(json.loads(CONTRACT.read_text()), format_checker=FormatChecker()).validate(
        alert
    )
    assert alert["payload"]["observed_value"] == 90
    assert alert["payload"]["first_detected_at"] != alert["payload"]["last_detected_at"]
    assert alert["payload"]["related_vehicle_ids"] == ["vehicle-a", "vehicle-b"]
    processor = BunchingProcessor(tmp_path, settings, output.append)
    processor(frame(spark_session, [event("a", 60, 0)]), 2)
    assert output[-1] == alerts  # Exact outbox replay, no extra observation.
    processor(frame(spark_session, [event("a", 90, 0), event("b", 90)]), 3)
    assert output[-1] == []  # 30 seconds since last alert; still in cooldown.
    processor(frame(spark_session, [event("a", 120, 0), event("b", 120)]), 4)
    assert len(output[-1]) == 1


def test_duplicate_snapshot_does_not_confirm_and_limits_rollback(spark_session, tmp_path):
    output = []
    settings = BunchingSettings(snapshot_lateness_seconds=10, max_buffered_events=2)
    processor = BunchingProcessor(tmp_path, settings, output.append)
    pair = [event("a", 0, 0), event("b", 0)]
    processor(frame(spark_session, pair), 0)
    processor(frame(spark_session, pair), 1)
    assert output[-1] == []
    with pytest.raises(ValueError, match="buffer"):
        processor(frame(spark_session, [event("c", 0)]), 2)
    processor(frame(spark_session, [event("a", 30, 0)]), 2)
    assert output[-1] == []


def test_rejects_invalid_settings():
    with pytest.raises(ValueError):
        BunchingSettings(confirmation_observations=0)


def test_input_filters_unselected_routes_and_future_feeds(spark_session):
    normal = event("a", 0)
    train = event("train", 0)
    train["route_id"] = "Red"
    future = event("future", 0)
    future["feed_timestamp"] = "2030-01-01T00:00:00+00:00"
    raw = spark_session.createDataFrame(
        [
            (item["trip_id"], json.dumps(item), "trip", 0, index, item["feed_timestamp"])
            for index, item in enumerate([normal, train, future])
        ],
        "key string, value string, topic string, partition int, offset long, timestamp string",
    )
    result = prepare_trip_updates(raw, BunchingSettings()).select("trip_id").collect()
    assert [row.trip_id for row in result] == ["a"]


def test_detector_uses_one_upcoming_visit_per_trip(spark_session):
    a = event("a", 0, 0)
    # A route loop or duplicate prediction must not insert trip-a again between a and b.
    a["payload"]["stop_time_updates"].append(
        {
            **a["payload"]["stop_time_updates"][0],
            "stop_sequence": 9,
            "predicted_arrival": "2026-08-15T10:01:00+00:00",
        }
    )
    candidates = detect_bunching_candidates(
        transform_trip_update(frame(spark_session, [a, event("b", 0)]))
    ).collect()
    assert len(candidates) == 1
    assert candidates[0].headway_seconds == 90


def test_late_snapshot_missing_observation_and_stale_prediction(spark_session, tmp_path):
    output = []
    processor = BunchingProcessor(
        tmp_path, BunchingSettings(snapshot_lateness_seconds=10), output.append
    )
    processor(frame(spark_session, [event("a", 0, 0), event("b", 0)]), 0)
    processor(frame(spark_session, [event("a", 30, 0)]), 1)
    processor(frame(spark_session, [event("a", 60, 0), event("b", 60)]), 2)
    processor(frame(spark_session, [event("b", 30), event("a", 90, 0)]), 3)
    assert output[-1] == []  # Late b/30 was dropped, so 60 is the first new observation.
    stale = event("b", 90)
    stale["source_timestamp"] = "2026-08-15T09:40:00+00:00"
    processor(frame(spark_session, [stale, event("a", 120, 0)]), 4)
    assert output[-1] == []


def test_empty_batch_settings_change_missing_state_and_old_retry(spark_session, tmp_path):
    output = []
    settings = BunchingSettings(snapshot_lateness_seconds=10)
    processor = BunchingProcessor(tmp_path, settings, output.append)
    df = frame(spark_session, [event("a", 0)])
    processor(df.limit(0), 0)
    processor(df, 1)
    assert output == [[], []]
    with pytest.raises(ValueError, match="Batch ID"):
        processor(df, 0)
    with pytest.raises(ValueError, match="Configuration"):
        BunchingProcessor(tmp_path, BunchingSettings(), output.append)(df, 2)
    missing = tmp_path / "missing"
    (missing / "commits").mkdir(parents=True)
    (missing / "commits/0").touch()
    with pytest.raises(ValueError, match="Missing"):
        BunchingProcessor(missing, settings)


def test_batch_limit_and_console_failure_retry(spark_session, tmp_path):
    settings = BunchingSettings(max_events_per_batch=1)
    df = frame(spark_session, [event("a", 0), event("b", 0)])
    processor = BunchingProcessor(tmp_path, settings)
    with pytest.raises(ValueError, match="Per-batch"):
        processor(df, 0)
    with pytest.raises(ValueError, match="Batch ID"):
        processor(df.limit(1), 2)

    def fail(_):
        raise RuntimeError("console disconnected")

    with pytest.raises(RuntimeError, match="console"):
        BunchingProcessor(tmp_path, settings, fail)(df.limit(1), 0)
    output = []
    BunchingProcessor(tmp_path, settings, output.append)(df.limit(1), 0)
    assert output == [[]]


@pytest.mark.parametrize(
    ("headway", "field", "value", "expected"),
    [
        (0, None, None, 1),
        (120, None, None, 1),
        (121, None, None, 0),
        (90, "route_id", "66", 0),
        (90, "direction_id", 1, 0),
        (90, "stop_id", "different", 0),
        (90, "vehicle_id", "vehicle-a", 0),
        (90, "schedule_relationship", "SKIPPED", 0),
        (90, "trip_schedule_relationship", "CANCELED", 0),
        (90, "vehicle_id", None, 1),
    ],
)
def test_detector_boundaries_and_pair_isolation(spark_session, headway, field, value, expected):
    a, b = event("a", 0, 0), event("b", 0, headway)
    if field == "direction_id":
        b["payload"][field] = value
    elif field == "trip_schedule_relationship":
        b["payload"]["schedule_relationship"] = value
    elif field in ("stop_id", "schedule_relationship"):
        b["payload"]["stop_time_updates"][0][field] = value
    elif field:
        b[field] = value
    assert (
        detect_bunching_candidates(transform_trip_update(frame(spark_session, [a, b]))).count()
        == expected
    )


def test_detector_rejects_invalid_threshold(spark_session):
    with pytest.raises(ValueError):
        detect_bunching_candidates(frame(spark_session, [event("a", 0)]), 0)
