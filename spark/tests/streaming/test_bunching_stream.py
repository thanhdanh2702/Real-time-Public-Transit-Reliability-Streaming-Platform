import json

from spark.jobs.bunching_detector.pipeline import (
    BunchingSettings,
    print_alerts,
    start_bunching_query,
)
from spark.tests.unit.test_bunching_flow import event


def test_single_query_raw_to_alert_with_checkpoint_restart(spark_session, tmp_path):
    inputs = tmp_path / "input"
    inputs.mkdir()
    checkpoint = tmp_path / "checkpoint"
    settings = BunchingSettings(snapshot_lateness_seconds=10)
    output = []

    def emit(alerts):
        output.append(alerts)
        print_alerts(alerts)

    raw = (
        spark_session.readStream.schema(
            "key string, value string, topic string, partition int, "
            "offset long, timestamp timestamp"
        )
        .option("maxFilesPerTrigger", 1)
        .json(str(inputs))
    )

    def publish(number, events):
        rows = [
            json.dumps(
                {
                    "key": item["trip_id"],
                    "value": json.dumps(item),
                    "topic": "transit.trip_updates.v1",
                    "partition": number % 3,
                    "offset": number * 100 + index,
                    "timestamp": item["feed_timestamp"],
                }
            )
            for index, item in enumerate(events)
        ]
        (inputs / f"{number}.json").write_text("\n".join(rows))

    query = start_bunching_query(spark_session, raw, checkpoint, settings, "1 second", emit)
    try:
        publish(0, [event("a", 0, 0)])
        query.processAllAvailable()
    finally:
        query.stop()
    query = start_bunching_query(spark_session, raw, checkpoint, settings, "1 second", emit)
    try:
        invalid = event("invalid", 0)
        invalid["schema_version"] = 99
        # Same event_id/source_timestamp in the next feed must remain an observation.
        a30, b30 = event("a", 30, 0), event("b", 30)
        a30.update(event_id="a:0", source_timestamp=event("a", 0)["source_timestamp"])
        b30.update(event_id="b:0", source_timestamp=event("b", 0)["source_timestamp"])
        publish(1, [event("b", 0), invalid, a30, b30])
        query.processAllAvailable()
        publish(2, [event("a", 60, 0)])
        query.processAllAvailable()
        alerts = [json.loads(alert) for batch in output for alert in batch]
        assert len(alerts) == 1
        assert alerts[0]["payload"]["observed_value"] == 90
        assert alerts[0]["payload"]["evidence"]["leading_trip_id"] == "a"
    finally:
        query.stop()
