import json
from uuid import uuid4

from pyspark.sql import SparkSession

from spark.common.transforms.deduplication import deduplicate_events
from spark.common.transforms.event_time import add_event_time


def test_events_are_deduplicated_across_micro_batches(
    spark_session: SparkSession,
    tmp_path,
) -> None:
    input_path = tmp_path / "input"
    checkpoint_path = tmp_path / "checkpoint"
    input_path.mkdir()

    stream_df = spark_session.readStream.schema("event_id string, source_timestamp timestamp").json(
        str(input_path)
    )
    output_df = deduplicate_events(add_event_time(stream_df))
    query_name = f"event_dedup_{uuid4().hex}"

    query = (
        output_df.writeStream.format("memory")
        .queryName(query_name)
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_path))
        .start()
    )

    event = {
        "event_id": "event-1:2026-08-21T10:00:00Z",
        "source_timestamp": "2026-08-21T10:00:00+00:00",
    }

    try:
        (input_path / "batch-1.json").write_text(json.dumps(event), encoding="utf-8")
        query.processAllAvailable()
        (input_path / "batch-2.json").write_text(json.dumps(event), encoding="utf-8")
        query.processAllAvailable()

        assert spark_session.table(query_name).count() == 1
    finally:
        query.stop()
