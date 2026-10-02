import json
from types import SimpleNamespace
from unittest.mock import MagicMock

from spark.common.metrics.streaming_listener import (
    PostgresStreamingMetricsListener,
    parse_progress,
)


def test_parse_progress_extracts_rates_duration_and_successful_offsets():
    progress = {
        "id": "query-1",
        "runId": "run-1",
        "timestamp": "2026-10-01T12:00:00.000Z",
        "batchId": 12,
        "numInputRows": 250,
        "inputRowsPerSecond": 12.5,
        "processedRowsPerSecond": 20.0,
        "durationMs": {"triggerExecution": 1750},
        "sources": [
            {
                "description": "KafkaV2[Subscribe[transit.trip_updates.v1]]",
                "endOffset": {"transit.trip_updates.v1": {"0": 100, "1": 120}},
            }
        ],
    }

    result = parse_progress("transitpulse-trip-update", json.dumps(progress))

    assert result["application_name"] == "transitpulse-trip-update"
    assert result["batch_id"] == 12
    assert result["num_input_rows"] == 250
    assert result["batch_duration_ms"] == 1750
    assert result["source_offsets"] == {"transit.trip_updates.v1": {"0": 100, "1": 120}}


def test_listener_records_progress_without_exposing_connection_details(monkeypatch):
    listener = PostgresStreamingMetricsListener(
        application_name="transitpulse-vehicle-state",
        connection={
            "host": "postgres",
            "port": 5432,
            "dbname": "transitpulse",
            "user": "transitpulse_admin",
            "password": "secret",
        },
    )
    write_progress = MagicMock()
    monkeypatch.setattr(listener, "_write_progress", write_progress)
    event = SimpleNamespace(
        progress=SimpleNamespace(
            json=json.dumps(
                {
                    "id": "query-1",
                    "runId": "run-1",
                    "timestamp": "2026-10-01T12:00:00.000Z",
                    "batchId": 1,
                    "numInputRows": 5,
                    "inputRowsPerSecond": 1.0,
                    "processedRowsPerSecond": 2.0,
                    "durationMs": {"triggerExecution": 500},
                    "sources": [],
                }
            )
        )
    )

    listener.onQueryProgress(event)

    write_progress.assert_called_once()
    assert write_progress.call_args.args[0]["application_name"] == "transitpulse-vehicle-state"
