import pandas as pd

from dashboard.queries import pipeline_health


def test_processed_offsets_are_read_from_latest_successful_spark_progress():
    progress = pd.DataFrame(
        [
            {
                "application_name": "transitpulse-trip-update",
                "source_offsets": {"transit.trip_updates.v1": {"0": 100, "1": 120}},
            },
            {
                "application_name": "transitpulse-vehicle-state",
                "source_offsets": '{"transit.vehicle_positions.v1": {"0": 80}}',
            },
        ]
    )

    assert pipeline_health.processed_offsets(progress) == {
        "transit.trip_updates.v1": {0: 100, 1: 120},
        "transit.vehicle_positions.v1": {0: 80},
    }


def test_kafka_backlog_uses_end_offset_minus_successfully_processed_offset():
    watermarks = pd.DataFrame(
        [
            {"topic": "transit.trip_updates.v1", "partition": 0, "low": 10, "high": 130},
            {"topic": "transit.trip_updates.v1", "partition": 1, "low": 20, "high": 150},
            {"topic": "transit.dead_letter.v1", "partition": 0, "low": 4, "high": 7},
        ]
    )
    offsets = {"transit.trip_updates.v1": {0: 100, 1: 120}}

    result = pipeline_health.calculate_kafka_backlog(watermarks, offsets)

    assert result.loc[result["partition"] == 0, "backlog"].iloc[0] == 30
    assert result.loc[result["partition"] == 1, "backlog"].iloc[0] == 30
    assert pd.isna(result.loc[result["topic"] == "transit.dead_letter.v1", "backlog"].iloc[0])
    assert pipeline_health.retained_record_count(watermarks, "transit.dead_letter.v1") == 3


def test_spark_status_is_unknown_without_evidence_and_stale_when_progress_is_old():
    now = pd.Timestamp("2026-10-01 12:00:00+00:00")

    assert pipeline_health.display_status(None, None, now=now) == "Unknown"
    assert (
        pipeline_health.display_status(
            "running",
            pd.Timestamp("2026-10-01 11:55:00+00:00"),
            now=now,
        )
        == "Stale"
    )
    assert (
        pipeline_health.display_status(
            "running",
            pd.Timestamp("2026-10-01 11:59:30+00:00"),
            now=now,
        )
        == "Running"
    )


def test_latest_dbt_run_marks_abandoned_running_metadata_stale(monkeypatch):
    captured: dict[str, str] = {}

    def fake_read_dataframe(query: str):
        captured["query"] = query
        return pd.DataFrame()

    monkeypatch.setattr(pipeline_health, "read_dataframe", fake_read_dataframe)

    pipeline_health.get_latest_dbt_run()

    assert "status = 'running'" in captured["query"]
    assert "INTERVAL '20 minutes'" in captured["query"]
    assert "THEN 'stale'" in captured["query"]
