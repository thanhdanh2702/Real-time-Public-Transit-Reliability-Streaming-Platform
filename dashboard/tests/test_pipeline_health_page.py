from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest

from dashboard.queries import pipeline_health

PIPELINE_PAGE = Path(__file__).resolve().parents[1] / "pages/4_pipeline_health.py"


def test_pipeline_page_shows_real_metrics_and_unknown_producer(monkeypatch):
    now = pd.Timestamp.now(tz="UTC")
    monkeypatch.setattr(
        pipeline_health,
        "get_spark_health",
        lambda: pd.DataFrame(
            [
                {
                    "application_name": "transitpulse-vehicle-state",
                    "display_name": "Vehicle positions",
                    "status": "running",
                    "last_progress_at": now,
                    "batch_id": 12,
                    "num_input_rows": 20,
                    "input_rows_per_second": 2.0,
                    "processed_rows_per_second": 4.0,
                    "batch_duration_ms": 500,
                    "source_offsets": {"transit.vehicle_positions.v1": {"0": 100}},
                }
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline_health,
        "get_latest_dbt_run",
        lambda: pd.DataFrame(
            [
                {
                    "status": "success",
                    "started_at": now - pd.Timedelta(seconds=4.5),
                    "finished_at": now,
                    "duration_seconds": 4.5,
                    "error_message": None,
                }
            ]
        ),
    )
    monkeypatch.setattr(
        pipeline_health,
        "get_data_times",
        lambda: pd.DataFrame(
            [{"latest_event_at": now, "latest_dbt_finished_at": now, "dashboard_read_at": now}]
        ),
    )
    monkeypatch.setattr(
        pipeline_health,
        "get_kafka_watermarks",
        lambda *_args, **_kwargs: pd.DataFrame(
            [
                {
                    "topic": "transit.vehicle_positions.v1",
                    "partition": 0,
                    "low": 0,
                    "high": 110,
                },
                {"topic": "transit.dead_letter.v1", "partition": 0, "low": 0, "high": 2},
            ]
        ),
    )

    page = AppTest.from_file(PIPELINE_PAGE).run()

    assert not page.exception
    assert any("Producer status: Unknown" in info.value for info in page.info)
    assert any("currently retained" in caption.value for caption in page.caption)
    assert page.get("dataframe")
