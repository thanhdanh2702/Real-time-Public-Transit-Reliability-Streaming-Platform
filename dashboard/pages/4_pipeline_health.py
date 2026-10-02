import os

import pandas as pd
import streamlit as st
from sqlalchemy.exc import SQLAlchemyError

from dashboard.queries import pipeline_health

st.set_page_config(page_title="TransitPulse | Pipeline Health", page_icon="⚙️", layout="wide")
st.title("Pipeline health")
st.caption(
    "Operational evidence from Spark progress, Kafka offsets and Airflow dbt runs · "
    "refreshes every 30 seconds"
)


def _timestamp(value: object) -> str:
    if pd.isna(value):
        return "—"
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")
    return pd.to_datetime(value, utc=True).tz_convert(timezone).strftime("%Y-%m-%d %H:%M:%S %Z")


def _spark_display(applications: pd.DataFrame) -> pd.DataFrame:
    if applications.empty:
        return applications
    result = applications.copy()
    result["Status"] = result.apply(
        lambda row: pipeline_health.display_status(row["status"], row["last_progress_at"]),
        axis=1,
    )
    result["Last progress"] = result["last_progress_at"].apply(_timestamp)
    result["Batch duration"] = result["batch_duration_ms"].apply(
        lambda value: f"{value / 1000:.2f} sec" if pd.notna(value) else "—"
    )
    return result[
        [
            "display_name",
            "Status",
            "Last progress",
            "batch_id",
            "num_input_rows",
            "input_rows_per_second",
            "processed_rows_per_second",
            "Batch duration",
        ]
    ].rename(
        columns={
            "display_name": "Application",
            "batch_id": "Batch ID",
            "num_input_rows": "Input rows",
            "input_rows_per_second": "Input rows/sec",
            "processed_rows_per_second": "Processed rows/sec",
        }
    )


@st.fragment(run_every="30s")
def show_pipeline_health() -> None:
    try:
        applications = pipeline_health.get_spark_health()
        dbt_runs = pipeline_health.get_latest_dbt_run()
        data_times = pipeline_health.get_data_times()
    except (SQLAlchemyError, pd.errors.DatabaseError, RuntimeError):
        st.error(
            "Pipeline metrics are unavailable. Apply migration 007 and start the instrumented jobs."
        )
        return

    st.subheader("Spark Structured Streaming")
    st.caption(
        "A running label requires a recent successful progress event; "
        "container state alone is not used."
    )
    st.dataframe(_spark_display(applications), hide_index=True, width="stretch")

    st.subheader("Kafka backlog")
    try:
        watermarks = pipeline_health.get_kafka_watermarks(
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        )
        offsets = pipeline_health.processed_offsets(applications)
        backlog = pipeline_health.calculate_kafka_backlog(watermarks, offsets)
        streaming_backlog = backlog[backlog["topic"].isin(pipeline_health.STREAMING_TOPICS)]
        if streaming_backlog.empty:
            st.info("Kafka backlog is unavailable because broker offsets could not be read.")
        else:
            st.dataframe(
                streaming_backlog[
                    ["topic", "partition", "high", "processed_offset", "backlog"]
                ].rename(
                    columns={
                        "topic": "Topic",
                        "partition": "Partition",
                        "high": "Broker end offset",
                        "processed_offset": "Spark processed offset",
                        "backlog": "Backlog",
                    }
                ),
                hide_index=True,
                width="stretch",
            )
        dlq_count = pipeline_health.retained_record_count(watermarks, pipeline_health.DLQ_TOPIC)
        st.metric("DLQ records retained", dlq_count if dlq_count is not None else "—")
        st.caption(
            "DLQ count means records currently retained in the dead-letter topic "
            "(broker high watermark minus low watermark), not records created in this UI window."
        )
    except Exception:
        st.info("Kafka metrics are unavailable; broker connectivity could not be verified.")

    st.info(
        "Producer status: Unknown. The producer does not yet emit an independent heartbeat, "
        "so Kafka/Spark activity is not presented as proof that the producer process is healthy."
    )

    st.subheader("dbt refresh")
    if dbt_runs.empty:
        st.info("No Airflow dbt run metadata has been recorded yet.")
    else:
        latest = dbt_runs.iloc[0]
        dbt_status, started, finished, duration = st.columns(4)
        dbt_status.metric("Latest dbt run", str(latest["status"]).title())
        started.metric("Started", _timestamp(latest["started_at"]))
        finished.metric("Finished", _timestamp(latest["finished_at"]))
        duration.metric(
            "Duration",
            f"{float(latest['duration_seconds']):.1f} sec"
            if pd.notna(latest["duration_seconds"])
            else "—",
        )
        run_status = str(latest["status"]).lower()
        error_message = latest.get("error_message")
        if run_status == "failed":
            st.error(
                "Latest dbt refresh failed. "
                + (str(error_message) if pd.notna(error_message) else "Inspect Airflow logs.")
            )
        elif run_status == "stale":
            st.warning(
                "Latest dbt run did not reach a terminal state. Inspect the Airflow task run."
            )

    st.subheader("Data timestamps")
    if not data_times.empty:
        times = data_times.iloc[0]
        event_col, mart_col, ui_col = st.columns(3)
        event_col.metric("Latest event observed", _timestamp(times["latest_event_at"]))
        mart_col.metric("Latest mart build", _timestamp(times["latest_dbt_finished_at"]))
        ui_col.metric("Dashboard read", _timestamp(times["dashboard_read_at"]))
    st.caption(
        "The dashboard may refresh every 30 seconds while marts only change "
        "after a successful dbt run."
    )


show_pipeline_health()
