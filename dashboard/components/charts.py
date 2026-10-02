import os

import pandas as pd
import plotly.express as px
from plotly.graph_objects import Bar, Figure, Scatter
from plotly.subplots import make_subplots


def reliability_trend_chart(trend: pd.DataFrame) -> Figure:
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")
    chart_data = trend.copy()
    chart_data["observation_bucket"] = pd.to_datetime(
        chart_data["observation_bucket"], utc=True
    )
    chart_data = chart_data.set_index("observation_bucket").sort_index()
    if len(chart_data) > 1:
        complete_index = pd.date_range(
            chart_data.index.min(), chart_data.index.max(), freq="5min", tz="UTC"
        )
        chart_data = chart_data.reindex(complete_index)
    chart_data["local_time"] = chart_data.index.tz_convert(timezone).tz_localize(None)
    late_percentage = chart_data["predicted_late_percentage"].astype(object)
    late_percentage = late_percentage.where(late_percentage.notna(), None)

    chart = make_subplots(specs=[[{"secondary_y": True}]])
    chart.add_trace(
        Bar(
            x=chart_data["local_time"],
            y=chart_data["eligible_trip_count"],
            name="Eligible samples",
            marker_color="#bfdbfe",
        ),
        secondary_y=True,
    )
    chart.add_trace(
        Scatter(
            x=chart_data["local_time"],
            y=late_percentage,
            name="Predicted late",
            mode="lines+markers",
            connectgaps=False,
            line={"color": "#2563eb", "width": 3},
        ),
        secondary_y=False,
    )
    chart.update_layout(
        height=340,
        xaxis_title=f"Time ({timezone})",
        legend={"orientation": "h", "y": 1.12, "x": 0},
        margin={"l": 10, "r": 10, "t": 45, "b": 10},
    )
    chart.update_yaxes(title_text="Predicted late (%)", range=[0, 100], secondary_y=False)
    chart.update_yaxes(title_text="Eligible samples", rangemode="tozero", secondary_y=True)
    return chart


def predicted_late_trend_chart(trend: pd.DataFrame) -> Figure:
    """Backward-compatible name used by older page imports."""
    return reliability_trend_chart(trend)


def route_comparison_chart(comparison: pd.DataFrame) -> Figure:
    comparison = comparison.copy()
    comparison["predicted_late_percentage"] = pd.to_numeric(comparison["predicted_late_percentage"])
    chart_data = (
        comparison.sort_values(
            ["predicted_late_percentage", "eligible_trip_count"],
            ascending=False,
        )
        .head(10)
        .copy()
    )
    chart = px.bar(
        chart_data,
        x="route_id",
        y="predicted_late_percentage",
        hover_data=["route_short_name", "eligible_trip_count", "late_trip_count"],
    )
    chart.update_layout(
        height=340,
        xaxis_title="Route",
        yaxis_title="Predicted late (%)",
        showlegend=False,
    )
    chart.update_yaxes(range=[0, 100])
    return chart
