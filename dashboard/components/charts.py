import os

import pandas as pd
import plotly.express as px
from plotly.graph_objects import Figure


def predicted_late_trend_chart(trend: pd.DataFrame) -> Figure:
    timezone = os.getenv("APP_TIMEZONE", "America/New_York")
    local_time = (
        pd.to_datetime(trend["observation_bucket"], utc=True)
        .dt.tz_convert(timezone)
        .dt.tz_localize(None)
    )
    chart_data = trend.assign(local_time=local_time)
    chart = px.line(
        chart_data,
        x="local_time",
        y="predicted_late_percentage",
        markers=True,
    )
    chart.update_layout(
        height=340,
        xaxis_title=f"Time ({timezone})",
        yaxis_title="Predicted late (%)",
        showlegend=False,
    )
    chart.update_yaxes(range=[0, 100])
    return chart
