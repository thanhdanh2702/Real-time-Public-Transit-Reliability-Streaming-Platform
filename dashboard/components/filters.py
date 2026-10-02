import pandas as pd
import streamlit as st


def route_reliability_filters(
    routes: pd.DataFrame,
) -> tuple[int, str | None, int | None, int, str]:
    st.sidebar.header("Filters")
    windows = {"60 minutes": 60, "6 hours": 360, "24 hours": 1440}
    window = st.sidebar.selectbox("Time window", list(windows))

    labels = {}
    for _, row in routes.iterrows():
        route_id = str(row["route_id"])
        short_name = row["route_short_name"]
        long_name = row["route_long_name"]
        short_name = short_name if isinstance(short_name, str) and short_name else route_id
        if isinstance(long_name, str) and long_name:
            labels[route_id] = f"{short_name} · {long_name}"
        else:
            labels[route_id] = short_name

    route_id = st.sidebar.selectbox(
        "Route",
        [None, *labels],
        format_func=lambda value: "All routes" if value is None else labels[value],
    )
    directions = {None: "Both directions", 0: "Direction 0", 1: "Direction 1", -1: "Unknown"}
    direction_id = st.sidebar.selectbox(
        "Direction", list(directions), format_func=lambda value: directions[value]
    )
    min_samples = st.sidebar.slider(
        "Minimum eligible samples",
        min_value=1,
        max_value=200,
        value=20,
        help="Routes below this threshold are excluded from route comparison.",
    )
    sort_label = st.sidebar.selectbox(
        "Sort samples",
        ["Latest observation", "Largest predicted delay"],
    )
    sort_by = "latest" if sort_label == "Latest observation" else "largest_delay"
    return windows[window], route_id, direction_id, min_samples, sort_by
