import streamlit as st

st.set_page_config(
    page_title="TransitPulse",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded",
)

navigation = st.navigation(
    [
        st.Page(
            "pages/1_overview.py",
            title="Overview",
            icon=":material/dashboard:",
            default=True,
        ),
        st.Page(
            "pages/2_route_reliability.py",
            title="Route reliability",
            icon=":material/query_stats:",
        ),
        st.Page(
            "pages/3_live_operations.py",
            title="Live operations",
            icon=":material/map:",
        ),
        st.Page(
            "pages/4_pipeline_health.py",
            title="Pipeline health",
            icon=":material/monitor_heart:",
        ),
    ]
)
navigation.run()
