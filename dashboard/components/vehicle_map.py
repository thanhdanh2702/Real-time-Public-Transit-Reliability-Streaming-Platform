import pandas as pd
import pydeck as pdk


def build_vehicle_map(
    vehicles: pd.DataFrame, selected_vehicle_id: str | None = None
) -> pdk.Deck | None:
    """Show only supplied positions; never reuse markers from an older refresh."""
    if vehicles.empty:
        return None

    markers = vehicles[["vehicle_id", "route_short_name", "latitude", "longitude"]].dropna(
        subset=["latitude", "longitude"]
    )
    if markers.empty:
        return None

    markers = markers.copy()
    markers["route_short_name"] = markers["route_short_name"].fillna("Unknown")
    markers["color"] = markers["vehicle_id"].apply(
        lambda vehicle_id: (
            [249, 115, 22, 220] if vehicle_id == selected_vehicle_id else [37, 99, 235, 190]
        )
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=markers.to_dict("records"),
        get_position="[longitude, latitude]",
        get_fill_color="color",
        get_radius=100,
        radius_min_pixels=5,
        radius_max_pixels=14,
        pickable=True,
    )
    view = pdk.ViewState(
        latitude=float(markers["latitude"].median()),
        longitude=float(markers["longitude"].median()),
        zoom=11,
        pitch=0,
    )
    return pdk.Deck(
        layers=[layer],
        initial_view_state=view,
        tooltip={"text": "Vehicle {vehicle_id} · Route {route_short_name}"},
    )
