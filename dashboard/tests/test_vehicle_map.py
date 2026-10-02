import json

import pandas as pd
import pytest

from dashboard.components.vehicle_map import build_vehicle_map, occupancy_label


def test_occupancy_status_is_presented_as_readable_text():
    assert occupancy_label("MANY_SEATS_AVAILABLE") == "Many seats available"
    assert occupancy_label(None) == "Unknown"
    assert occupancy_label("NEW_STATUS") == "New status"


def test_vehicle_map_uses_actual_coordinates_and_selected_vehicle():
    vehicles = pd.DataFrame(
        [
            {
                "vehicle_id": "bus-a",
                "route_short_name": "A",
                "latitude": 42.35,
                "longitude": -71.06,
            },
            {
                "vehicle_id": "bus-b",
                "route_short_name": "B",
                "latitude": 42.36,
                "longitude": -71.07,
            },
        ]
    )

    deck = build_vehicle_map(vehicles, selected_vehicle_id="bus-b")
    spec = json.loads(deck.to_json())

    assert spec["initialViewState"]["latitude"] == pytest.approx(42.355)
    assert spec["initialViewState"]["longitude"] == pytest.approx(-71.065)
    assert {row["vehicle_id"] for row in spec["layers"][0]["data"]} == {"bus-a", "bus-b"}
    colors = {row["vehicle_id"]: row["color"] for row in spec["layers"][0]["data"]}
    assert colors["bus-a"] != colors["bus-b"]


def test_vehicle_map_does_not_create_markers_for_empty_data():
    assert build_vehicle_map(pd.DataFrame()) is None
