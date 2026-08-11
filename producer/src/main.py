import json

from clients.mbta_client import MBTAClient
from parsers.alert_parser import parse_service_alerts
from parsers.gtfs_realtime_parser import parse_feed
from parsers.trip_update_parser import parse_trip_updates
from parsers.vehicle_position_parser import parse_vehicle_positions


def main() -> None:
    client = MBTAClient(timeout_seconds=30)

    try:
        vehicle_response = client.fetch_feed("vehicle_positions")
        vehicle_feed = parse_feed(vehicle_response.payload)
        vehicle_events = parse_vehicle_positions(
            vehicle_feed,
            vehicle_response.fetched_at,
        )

        trip_response = client.fetch_feed("trip_updates")
        trip_feed = parse_feed(trip_response.payload)
        trip_events = parse_trip_updates(
            trip_feed,
            trip_response.fetched_at,
        )

        alert_response = client.fetch_feed("alerts")
        alert_feed = parse_feed(alert_response.payload)
        alert_events = parse_service_alerts(
            alert_feed,
            alert_response.fetched_at,
        )

        print(f"Vehicle events: {len(vehicle_events)}")
        print(f"Trip update events: {len(trip_events)}")
        print(f"Service alert events: {len(alert_events)}")

        print("\nVehicle sample:")
        print(json.dumps(vehicle_events[:1], indent=2))

        print("\nTrip update sample:")
        print(json.dumps(trip_events[:1], indent=2))

        print("\nService alert sample:")
        print(json.dumps(alert_events[:1], indent=2))

    finally:
        client.close()


if __name__ == "__main__":
    main()
