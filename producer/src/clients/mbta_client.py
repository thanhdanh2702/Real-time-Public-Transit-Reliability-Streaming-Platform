from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import requests

FeedName = Literal["vehicle_positions", "trip_updates", "alerts"]

FEED_URLS: dict[FeedName, str] = {
    "vehicle_positions": "https://cdn.mbta.com/realtime/VehiclePositions.pb",
    "trip_updates": "https://cdn.mbta.com/realtime/TripUpdates.pb",
    "alerts": "https://cdn.mbta.com/realtime/Alerts.pb",
}


class MBTAClientError(Exception):
    """Lỗi khi tải dữ liệu từ MBTA."""


@dataclass(frozen=True)
class FeedResponse:
    feed_name: FeedName
    source_url: str
    fetched_at: datetime
    payload: bytes


class MBTAClient:
    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()

        self.session.headers.update(
            {
                "User-Agent": "TransitPulse/1.0",
                "Accept": "application/x-protobuf",
            }
        )

    def fetch_feed(self, feed_name: FeedName) -> FeedResponse:
        url = FEED_URLS[feed_name]

        try:
            response = self.session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise MBTAClientError(f"Failed to fetch {feed_name} from {url}") from exc

        if not response.content:
            raise MBTAClientError(f"MBTA returned an empty {feed_name} response")

        return FeedResponse(
            feed_name=feed_name,
            source_url=url,
            fetched_at=datetime.now(UTC),
            payload=response.content,
        )

    def close(self) -> None:
        self.session.close()
