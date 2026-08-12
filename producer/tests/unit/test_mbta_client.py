from datetime import UTC, datetime
from unittest.mock import Mock, patch

import pytest
import requests

from producer.src.clients.mbta_client import MBTAClient, MBTAClientError


def test_fetch_feed_returns_payload_and_fetch_timestamp() -> None:
    response = Mock()
    response.content = b"protobuf payload"
    response.raise_for_status.return_value = None

    client = MBTAClient()
    with patch.object(client.session, "get", return_value=response):
        result = client.fetch_feed("vehicle_positions")

    assert result.feed_name == "vehicle_positions"
    assert result.source_url.endswith("VehiclePositions.pb")
    assert result.fetched_at.tzinfo == UTC
    assert result.fetched_at <= datetime.now(UTC)
    assert result.payload == b"protobuf payload"


def test_fetch_feed_rejects_empty_response() -> None:
    response = Mock()
    response.content = b""
    response.raise_for_status.return_value = None

    client = MBTAClient()
    with (
        patch.object(client.session, "get", return_value=response),
        pytest.raises(MBTAClientError, match="empty vehicle_positions response"),
    ):
        client.fetch_feed("vehicle_positions")


def test_fetch_feed_wraps_request_errors() -> None:
    client = MBTAClient()
    with (
        patch.object(client.session, "get", side_effect=requests.Timeout("timed out")),
        pytest.raises(MBTAClientError, match="Failed to fetch vehicle_positions"),
    ):
        client.fetch_feed("vehicle_positions")


def test_close_closes_session() -> None:
    client = MBTAClient()
    with patch.object(client.session, "close") as close:
        client.close()

    close.assert_called_once_with()
