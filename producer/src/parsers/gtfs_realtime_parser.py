from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

class GTFSRealtimeParseError(ValueError):
    """Payload khong phai GTFS-RealTime hop le!"""

def parse_feed(payload: bytes) -> gtfs_realtime_pb2.FeedMessage:
    if not payload:
        raise GTFSRealtimeParseError("GTFS-Realtime payload is empty!")

    feed = gtfs_realtime_pb2.FeedMessage()

    try:
        feed.ParseFromString(payload)
    except DecodeError as exc:
        raise GTFSRealtimeParseError(
            "Could not decode GTFS-Realtime payload"
        ) from exc

    if not feed.IsInitialized():
        missing_fields = feed.FindInitializationErrors()

        raise GTFSRealtimeParseError(
            f"Missing required fields: {missing_fields}"
        )

    return feed
