from unittest.mock import MagicMock, call

from spark.common.sources.kafka_source import read_kafka_stream


def test_read_kafka_stream_configures_kafka_source() -> None:
    expected_df = object()
    reader = MagicMock()
    reader.format.return_value = reader
    reader.option.return_value = reader
    reader.load.return_value = expected_df

    spark = MagicMock()
    spark.readStream = reader

    result = read_kafka_stream(
        spark=spark,
        bootstrap_servers="kafka:9092",
        topic="transit.vehicle_positions.v1",
        starting_offsets="earliest",
    )

    reader.format.assert_called_once_with("kafka")
    assert reader.option.call_args_list == [
        call("kafka.bootstrap.servers", "kafka:9092"),
        call("subscribe", "transit.vehicle_positions.v1"),
        call("startingOffsets", "earliest"),
    ]
    reader.load.assert_called_once_with()
    assert result is expected_df
