import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("transitpulse-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark
    spark.stop()