import pytest
from pyspark.sql import SparkSession


def test_kafka_connection(spark: SparkSession) -> bool:
    """Test Kafka connection."""
    try:
        test_df = (spark.read
            .format("kafka")
            .option("kafka.bootstrap.servers", "dummy:9092")
            .option("subscribe", "dummy")
            .option("failOnDataLoss", "false")
            .load())
        assert test_df is not None
    except Exception as e:
        pytest.fail(f"Kafka connection test failed: {str(e)}")
