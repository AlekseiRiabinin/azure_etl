from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *
from typing import List
from datetime import datetime
import json
import pytest

@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Core Spark session fixture."""
    return (
        SparkSession.builder
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.host", "localhost")
            .config("spark.sql.streaming.schemaInference", "true")
            .getOrCreate()
    )

@pytest.fixture
def batch_test_data(spark: SparkSession) -> DataFrame:
    """Fixture for batch processing tests."""
    return spark.createDataFrame([
        Row(meter_id="DUNEDIN_222", kwh_usage=3.93, voltage=230, timestamp="2025-06-10T13:10:07.415964Z"),
        Row(meter_id="HAMILTON_337", kwh_usage=4.88, voltage=260, timestamp="2025-06-10T05:53:07.416031Z"),
        Row(meter_id="AUCKLAND_218", kwh_usage=2.96, voltage=250, timestamp="2025-06-10T13:48:07.416100Z")
    ])

@pytest.fixture
def boundary_test_data(spark_session: SparkSession) -> DataFrame:
    """Test data with boundary values around the anomaly threshold."""
    return spark_session.createDataFrame([
        Row(meter_id="BOUNDARY_249", voltage=249, kwh_usage=2.5),  # Below
        Row(meter_id="BOUNDARY_250", voltage=250, kwh_usage=2.5),  # At threshold
        Row(meter_id="BOUNDARY_251", voltage=251, kwh_usage=2.5)   # Above
    ])


def create_kafka_test_schema() -> StructType:
    """Schema for mock Kafka streams."""
    return StructType([
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType())
    ])

def create_mock_kafka_stream(
    spark: SparkSession,
    json_messages: List[str]
) -> DataFrame:
    """Create streaming DataFrame from JSON messages."""
    schema = create_kafka_test_schema()
    rows = [Row(
        key=None,
        value=msg.encode('utf-8'),
        topic="iot_meter_readings",
        partition=0,
        offset=i,
        timestamp=datetime.strptime(
            json.loads(msg)["timestamp"], 
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ),
        timestampType=0
    ) for i, msg in enumerate(json_messages)]    
    return spark.createDataFrame(rows, schema)

@pytest.fixture
def test_stream(spark: SparkSession) -> DataFrame:
    """Create streaming source for tests."""
    json_messages: List[str] = [
        (
            '{"meter_id": "HAMILTON_337", '
            '"kwh_usage": 3.93, '
            '"voltage": 230, '
            '"timestamp": "2025-06-10T13:10:07.415964Z"}'
        ),
        (
            '{"meter_id": "AUCKLAND_218", '
            '"kwh_usage": 4.88, '
            '"voltage": 260, '
            '"timestamp": "2025-06-10T13:48:07.416100Z"}'
        )
    ]
    return create_mock_kafka_stream(spark, json_messages)
