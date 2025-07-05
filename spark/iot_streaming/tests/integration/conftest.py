import pytest
import json
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *
from datetime import datetime
from typing import List, Generator

@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """Session-scoped Spark fixture with test-optimized config."""
    spark = (SparkSession.builder
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate())
    yield spark
    spark.stop()

@pytest.fixture
def spark(spark_session: SparkSession) -> SparkSession:
    """Function-scoped Spark session (no teardown - uses session-scoped one)."""
    return spark_session

def _create_kafka_test_schema() -> StructType:
    """Private helper for Kafka stream schema."""
    return StructType([
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType())
    ])

def _create_mock_kafka_stream(spark: SparkSession, json_messages: List[str]) -> DataFrame:
    """Creates a streaming DataFrame from test messages."""
    schema = _create_kafka_test_schema()
    
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
    
    test_data_path = "/tmp/spark_test_stream"
    df = spark.createDataFrame(rows, schema)
    df.write.mode("overwrite").parquet(test_data_path)
    
    return spark.readStream.schema(schema).parquet(test_data_path)

@pytest.fixture
def kafka_test_stream(spark: SparkSession) -> DataFrame:
    """Main streaming test fixture."""
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
    return _create_mock_kafka_stream(spark, json_messages)

@pytest.fixture 
def empty_kafka_test_stream(spark: SparkSession) -> DataFrame:
    """Edge case: Empty stream."""
    return _create_mock_kafka_stream(spark, [])

@pytest.fixture
def cleanup_test_files() -> Generator[None, None, None]:
    """Cleans up test files after each test completes."""
    yield
    import shutil
    shutil.rmtree("/tmp/spark_test_stream", ignore_errors=True)
