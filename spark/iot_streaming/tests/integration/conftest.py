import pytest
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *
from datetime import datetime
import json
from typing import List

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

def _create_mock_kafka_stream(
    spark: SparkSession,
    json_messages: List[str]
) -> DataFrame:
    """Private helper to create streaming DataFrames."""
    schema = _create_kafka_test_schema()
    rows = [Row(
        key=None,
        value=msg.encode('utf-8'),
        topic="iot_meter_readings",
        partition=0,
        offset=i,
        timestamp=datetime.strptime(
            json.loads(msg)["timestamp"], 
            "%Y-%m-%dT%H:%M.%fZ"
        ),
        timestampType=0
    ) for i, msg in enumerate(json_messages)]
    return spark.createDataFrame(rows, schema)

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
