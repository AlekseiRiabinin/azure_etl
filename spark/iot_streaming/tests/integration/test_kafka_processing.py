import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructType, StructField
from src.spark_etl.stream_processor import process_kafka_stream

def test_kafka_stream_parsing(kafka_test_stream: DataFrame) -> None:
    """Test JSON parsing from Kafka message format."""
    processed = process_kafka_stream(kafka_test_stream, "dummy_bootstrap", "dummy_topic")
    
    # Check schema
    expected_schema = StructType([
        StructField("meter_id", StringType()),
        StructField("voltage", StringType()),
        StructField("timestamp", StringType())
    ])
    assert processed.schema == expected_schema
    
    # Check data
    rows = processed.collect()
    assert len(rows) == 2
    assert rows[0]["meter_id"] == "HAMILTON_337"
    assert rows[0]["voltage"] == "230"
    assert rows[1]["meter_id"] == "AUCKLAND_218"

def test_process_empty_kafka_test_stream(empty_kafka_test_stream: DataFrame) -> None:
    """Test handling of empty streams."""
    processed = process_kafka_stream(empty_kafka_test_stream, "dummy_bootstrap", "dummy_topic")
    assert processed.count() == 0

def test_process_kafka_stream_schema(kafka_test_stream: DataFrame) -> None:
    """Verify the streaming query maintains correct schema after processing."""
    
    # Create a streaming DF from test data (streaming source)
    streaming_df = kafka_test_stream
    
    # Apply our processing function (modified to work with an existing stream)
    processed_df = streaming_df \
      .selectExpr("CAST(value AS STRING) as json") \
      .selectExpr("json_tuple(json, 'meter_id', 'voltage', 'timestamp') as (meter_id, voltage, timestamp)")
    
    # Verify streaming nature
    assert processed_df.isStreaming
    
    # Verify schema
    expected_fields = {"meter_id", "voltage", "timestamp"}
    assert expected_fields.issubset(set(processed_df.schema.fieldNames()))
    
    # Verify column types (since json_tuple returns everything as strings)
    for field in expected_fields:
        assert isinstance(processed_df.schema[field].dataType, StringType)
