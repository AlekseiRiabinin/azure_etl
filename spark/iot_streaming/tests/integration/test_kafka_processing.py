import shutil
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import *
from spark.iot_streaming.src.spark_etl.stream_processor import process_kafka_stream

def test_stream_processing(spark: SparkSession, kafka_test_stream: DataFrame) -> None:
    """Test the streaming pipeline end-to-end using memory sink."""
    processed_stream = process_kafka_stream(kafka_test_stream)
    
    query = (processed_stream
        .writeStream
        .format("memory")
        .queryName("test_output")
        .outputMode("append")
        .start())
    
    try:
        query.processAllAvailable() # only for testing       
        results = spark.sql("SELECT * FROM test_output").collect()
        
        assert len(results) == 2
        assert results[0]["meter_id"] == "HAMILTON_337"
        assert results[1]["meter_id"] == "AUCKLAND_218"
        
    finally:
        query.stop()

def test_processing_empty_kafka_stream(
        spark: SparkSession,
        empty_kafka_test_stream: DataFrame
) -> None:
    """Test handling of empty streams."""
    processed = process_kafka_stream(empty_kafka_test_stream)
    
    query = (processed
        .writeStream
        .format("memory")
        .queryName("test_empty_stream")
        .outputMode("append")
        .start())
    
    query.processAllAvailable()
    result = spark.sql("SELECT * FROM test_empty_stream")
    assert result.count() == 0
    
    query.stop()

def test_kafka_stream_parsing_invalid_data(
    spark: SparkSession,
    kafka_test_stream: DataFrame
) -> None:
    """Test how the stream processor handles invalid/malformed Kafka messages."""

    # missing voltage field    
    invalid_message = (
        '{"meter_id": "CORRUPTED_001", '
        '"kwh_usage": 1.23, '
        '"timestamp": "2025-06-10T14:00:00.000000Z"}'
    )    
    invalid_path = "/tmp/spark_test_invalid_stream"
    invalid_row = [Row(
        key=None,
        value=invalid_message.encode('utf-8'),
        topic="iot_meter_readings",
        partition=0,
        offset=999,
        timestamp=datetime.strptime("2025-06-10T14:00:00.000000Z", "%Y-%m-%dT%H:%M:%S.%fZ"),
        timestampType=0
    )]
    
    spark.createDataFrame(invalid_row, schema=kafka_test_stream.schema) \
        .write.mode("overwrite").parquet(invalid_path)
    
    invalid_stream = spark.readStream.schema(kafka_test_stream.schema).parquet(invalid_path)
    invalid_processed = process_kafka_stream(invalid_stream)

    invalid_query = (invalid_processed
        .writeStream
        .format("memory")
        .queryName("invalid_results")
        .start())
    
    try:
        invalid_query.processAllAvailable()
        invalid_results = spark.sql("SELECT * FROM invalid_results").collect()      
        assert len(invalid_results) in (0, 1)
        
        if invalid_results:
            row = invalid_results[0]
            assert row["meter_id"] == "CORRUPTED_001"

            if "voltage" in row:
                assert row["voltage"] is None

    finally:
        invalid_query.stop()
        shutil.rmtree(invalid_path, ignore_errors=True)

def test_processing_kafka_stream_schema(kafka_test_stream: DataFrame) -> None:
    """Verify the streaming query maintains correct schema after processing."""

    processed_df = process_kafka_stream(kafka_test_stream)
    expected_fields = {"meter_id", "voltage", "timestamp"}    

    assert processed_df.isStreaming
    assert expected_fields.issubset(set(processed_df.schema.fieldNames()))
