from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, StructType, StructField
from spark.iot_streaming.src.spark_etl.stream_processor import process_kafka_stream

def test_kafka_stream_parsing(kafka_test_stream: DataFrame) -> None:
    """Test JSON parsing from Kafka message format."""
    processed = process_kafka_stream(kafka_test_stream)
    
    # Check schema
    expected_schema = StructType([
        StructField("meter_id", StringType()),
        StructField("voltage", StringType()),
        StructField("timestamp", StringType())
    ])
    assert processed.schema == expected_schema
    
    rows = processed.collect()
    assert len(rows) == 2
    assert rows[0]["meter_id"] == "HAMILTON_337"
    assert rows[0]["voltage"] == "230"
    assert rows[1]["meter_id"] == "AUCKLAND_218"

def test_streaming_processing(spark: SparkSession, kafka_test_stream: DataFrame) -> None:
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

def test_process_empty_kafka_test_stream(empty_kafka_test_stream: DataFrame) -> None:
    """Test handling of empty streams."""
    processed = process_kafka_stream(empty_kafka_test_stream)
    assert processed.count() == 0

def test_process_kafka_stream_schema(kafka_test_stream: DataFrame) -> None:
    """Verify the streaming query maintains correct schema after processing."""
    processed_df = process_kafka_stream(kafka_test_stream)
    
    assert processed_df.isStreaming
    
    expected_fields = {"meter_id", "voltage", "timestamp"}
    assert expected_fields.issubset(set(processed_df.schema.fieldNames()))
