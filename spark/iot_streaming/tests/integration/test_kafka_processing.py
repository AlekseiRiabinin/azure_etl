# import pytest
# from pyspark.sql import SparkSession, Row
# from pyspark.sql.types import (
#   StructType,
#   StructField,
#   StringType,
#   IntegerType,
#   TimestampType
# )
# from src.spark_etl.stream_processor import process_kafka_stream

# @pytest.fixture
# def kafka_test_schema():
#     """Define schema for mock Kafka data"""
#     return StructType([
#         StructField("key", StringType()),
#         StructField("value", StringType()),
#         StructField("topic", StringType()),
#         StructField("partition", IntegerType()),
#         StructField("offset", IntegerType()),
#         StructField("timestamp", TimestampType()),
#     ])

# @pytest.fixture
# def mock_kafka_data(spark, kafka_test_schema):
#     """Create mock Kafka DataFrame with realistic structure"""
#     test_data = [
#         Row(
#             key=None,
#             value='{"meter_id": "sensor1", "voltage": 230, "timestamp": "2024-01-01T00:00:00Z"}',
#             topic="iot_meter_readings",
#             partition=0,
#             offset=1,
#             timestamp="2024-01-01 00:00:00"
#         ),
#         Row(
#             key=None,
#             value='{"meter_id": "sensor2", "voltage": 260, "timestamp": "2024-01-01T00:01:00Z"}',
#             topic="iot_meter_readings",
#             partition=0,
#             offset=2,
#             timestamp="2024-01-01 00:01:00"
#         )
#     ]
#     return spark.createDataFrame(test_data, schema=kafka_test_schema)

# def test_kafka_stream_parsing(mock_kafka_data):
#     """Test if the stream processor correctly parses Kafka messages"""
#     processed = process_kafka_stream(mock_kafka_data)
    
#     # Check expected columns exist
#     assert set(processed.columns) == {"meter_id", "voltage", "timestamp"}
    
#     # Check data parsing
#     rows = processed.collect()
#     assert len(rows) == 2
#     assert rows[0]["meter_id"] == "sensor1"
#     assert rows[1]["voltage"] == 260

# def test_malformed_kafka_messages(spark, kafka_test_schema):
#     """Test handling of invalid JSON messages"""
#     bad_data = [
#         Row(
#             key=None,
#             value="NOT_JSON",  # Invalid message
#             topic="iot_meter_readings",
#             partition=0,
#             offset=3,
#             timestamp="2024-01-01 00:02:00"
#         )
#     ]
#     bad_df = spark.createDataFrame(bad_data, schema=kafka_test_schema)
    
#     processed = process_kafka_stream(bad_df)
#     assert processed.count() == 0  # Should filter out bad records

# def test_streaming_query_schema(spark, mock_kafka_data):
#     """Verify the streaming query maintains correct schema"""
#     from pyspark.sql.streaming import StreamingQuery
    
#     # Create streaming DF
#     stream_df = spark.readStream.format("memory").load()
    
#     # Apply processing
#     processed_stream = process_kafka_stream(stream_df)
    
#     # Test schema without starting the stream
#     assert processed_stream.isStreaming
#     assert {"meter_id", "voltage", "timestamp"}.issubset(set(processed_stream.schema.fieldNames()))