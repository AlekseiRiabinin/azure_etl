from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *

def get_spark_session():
    """Core Spark session fixture"""
    return (
        SparkSession.builder
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.streaming.schemaInference", "true")
        .getOrCreate()
    )

def create_batch_test_data(spark):
    """Fixture for batch processing tests"""
    return spark.createDataFrame([
        Row(meter_id="sensor1", voltage=230, kwh_usage=50.0, timestamp="2024-01-01 00:00:00"),
        Row(meter_id="sensor2", voltage=260, kwh_usage=60.0, timestamp="2024-01-01 00:01:00"),
        Row(meter_id="sensor3", voltage=250, kwh_usage=55.0, timestamp="2024-01-01 00:02:00")
    ])

def create_kafka_test_schema():
    """Schema for mock Kafka streams"""
    return StructType([
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType())
    ])

def create_mock_kafka_stream(spark, json_messages):
    """Create streaming DataFrame from JSON messages"""
    schema = create_kafka_test_schema()
    rows = [Row(
        key=None,
        value=msg.encode('utf-8'),
        topic="iot_meter_readings",
        partition=0,
        offset=i,
        timestamp="2024-01-01 00:00:00",
        timestampType=0
    ) for i, msg in enumerate(json_messages)]
    
    return spark.createDataFrame(rows, schema)

def create_test_stream(spark):
    """Create streaming source for integration tests"""
    json_messages = [
        '{"meter_id": "sensor1", "voltage": 230, "timestamp": "2024-01-01T00:00:00Z"}',
        '{"meter_id": "sensor2", "voltage": 260, "timestamp": "2024-01-01T00:01:00Z"}'
    ]
    return create_mock_kafka_stream(spark, json_messages)