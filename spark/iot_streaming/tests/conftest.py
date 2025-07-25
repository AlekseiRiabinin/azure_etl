import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
from typing import Generator
from datetime import datetime


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    spark = (SparkSession.builder
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate())
    yield spark
    spark.stop()


@pytest.fixture
def spark(spark_session: SparkSession) -> SparkSession:
    """Function-scoped Spark session (no teardown - uses session-scoped one)."""
    return spark_session


@pytest.fixture
def batch_test_data(spark: SparkSession) -> DataFrame:
    """Test data fixture with explicit schema."""
    schema = StructType([
        StructField("meter_id", StringType(), nullable=False),
        StructField("kwh_usage", DoubleType(), nullable=False),
        StructField("voltage", IntegerType(), nullable=False),
        StructField("timestamp", StringType(), nullable=False)
    ])
    
    df = spark.createDataFrame([
        ("DUNEDIN_222", 3.93, 230, "2025-06-10T13:10:07"),
        ("HAMILTON_337", 4.88, 260, "2025-06-10T05:53:07"),
        ("AUCKLAND_218", 2.96, 250, "2025-06-10T13:48:07")
    ], schema=schema)
    
    # Convert to timestamp
    return df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))


@pytest.fixture
def boundary_test_data(spark: SparkSession) -> DataFrame:
    """Test data with boundary values around the anomaly threshold."""
    return spark.createDataFrame([
        Row(meter_id="BOUNDARY_249", voltage=249, kwh_usage=2.5),  # Below
        Row(meter_id="BOUNDARY_250", voltage=250, kwh_usage=2.5),  # At threshold
        Row(meter_id="BOUNDARY_251", voltage=251, kwh_usage=2.5)   # Above
    ])


@pytest.fixture
def test_kafka_data(spark: SparkSession) -> DataFrame:
    """Fixture that provides simulated Kafka message data in Spark DataFrame format."""
    schema = StructType([
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", IntegerType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType())
    ])
    
    test_data = [
        ('key1', '{"meter_id":"m1","timestamp":"2023-01-01T12:00:00Z","kwh_usage":1.5,"voltage":230,"customer_id":"c1","region":"Auckland"}', 'smart_meter_data', 0, 1, datetime.now(), 0),
        ('key2', '{"meter_id":"m2","timestamp":"2023-01-01T12:01:00Z","kwh_usage":2.0,"voltage":240,"customer_id":"c2","region":"Wellington"}', 'smart_meter_data', 0, 2, datetime.now(), 0),
        ('key3', '{"meter_id":"m3","timestamp":"2023-01-01T12:02:00Z","kwh_usage":-1.0,"voltage":0,"customer_id":"c3","region":"Christchurch"}', 'smart_meter_data', 0, 3, datetime.now(), 0),
        ('key4', '{"meter_id":"m4","timestamp":"2023-01-01T12:03:00Z","kwh_usage":25.0,"voltage":250,"customer_id":"c4","region":"Auckland"}', 'smart_meter_data', 0, 4, datetime.now(), 0),
        ('key5', 'invalid_json_data', 'smart_meter_data', 0, 5, datetime.now(), 0)
    ]
    
    return spark.createDataFrame(test_data, schema)


@pytest.fixture
def parsed_df(spark: SparkSession) -> DataFrame:
    """Fixture providing parsed smart meter data in the target schema after JSON processing."""
    meter_schema = StructType([
        StructField("meter_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("kwh_usage", DoubleType()),
        StructField("voltage", IntegerType()),
        StructField("customer_id", StringType()),
        StructField("region", StringType())
    ])
    
    test_data = [
        ("m1", datetime(2023, 1, 1, 12, 0), 1.5, 230, "c1", "Auckland"),
        ("m2", datetime(2023, 1, 1, 12, 1), 2.0, 240, "c2", "Wellington"),
        ("m3", datetime(2023, 1, 1, 12, 2), -1.0, 0, "c3", "Christchurch"),
        ("m4", datetime(2023, 1, 1, 12, 3), 25.0, 250, "c4", "Auckland"),
        (None, datetime(2023, 1, 1, 12, 4), 1.0, 230, None, "Auckland")
    ]
    
    return spark.createDataFrame(test_data, meter_schema)

