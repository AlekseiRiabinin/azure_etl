import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import *
from typing import Generator

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
