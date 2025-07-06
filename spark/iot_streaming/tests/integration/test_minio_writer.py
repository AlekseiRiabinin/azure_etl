import os
from tempfile import TemporaryDirectory
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from spark.iot_streaming.src.spark_etl.stream_processor import write_to_minio

def parse_iot_kafka_stream(kafka_stream: DataFrame) -> DataFrame:
    """Parse raw Kafka JSON messages into structured IoT data."""
    iot_schema = StructType([
        StructField("meter_id", StringType(), nullable=False),
        StructField("kwh_usage", DoubleType(), nullable=False),
        StructField("voltage", IntegerType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False)
    ])

    return (kafka_stream
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), iot_schema).alias("data"))
        .select("data.*"))

def test_writing_to_minio_as_integration(
    spark: SparkSession,
    kafka_test_stream: DataFrame
) -> None:
    """Integration test for MinIO (mocked via file system) write functionality."""
    parsed_stream = parse_iot_kafka_stream(kafka_test_stream)

    with TemporaryDirectory() as tmpdir:
        minio_path = f"file://{tmpdir}/output"

        write_query = write_to_minio(parsed_stream, minio_path)

        write_query.processAllAvailable()
        write_query.stop()

        written_df = spark.read.format("delta").load(minio_path)

        expected_ids = {"HAMILTON_337", "AUCKLAND_218"}
        result_ids = {row["meter_id"] for row in written_df.collect()}

        assert result_ids == expected_ids
        assert written_df.count() == 2
        assert os.path.exists(f"{tmpdir}/output/_checkpoints")

def test_writing_to_minio_with_empty_stream(
    spark: SparkSession,
    empty_kafka_test_stream: DataFrame
) -> None:
    """Test writing to Minio/S3 an empty stream."""
    parsed_stream = parse_iot_kafka_stream(empty_kafka_test_stream)

    with TemporaryDirectory() as tmpdir:
        minio_path = f"file://{tmpdir}/output"

        write_query = write_to_minio(parsed_stream, minio_path)

        write_query.processAllAvailable()
        write_query.stop()

        assert os.path.exists(f"{tmpdir}/output/_delta_log"), "Delta log was not created"

        dt = DeltaTable.forPath(spark, minio_path)
        count = dt.toDF().count()
        assert count == 0, f"Expected 0 records in Delta table, found {count}"
