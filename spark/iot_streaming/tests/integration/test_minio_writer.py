import os
from tempfile import TemporaryDirectory
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from spark.iot_streaming.src.spark_etl.stream_processor import write_to_minio

def test_writing_to_minio_as_integration(
    spark: SparkSession,
    kafka_test_stream: DataFrame
) -> None:
    """Integration test for MinIO (mocked via file system) write functionality."""

    iot_schema = StructType([
        StructField("meter_id", StringType(), nullable=False),
        StructField("kwh_usage", DoubleType(), nullable=False),
        StructField("voltage", IntegerType(), nullable=False),
        StructField("timestamp", TimestampType(), nullable=False)
    ])

    parsed_stream = (kafka_test_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), iot_schema).alias("data"))
        .select("data.*"))

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

# def test_writing_to_minio_with_empty_stream(empty_kafka_test_stream: DataFrame) -> None:
#     """Test writing to Minio/S3 an empty stream."""

#     with TemporaryDirectory() as tmpdir:
#         minio_path = f"file://{tmpdir}/output"
#         write_query = write_to_minio(empty_kafka_test_stream, minio_path)
#         write_query.processAllAvailable()
#         write_query.stop()

#         assert not os.path.exists(f"{tmpdir}/output/_delta_log")

# def test_write_to_minio_with_mock_kafka(
#         spark: SparkSession,
#         kafka_test_stream: DataFrame
# ) -> None:
#     """Integration test for MinIO (local file system) write functionality."""

#     with TemporaryDirectory() as tmpdir:
#         minio_path = f"file://{tmpdir}/output"
#         write_query = write_to_minio(kafka_test_stream, minio_path)
#         write_query.processAllAvailable()
#         write_query.stop()

#         df = spark.read.format("delta").load(minio_path)
#         assert df.count() == 2
#         assert set(df.columns) == {
#             "key", "value", "topic", "partition", "offset", "timestamp", "timestampType"
#         }
