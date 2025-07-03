from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit

def detect_anomalies(df: DataFrame) -> DataFrame:
    """Flag voltage > 250V as anomalies."""
    return df.withColumn(
        "is_anomaly",
        when(col("voltage") > 250, lit(True)).otherwise(lit(False))
    )

def process_kafka_stream(df: DataFrame) -> DataFrame:
    """Read from Kafka and parse JSON payload."""
    return df \
        .selectExpr("CAST(value AS STRING) as json") \
        .selectExpr("json_tuple(json, 'meter_id', 'voltage', 'timestamp') as (meter_id, voltage, timestamp)")

def write_to_minio(df: DataFrame, minio_path: str) -> None:
    """Write (append) data to S3/MinIO."""
    df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{minio_path}/_checkpoints") \
        .start(minio_path)
