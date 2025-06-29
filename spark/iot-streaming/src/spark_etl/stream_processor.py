from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
from pyspark.sql import SparkSession

def detect_anomalies(df: DataFrame) -> DataFrame:
    """Flag voltage > 250V as anomalies."""
    return df.withColumn(
        "is_anomaly",
        when(col("voltage") > 250, lit(True)).otherwise(lit(False))
    )

def process_kafka_stream(spark: SparkSession, bootstrap_servers: str, topic: str):
    """Read from Kafka and parse JSON payload."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load() \
        .selectExpr("CAST(value AS STRING) as json") \
        .selectExpr("json_tuple(json, 'meter_id', 'voltage', 'timestamp') as (meter_id, voltage, timestamp)")

def write_to_minio(df: DataFrame, minio_path: str):
    """Write (append) data to S3/MinIO."""
    df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{minio_path}/_checkpoints") \
        .start(minio_path)
