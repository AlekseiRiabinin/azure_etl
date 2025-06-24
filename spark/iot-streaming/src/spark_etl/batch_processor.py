from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def aggregate_daily_usage(spark: SparkSession, minio_path: str) -> DataFrame:
    return spark.read.format("delta").load(minio_path) \
        .groupBy("meter_id", F.date_trunc("day", "timestamp").alias("day")) \
        .agg(F.avg("voltage").alias("avg_voltage"))