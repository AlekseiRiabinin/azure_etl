# test_spark.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AirflowIntegrationTest") \
    .getOrCreate()

df = spark.createDataFrame([("Test", 1), ("Spark", 2)], ["word", "count"])
df.show()

print("Spark test completed successfully!")
spark.stop()