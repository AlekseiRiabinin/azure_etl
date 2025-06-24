import os
from pyspark import SparkConf
from pyspark.sql import SparkSession

# Force Java 21 recognition
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-21-openjdk-amd64"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"

# Debug configuration
conf = SparkConf() \
    .setMaster("local[1]") \
    .setAppName("DebugSession") \
    .set("spark.driver.host", "localhost") \
    .set("spark.executor.memory", "1g") \
    .set("spark.driver.extraJavaOptions", f"-Djava.home={os.environ['JAVA_HOME']}")

# Create session with error handling
try:
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("SUCCESS! Spark version:", spark.version)
    spark.stop()
except Exception as e:
    print("FAILURE! Error details:")
    print(f"Type: {type(e)}")
    print(f"Message: {str(e)}")
    print(f"Java Home: {os.environ.get('JAVA_HOME')}")
    print(f"Python Path: {os.environ.get('PYSPARK_PYTHON')}")