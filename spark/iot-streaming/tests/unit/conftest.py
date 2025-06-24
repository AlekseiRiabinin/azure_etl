import pytest
from pyspark.sql import SparkSession, Row

@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.host", "localhost")
            .getOrCreate()
    )
    yield spark
    spark.stop()

@pytest.fixture
def test_data(spark):
    data = [
        Row(meter_id="sensor1", voltage=230, kwh_usage=50.0),
        Row(meter_id="sensor2", voltage=260, kwh_usage=60.0),
        Row(meter_id="sensor3", voltage=250, kwh_usage=55.0)
    ]
    return spark.createDataFrame(data)