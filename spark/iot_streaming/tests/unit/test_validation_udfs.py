from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import BooleanType


def test_validation_udfs(spark: SparkSession) -> bool:
    """Test validation UDFs."""
    # Test is_valid_voltage UDF
    @udf(returnType=BooleanType())
    def is_valid_voltage(voltage: int) -> bool:
        return voltage in [230, 240]
    
    test_data = [(230,), (240,), (250,), (0,)]
    test_df = spark.createDataFrame(test_data, ["voltage"])
    test_df = test_df.withColumn("is_valid", is_valid_voltage(col("voltage")))
    
    results = test_df.collect()
    assert results[0]["is_valid"] is True
    assert results[1]["is_valid"] is True
    assert results[2]["is_valid"] is False
    assert results[3]["is_valid"] is False
    
    # Test is_valid_kwh UDF
    @udf(returnType=BooleanType())
    def is_valid_kwh(kwh: float) -> bool:
        return 0 <= kwh <= 20
    
    test_data = [(0.0,), (10.0,), (20.0,), (-1.0,), (21.0,)]
    test_df = spark.createDataFrame(test_data, ["kwh"])
    test_df = test_df.withColumn("is_valid", is_valid_kwh(col("kwh")))
    
    results = test_df.collect()
    assert results[0]["is_valid"] is True
    assert results[1]["is_valid"] is True
    assert results[2]["is_valid"] is True
    assert results[3]["is_valid"] is False
    assert results[4]["is_valid"] is False
