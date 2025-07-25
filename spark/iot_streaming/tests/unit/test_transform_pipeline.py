import pytest
from pyspark.sql.functions import current_timestamp, to_date, hour, col, when
from pyspark.sql import DataFrame


def test_transform_pipeline(parsed_df: DataFrame) -> bool:
    """Test transformation logic."""   
    enhanced_df = (parsed_df
        .withColumn("processing_time", current_timestamp())
        .withColumn("date", to_date(col("timestamp")))
        .withColumn("hour_of_day", hour(col("timestamp"))))
    
    # Test basic transformations
    assert "processing_time" in enhanced_df.columns
    assert "date" in enhanced_df.columns
    assert "hour_of_day" in enhanced_df.columns
    
    # Test cost calculation based on region
    enhanced_df = enhanced_df.withColumn("cost", 
        when(col("region") == "Auckland", col("kwh_usage") * 0.25)
        .when(col("region") == "Wellington", col("kwh_usage") * 0.23)
        .otherwise(col("kwh_usage") * 0.20))
    
    auckland_cost = enhanced_df.filter(col("meter_id") == "m1").select("cost").first()[0]
    assert auckland_cost == pytest.approx(1.5 * 0.25)
    
    # Test data quality flags
    enhanced_df = enhanced_df.withColumn("is_valid_voltage", 
        col("voltage").isin([230, 240]))
    enhanced_df = enhanced_df.withColumn("is_valid_kwh", 
        (col("kwh_usage") >= 0) & (col("kwh_usage") <= 20))
    
    invalid_voltage = enhanced_df.filter(col("meter_id") == "m4").select("is_valid_voltage").first()[0]
    assert not invalid_voltage
    
    invalid_kwh = enhanced_df.filter(col("meter_id") == "m3").select("is_valid_kwh").first()[0]
    assert not invalid_kwh
