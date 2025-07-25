from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *
from pyspark.sql import DataFrame


def test_parse_kafka_data(test_kafka_data: DataFrame) -> bool:
    """Test parsing of Kafka data."""    
    meter_schema = StructType([
        StructField("meter_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("kwh_usage", DoubleType()),
        StructField("voltage", IntegerType()),
        StructField("customer_id", StringType()),
        StructField("region", StringType())
    ])
    
    parsed_df = test_kafka_data.select(
        from_json(col("value").cast("string"), meter_schema).alias("data")
    ).select("data.*")
    
    # Valid records should be parsed correctly
    valid_records = parsed_df.filter(col("meter_id").isin(["m1", "m2", "m3", "m4"]))
    assert valid_records.count() == 4
    
    # Invalid JSON should be null
    invalid_records = parsed_df.filter(col("meter_id").isNull())
    assert invalid_records.count() == 1
