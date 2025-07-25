from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def test_null_handling(parsed_df: DataFrame) -> bool:
    """Test null handling in transformation."""
    initial_count = parsed_df.count()
    filtered_df = parsed_df.filter(
        col("meter_id").isNotNull() & 
        col("customer_id").isNotNull() &
        col("timestamp").isNotNull()
    )

    # One record has null meter_id and customer_id    
    assert filtered_df.count() == initial_count - 1
