# from pyspark.sql.types import BooleanType
# from src.spark_etl.stream_processor import detect_anomalies

# def test_detect_anomalies_schema(test_data):
#     result = detect_anomalies(test_data)
#     assert "is_anomaly" in result.columns
#     assert result.schema["is_anomaly"].dataType == BooleanType()