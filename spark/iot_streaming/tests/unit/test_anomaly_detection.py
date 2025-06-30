import pytest
from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql.types import *
from spark.iot_streaming.src.spark_etl.stream_processor import detect_anomalies

@pytest.mark.parametrize("voltage,expected", [
    (240, False),  # Normal
    (250, False),  # Boundary
    (251, True),   # Anomaly
    (300, True)    # Extreme
])
def test_anomaly_threshold(
    spark_session: SparkSession,
    voltage: int,
    expected: bool
) -> None:
    """Verify that voltage values above 250 are flagged as anomalies."""
    test_df = spark_session.createDataFrame(
        [Row(meter_id="test", kwh_usage=1.00, voltage=voltage)]
    )
    result = detect_anomalies(test_df).first()
    assert result["is_anomaly"] == expected

def test_detect_anomalies_identifies_high_voltage(batch_test_data: DataFrame) -> None:
    """Test that high voltage (>250) records are flagged as anomalies."""
    result = detect_anomalies(batch_test_data)
    
    # Get all anomalies
    anomalies = result.filter("is_anomaly = True").collect()
    
    # Verify only HAMILTON_337 (voltage=260) is flagged
    assert len(anomalies) == 1
    assert anomalies[0]["meter_id"] == "HAMILTON_337"
    assert anomalies[0]["voltage"] == 260

def test_detect_anomalies_preserves_normal_records(batch_test_data: DataFrame) -> None:
    """Test that normal voltage records are not flagged."""
    result = detect_anomalies(batch_test_data)
    
    # Get all normal records
    normals = result.filter("is_anomaly = False").collect()
    normal_ids = {row["meter_id"] for row in normals}
    
    # Verify DUNEDIN_222 and AUCKLAND_218 are normal
    assert len(normals) == 2
    assert normal_ids == {"DUNEDIN_222", "AUCKLAND_218"}
    assert all(row["voltage"] <= 250 for row in normals)

def test_empty_dataframe(spark_session: SparkSession, batch_test_data: DataFrame) -> None:
    """Verify function handles empty input gracefully."""
    # Get schema from existing fixture to ensure consistency
    empty_df = spark_session.createDataFrame([], schema=batch_test_data.schema)
    result = detect_anomalies(empty_df)
    
    assert result.count() == 0, "Empty input should return empty result"
    assert "is_anomaly" in result.columns, "Output should include anomaly flag column"
