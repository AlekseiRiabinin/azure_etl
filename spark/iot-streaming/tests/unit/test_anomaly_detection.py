import pytest
from pyspark.sql import Row
from src.spark_etl.stream_processor import detect_anomalies

@pytest.mark.parametrize("voltage,expected", [
    (240, False),  # Normal
    (250, False),  # Boundary
    (251, True),   # Anomaly
    (300, True)    # Extreme
])
def test_anomaly_threshold(spark, voltage, expected):
    test_df = spark.createDataFrame(
        [Row(meter_id="test", voltage=voltage, kwh_usage=50.0)]
    )
    result = detect_anomalies(test_df).first()
    assert result["is_anomaly"] == expected

def test_detect_anomalies_identifies_high_voltage(test_data):
    result = detect_anomalies(test_data)
    anomalies = result.filter("is_anomaly = True").collect()
    assert len(anomalies) == 1
    assert anomalies[0]["meter_id"] == "sensor2"

def test_detect_anomalies_preserves_normal_records(test_data):
    result = detect_anomalies(test_data)
    normals = result.filter("is_anomaly = False").collect()
    assert len(normals) == 2
    assert {r["meter_id"] for r in normals} == {"sensor1", "sensor3"}