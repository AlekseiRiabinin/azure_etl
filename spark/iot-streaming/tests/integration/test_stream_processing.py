from src.spark_etl.stream_processor import process_kafka_stream

def test_kafka_stream_parsing(spark):
    kafka_df = spark.createDataFrame(
        [("{\"meter_id\": \"sensor1\", \"voltage\": 230}",)],
        ["value"]
    )
    result = process_kafka_stream(kafka_df)
    assert "meter_id" in result.columns

def test_output_schema(spark):
    df = process_kafka_stream(spark.createDataFrame(...)) 
    assert set(df.columns) == {"meter_id", "voltage", "timestamp"}