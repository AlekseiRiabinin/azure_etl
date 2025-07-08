from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.kafka import ProduceToTopicOperator
from datetime import datetime, timedelta

# Kafka Event Processing with Spark Streaming

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kafka_spark_streaming',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    # Produce test messages to Kafka
    produce_events = ProduceToTopicOperator(
        task_id="produce_test_events",
        kafka_conn_id="kafka_default",
        topic="raw_events",
        producer_config={
            "bootstrap.servers": "kafka-1:9092,kafka-2:9095"
        },
        message="{'event_time': '{{ ts }}', 'value': {{ range(1, 100) | random }}}",
    )

    # Spark Streaming job to process Kafka events
    spark_streaming = SparkSubmitOperator(
        application="/opt/airflow/dags/scripts/kafka_stream_processor.py",
        conn_id="spark_default",
        task_id="spark_streaming_job",
        application_args=[
            "--kafka-brokers", "kafka-1:9092,kafka-2:9095",
            "--input-topic", "raw_events",
            "--output-topic", "processed_events",
            "--checkpoint-location", "s3a://spark-checkpoints/{{ ds_nodash }}"
        ]
    )

    produce_events >> spark_streaming