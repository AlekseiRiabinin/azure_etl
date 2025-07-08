from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.kafka import ProduceToTopicOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from datetime import datetime, timedelta

# End-to-End Smart Energy Pipeline

default_args = {
    'owner': 'energy_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'smart_energy_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Simulate IoT device messages
    produce_energy_events = ProduceToTopicOperator(
        task_id="produce_energy_events",
        kafka_conn_id="kafka_default",
        topic="raw_energy_readings",
        producer_config={
            "bootstrap.servers": "kafka-1:9092,kafka-2:9095"
        },
        message="""{
            "device_id": "sensor_{{ range(1, 100) | random }}",
            "timestamp": "{{ ts }}",
            "kwh_usage": {{ range(1, 10) | random }},
            "voltage": {{ range(220, 240) | random }},
            "current": {{ range(1, 20) | random }}
        }""",
    )

    # Stream processing with Spark
    process_stream = SparkSubmitOperator(
        application="/opt/airflow/dags/scripts/energy_stream_processor.py",
        conn_id="spark_default",
        task_id="process_energy_stream",
        application_args=[
            "--kafka-brokers", "kafka-1:9092,kafka-2:9095",
            "--input-topic", "raw_energy_readings",
            "--output-topic", "processed_energy_metrics",
            "--checkpoint-location", "s3a://energy-checkpoints/{{ ds_nodash }}",
            "--output-path", "s3a://energy-data/hourly/{{ ds_nodash }}/{{ execution_date.hour }}"
        ]
    )

    # Daily aggregation
    daily_aggregation = SparkSubmitOperator(
        application="/opt/airflow/dags/scripts/daily_energy_agg.py",
        conn_id="spark_default",
        task_id="daily_energy_aggregation",
        application_args=[
            "--input-path", "s3a://energy-data/hourly/{{ ds_nodash }}/*",
            "--output-path", "s3a://energy-data/daily/{{ ds_nodash }}",
            "--postgres-table", "public.daily_energy_consumption"
        ]
    )

    # Load to data warehouse (Postgres)
    load_to_dwh = PostgresOperator(
        task_id="load_to_data_warehouse",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO energy_analytics.daily_consumption
        SELECT * FROM public.daily_energy_consumption
        WHERE date = '{{ ds }}'
        ON CONFLICT (date, region) DO UPDATE
        SET total_kwh = EXCLUDED.total_kwh,
            avg_voltage = EXCLUDED.avg_voltage
        """
    )

    produce_energy_events >> process_stream >> daily_aggregation >> load_to_dwh