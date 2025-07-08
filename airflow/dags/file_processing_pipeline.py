from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator

# Basic File Processing Pipeline (MinIO → Spark → Postgres)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'minio_spark_postgres_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Process raw data with Spark
    spark_process = SparkSubmitOperator(
        application="/opt/airflow/dags/scripts/spark_processor.py",
        conn_id="spark_default",
        task_id="spark_process",
        application_args=[
            "--input", "s3a://raw-data-bucket/daily_data_{{ ds_nodash }}.parquet",
            "--output", "s3a://processed-data-bucket/result_{{ ds_nodash }}.parquet"
        ]
    )

    # Load processed data to Postgres
    load_to_postgres = PostgresOperator(
        task_id="create_target_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            date DATE,
            metric_name VARCHAR(255),
            value FLOAT,
            PRIMARY KEY (date, metric_name)
        """
    )

    transfer_to_postgres = S3ToSqlOperator(
        task_id="transfer_to_postgres",
        s3_bucket="processed-data-bucket",
        s3_key="result_{{ ds_nodash }}.parquet",
        table="daily_metrics",
        column_list=["date", "metric_name", "value"],
        parser="parquet",
        postgres_conn_id="postgres_default",
        aws_conn_id="minio_default"
    )

    spark_process >> load_to_postgres >> transfer_to_postgres