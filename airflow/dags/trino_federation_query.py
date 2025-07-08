from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import io

# Trino Federation Query with Results to MinIO

def upload_to_minio(data, key, bucket_name='trino-results'):
    s3_hook = S3Hook(aws_conn_id='minio_default')
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer, index=False)
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket_name,
        replace=True
    )

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'trino_federation_query',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Query joining data from Postgres and MinIO (via Trino)
    trino_query = TrinoOperator(
        task_id='execute_federated_query',
        trino_conn_id='trino_default',
        sql="""
        INSERT INTO minio.trino_results.daily_combined
        SELECT 
            p.customer_id,
            p.transaction_date,
            p.amount,
            c.customer_name,
            c.segment
        FROM postgres.public.transactions p
        JOIN minio.customer_data.customers c
        ON p.customer_id = c.customer_id
        WHERE p.transaction_date = DATE '{{ ds }}'
        """
    )

    # Alternative approach: Fetch data to Airflow and then upload to MinIO
    fetch_data = TrinoOperator(
        task_id='fetch_data_with_trino',
        trino_conn_id='trino_default',
        sql="""
        SELECT * FROM postgres.public.transactions
        WHERE transaction_date = DATE '{{ ds }}'
        """,
        handler=lambda x: upload_to_minio(
            x,
            key='postgres_data/{{ ds_nodash }}.csv'
        )
    )

    trino_query >> fetch_data