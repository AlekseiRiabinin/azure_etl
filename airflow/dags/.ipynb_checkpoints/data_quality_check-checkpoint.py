from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from datetime import datetime, timedelta

# Data Quality Check with Great Expectations

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_quality_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Load data from MinIO to Postgres
    load_data = S3ToSqlOperator(
        task_id="load_data_to_postgres",
        s3_bucket="source-data",
        s3_key="daily_updates/{{ ds_nodash }}.csv",
        table="staging_table",
        column_list=["id", "name", "value", "date"],
        parser="csv",
        postgres_conn_id="postgres_default",
        aws_conn_id="minio_default"
    )

    # Data quality check with Great Expectations
    data_quality_check = GreatExpectationsOperator(
        task_id="data_quality_check",
        conn_id="postgres_default",
        expectation_suite_name="staging_data_suite",
        data_context_root_dir="/opt/airflow/great_expectations",
        data_asset_name="public.staging_table",
        execution_engine="Postgres",
        return_json_dict=True
    )

    # Only proceed if data quality checks pass
    transform_data = PostgresOperator(
        task_id="transform_and_load",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO production_table
        SELECT * FROM staging_table
        WHERE date = '{{ ds }}'
        """,
        trigger_rule="all_success"
    )

    load_data >> data_quality_check >> transform_data