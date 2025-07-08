from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def check_jupyter_connection():
    """Verify JupyterLab files are accessible"""
    try:
        with open('/opt/airflow/dags/test_jupyter.txt', 'r') as f:
            content = f.read()
        print(f"Found Jupyter-created file with content: {content}")
        return True
    except Exception as e:
        print(f"Error reading Jupyter file: {e}")
        return False

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    'test_integration',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['integration_test']
) as dag:

    # Task 1: Verify JupyterLab connection
    test_jupyter = PythonOperator(
        task_id='test_jupyter_connection',
        python_callable=check_jupyter_connection
    )

    # Task 2: Test Kafka producer
    test_kafka = ProduceToTopicOperator(
        task_id='test_kafka_producer',
        topic='test_topic',
        producer_config={
            'bootstrap.servers': 'kafka-1:9092,kafka-2:9095'
        },
        message='Test message from Airflow at {{ ts }}',
        delivery_timeout=10
    )

    # Task 3: Test Spark connection
    test_spark = SparkSubmitOperator(
        task_id='test_spark_connection',
        application='/opt/airflow/dags/test_spark.py',
        conn_id='spark_default',
        verbose=True
    )

    # Task 4: Test Postgres connection
    test_postgres = PostgresOperator(
        task_id='test_postgres_connection',
        postgres_conn_id='postgres_default',
        sql="CREATE TABLE IF NOT EXISTS test_table (id SERIAL, message TEXT); \
             INSERT INTO test_table (message) VALUES ('Airflow test at {{ ts }}'); \
             SELECT * FROM test_table;"
    )

    # Task 5: Create a file for Jupyter to read
    create_for_jupyter = PythonOperator(
        task_id='create_for_jupyter',
        python_callable=lambda: open('/opt/airflow/dags/from_airflow.txt', 'w').write('Hello from Airflow!')
    )

    test_jupyter >> test_kafka >> test_spark >> test_postgres >> create_for_jupyter