from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time


def print_hello():
    print("Hello from Airflow 3 DAG!")


def wait_five_seconds():
    time.sleep(5)
    print("Waited for 5 seconds")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
        dag_id='test_dag_airflow_3',
        default_args=default_args,
        description='Простой тестовый DAG для Airflow 3',
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:
    task_hello = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello,
    )

    task_wait = PythonOperator(
        task_id='wait_five_seconds',
        python_callable=wait_five_seconds,
    )

    task_hello >> task_wait
