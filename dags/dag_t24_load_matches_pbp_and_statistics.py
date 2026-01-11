from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules_t24.t24__run_manager import t24_load_matches_pbp_and_statistics


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='t24_load_matches_pbp_and_statistics',
    default_args=default_args,
    description='Test DAG',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_load_matches_pbp_and_statistics = PythonOperator(
        task_id="t24_load_matches_pbp_and_statistics",
        python_callable=t24_load_matches_pbp_and_statistics
    )
