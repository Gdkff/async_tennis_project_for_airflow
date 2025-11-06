from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules.run_manager import load_tournaments_results


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='t24_load_tournaments_results',
    default_args=default_args,
    description='Test DAG',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_load_tournaments_results = PythonOperator(
        task_id="t24_load_tournaments_results",
        python_callable=load_tournaments_results
    )
