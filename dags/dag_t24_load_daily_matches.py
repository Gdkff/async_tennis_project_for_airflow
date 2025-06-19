from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules.t24_load_daily_matches import (t24_load_daily_matches,
                                                 t24_load_initial_match_data,
                                                 t24_load_final_match_data)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='t24_load_daily_matches',
    default_args=default_args,
    description='Test DAG',
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_load_daily_matches = PythonOperator(
        task_id="t24_load_daily_matches",
        python_callable=t24_load_daily_matches
    )

    task_t24_load_initial_match_data = PythonOperator(
        task_id="t24_load_initial_match_data",
        python_callable=t24_load_initial_match_data
    )

    task_t24_load_finish_match_data = PythonOperator(
        task_id="t24_load_final_match_data",
        python_callable=t24_load_final_match_data
    )
