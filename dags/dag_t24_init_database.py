from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules.t24_init_database import t24_init_database


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='t24_init_database',
    default_args=default_args,
    description='First initial loading all data from Tennis 24',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_init_database = PythonOperator(
        task_id="t24_load_daily_matches",
        python_callable=t24_init_database
    )
