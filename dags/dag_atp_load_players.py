from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules_atp.atp__run_manager import load_new_players


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='atp_load_new_players',
    default_args=default_args,
    description='Load new players',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_load_daily_matches = PythonOperator(
        task_id="atp_load_new_players",
        python_callable=load_new_players
    )
