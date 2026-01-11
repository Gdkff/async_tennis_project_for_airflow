from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules_atp.atp__run_manager import atp_load_rankings


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='atp_load_ranking',
    default_args=default_args,
    description='Load singles and doubles ATP ranking',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_load_daily_matches = PythonOperator(
        task_id="atp_load_rankings",
        python_callable=atp_load_rankings
    )
