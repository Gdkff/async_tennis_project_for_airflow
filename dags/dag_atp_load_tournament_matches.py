from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules_atp.atp__run_manager import get_tournaments_matches


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='atp_load_tournament_matches',
    default_args=default_args,
    description='Load matches from tournaments',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_t24_load_daily_matches = PythonOperator(
        task_id="atp_load_tournament_matches",
        python_callable=get_tournaments_matches
    )
