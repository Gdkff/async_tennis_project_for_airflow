from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dags_modules.test_process import insert_match


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
    description='Test DAG',
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    load_data = PythonOperator(
        task_id="load_atp_matches",
        python_callable=insert_match
    )
