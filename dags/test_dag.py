from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dags_modules.test_process import insert_match


with DAG(
    dag_id="load_tennis_data",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["tennis"],
) as dag:

    load_data = PythonOperator(
        task_id="load_atp_matches",
        python_callable=insert_match
    )
