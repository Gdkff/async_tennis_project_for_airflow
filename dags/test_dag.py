from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # обновлённый параметр!
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

