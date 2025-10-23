from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "healthcheck",
    description="Sanity DAG to prove Airflow is alive",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=30),
    catchup=False,
) as dag:
    t = BashOperator(task_id="echo", bash_command="echo 'airflow ok'")


