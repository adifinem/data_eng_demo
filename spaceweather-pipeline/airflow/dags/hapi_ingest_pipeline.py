import os, time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {"owner": "jess", "retries": 0}
EVENT_LOG = os.getenv("PIPELINE_EVENT_LOG")
EVENT_SOURCE = os.getenv("PIPELINE_EVENT_SOURCE", "airflow")


def log_event(message: str, status: str | None = None) -> None:
    if not EVENT_LOG:
        return
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    tag = f"{status} " if status else ""
    try:
        with open(EVENT_LOG, "a", encoding="utf-8") as fh:
            fh.write(f"{ts} {EVENT_SOURCE} {tag}{message}\n")
    except Exception:
        pass


def task_callback(event: str):
    def _cb(context):
        dag_run = context.get("dag_run")
        run_id = (dag_run.run_id if dag_run else context.get("run_id")) or "unknown"
        task_id = context.get("task_instance").task_id
        log_event(f"hapi task {task_id} {event} run_id={run_id}", status=event)

    return _cb


def dag_callback(event: str):
    def _cb(context):
        dag_run = context.get("dag_run")
        run_id = (dag_run.run_id if dag_run else context.get("run_id")) or "unknown"
        dag_id = context.get("dag").dag_id
        log_event(f"hapi dag {dag_id} {event} run_id={run_id}", status=event)

    return _cb


with DAG(
    "hapi_ingest_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(minutes=5),
    catchup=False,
    default_args=default_args,
    tags=["hapi", "bronze"],
) as dag:
    dag.on_success_callback = dag_callback("success")
    dag.on_failure_callback = dag_callback("failure")

    harvest = SimpleHttpOperator(
        task_id="harvest_hapi",
        http_conn_id="ingestor_http",
        endpoint="harvest",
        method="POST",
        on_execute_callback=task_callback("start"),
        on_success_callback=task_callback("success"),
        on_failure_callback=task_callback("failure"),
    )

    run_dbt = SimpleHttpOperator(
        task_id="run_dbt",
        http_conn_id="dbt_runner_http",
        endpoint="run",
        method="GET",
        on_execute_callback=task_callback("start"),
        on_success_callback=task_callback("success"),
        on_failure_callback=task_callback("failure"),
    )

    harvest >> run_dbt
