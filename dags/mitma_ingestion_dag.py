from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.airflow_tasks import run_local_ingestion, run_mitma_ingestion

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="mitma_full_ingestion",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["mitma", "bronze", "local", "web"],
) as dag:

    # --------------------------
    # Tarea 1: ingestión de archivos locales
    # --------------------------
    ingest_local = PythonOperator(
        task_id="ingest_local_files",
        python_callable=run_local_ingestion,
    )

    # --------------------------
    # Tarea 2: ingestión de datos MITMA desde web
    # --------------------------
    ingest_web = PythonOperator(
        task_id="ingest_mitma_2023",
        python_callable=run_mitma_ingestion,
        op_kwargs={"year": 2023},
    )

    # --------------------------
    # Definir dependencia: primero local, luego web
    # --------------------------
    ingest_local >> ingest_web
