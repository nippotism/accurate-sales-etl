from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


from scripts.refresh_token import refresh_token
from scripts.ingest_raw_data import ingest_raw_data
from scripts.load_db import load_data_to_db

with DAG(
    dag_id="sales_etl_pipeline",
    start_date=datetime(2026, 2, 6),
    schedule_interval="@weekly",
    catchup=False,
    max_active_runs=1,
) as dag:
    t1 = PythonOperator(
        task_id="refresh_token",
        python_callable=refresh_token,
    )

    t2 = PythonOperator(
        task_id="extract_transform_data",
        python_callable=ingest_raw_data,
    )

    t3 = PythonOperator(
        task_id="load_data_to_db",
        python_callable=load_data_to_db,
    )


    t1 >> t2 >> t3