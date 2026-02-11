"""
Airflow Fundamentals - Reference Solution
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def extract():
    print("extract step")


def transform():
    print("transform step")


def load():
    print("load step")


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_practice_solution",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["practice", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    end = EmptyOperator(task_id="end")

    start >> extract_task >> transform_task >> load_task >> end
