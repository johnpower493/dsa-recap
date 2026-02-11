"""
Airflow Fundamentals - Practice Exercises

Goal: design a simple ETL DAG with retries and dependencies.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def extract():
    # TODO: add extraction logic
    print("extract step")


def transform():
    # TODO: add transform logic
    print("transform step")


def load():
    # TODO: add load logic
    print("load step")


default_args = {
    # TODO: set owner
    # TODO: set retries to 2
    # TODO: set retry_delay to 5 minutes
}


with DAG(
    dag_id="etl_practice_exercise",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["practice", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    end = EmptyOperator(task_id="end")

    # TODO: define task order: start >> extract >> transform >> load >> end
