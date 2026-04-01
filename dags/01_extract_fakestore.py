# dags/01_extract_fakestore.py

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)

FAKESTORE_BASE_URL = "https://fakestoreapi.com"
RAW_DATA_PATH = "/opt/airflow/data/raw"


def fetch_and_save(endpoint: str, filename: str, **context) -> dict:
    url = f"{FAKESTORE_BASE_URL}{endpoint}"
    execution_date = context["ds"]

    output_dir = os.path.join(RAW_DATA_PATH, execution_date)
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    log.info(f"Fetching data from: {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    data = response.json()
    record_count = len(data) if isinstance(data, list) else 1
    log.info(f"Received {record_count} records from {endpoint}")

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    log.info(f"Saved to: {output_path}")

    return {
        "endpoint": endpoint,
        "record_count": record_count,
        "output_path": output_path,
        "execution_date": execution_date,
    }


def extract_products(**context):
    return fetch_and_save("/products", "products.json", **context)

def extract_users(**context):
    return fetch_and_save("/users", "users.json", **context)

def extract_carts(**context):
    return fetch_and_save("/carts", "carts.json", **context)


default_args = {
    "owner": "de-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="01_extract_fakestore",
    description="Extract raw data from Fake Store API",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["extract", "fakestore", "raw"],
) as dag:

    task_extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_products,
    )

    task_extract_users = PythonOperator(
        task_id="extract_users",
        python_callable=extract_users,
    )

    task_extract_carts = PythonOperator(
        task_id="extract_carts",
        python_callable=extract_carts,
    )

    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load_bigquery",
        trigger_dag_id="02_load_bigquery",
        wait_for_completion=True,
        poke_interval=30,
    )

    task_extract_products >> task_extract_users >> task_extract_carts >> trigger_load