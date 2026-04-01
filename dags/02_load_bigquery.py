# dags/02_load_bigquery.py

import io
import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery

log = logging.getLogger(__name__)

RAW_DATA_PATH = "/opt/airflow/data/raw"
GCP_CONN_ID   = "google_cloud_default"
BQ_PROJECT    = "de-crypto-project-491302"
BQ_DATASET    = "fakestore_raw"

SCHEMAS = {
    "products": [
        {"name": "id",          "type": "INTEGER"},
        {"name": "title",       "type": "STRING"},
        {"name": "price",       "type": "FLOAT"},
        {"name": "description", "type": "STRING"},
        {"name": "category",    "type": "STRING"},
        {"name": "image",       "type": "STRING"},
        {"name": "rating", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "rate",  "type": "FLOAT"},
            {"name": "count", "type": "INTEGER"},
        ]},
    ],
    "users": [
        {"name": "id",       "type": "INTEGER"},
        {"name": "email",    "type": "STRING"},
        {"name": "username", "type": "STRING"},
        {"name": "password", "type": "STRING"},
        {"name": "phone",    "type": "STRING"},
        {"name": "name", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "firstname", "type": "STRING"},
            {"name": "lastname",  "type": "STRING"},
        ]},
        {"name": "address", "type": "RECORD", "mode": "NULLABLE", "fields": [
            {"name": "city",    "type": "STRING"},
            {"name": "street",  "type": "STRING"},
            {"name": "number",  "type": "INTEGER"},
            {"name": "zipcode", "type": "STRING"},
            {"name": "geolocation", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "lat",  "type": "STRING"},
                {"name": "long", "type": "STRING"},
            ]},
        ]},
    ],
    "carts": [
        {"name": "id",     "type": "INTEGER"},
        {"name": "userId", "type": "INTEGER"},
        {"name": "date",   "type": "STRING"},
        {"name": "products", "type": "RECORD", "mode": "REPEATED", "fields": [
            {"name": "productId", "type": "INTEGER"},
            {"name": "quantity",  "type": "INTEGER"},
        ]},
    ],
}


def build_schema_field(field_def: dict) -> bigquery.SchemaField:
    nested_fields = [
        build_schema_field(f)
        for f in field_def.get("fields", [])
    ]
    return bigquery.SchemaField(
        name=field_def["name"],
        field_type=field_def["type"],
        mode=field_def.get("mode", "NULLABLE"),
        fields=nested_fields,
    )


def clean_record(record: dict, schema: list) -> dict:
    schema_fields = {s["name"] for s in schema}
    cleaned = {}

    for key, value in record.items():
        if key not in schema_fields:
            log.warning(f"Dropping unknown field: '{key}'")
            continue

        field_def = next((s for s in schema if s["name"] == key), None)

        if field_def and field_def["type"] == "RECORD":
            nested_schema = field_def.get("fields", [])
            if isinstance(value, list):
                cleaned[key] = [clean_record(v, nested_schema) for v in value]
            elif isinstance(value, dict):
                cleaned[key] = clean_record(value, nested_schema)
            else:
                cleaned[key] = value
        else:
            cleaned[key] = value

    return cleaned


def load_to_bigquery(table_name: str, **context) -> dict:
    execution_date = context["ds"]

    file_path = os.path.join(RAW_DATA_PATH, execution_date, f"{table_name}.json")
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File tidak ditemukan: {file_path}\n"
            f"Jalankan DAG 01_extract_fakestore dulu untuk tanggal {execution_date}"
        )

    log.info(f"Reading: {file_path}")
    with open(file_path, "r") as f:
        data = json.load(f)
    log.info(f"Records read: {len(data)}")

    schema_def = SCHEMAS[table_name]
    data = [clean_record(record, schema_def) for record in data]

    for record in data:
        record["_extracted_at"] = execution_date
        record["_source"]       = "fakestoreapi.com"
        record["_dag_run_id"]   = context["run_id"]

    full_schema = schema_def + [
        {"name": "_extracted_at", "type": "STRING"},
        {"name": "_source",       "type": "STRING"},
        {"name": "_dag_run_id",   "type": "STRING"},
    ]

    hook   = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    client = hook.get_client(project_id=BQ_PROJECT)

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    log.info(f"Loading to: {table_id}")

    job_config = bigquery.LoadJobConfig(
        schema=[build_schema_field(s) for s in full_schema],
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    ndjson = "\n".join(json.dumps(record) for record in data)
    job    = client.load_table_from_file(io.StringIO(ndjson), table_id, job_config=job_config)
    job.result()

    table = client.get_table(table_id)
    log.info(f"Done! {table.num_rows} rows in {table_id}")

    return {
        "table":          table_id,
        "rows_loaded":    table.num_rows,
        "execution_date": execution_date,
    }


def load_products(**context): return load_to_bigquery("products", **context)
def load_users(**context):    return load_to_bigquery("users",    **context)
def load_carts(**context):    return load_to_bigquery("carts",    **context)


default_args = {
    "owner":            "de-team",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="02_load_bigquery",
    description="Load raw JSON ke BigQuery fakestore_raw",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["load", "bigquery", "raw"],
) as dag:

    task_load_products = PythonOperator(
        task_id="load_products",
        python_callable=load_products,
    )

    task_load_users = PythonOperator(
        task_id="load_users",
        python_callable=load_users,
    )

    task_load_carts = PythonOperator(
        task_id="load_carts",
        python_callable=load_carts,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_pipeline",
        trigger_dag_id="fakestore_dbt_pipeline",
        wait_for_completion=True,
        poke_interval=30,
    )

    [task_load_products, task_load_users, task_load_carts] >> trigger_dbt