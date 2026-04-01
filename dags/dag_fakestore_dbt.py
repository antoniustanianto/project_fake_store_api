from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='fakestore_dbt_pipeline',
    default_args=default_args,
    description='Run dbt models for Fake Store API',
    schedule='@daily',                          
    start_date=datetime(2026, 3, 30),
    catchup=False,
    tags=['dbt', 'fakestore', 'bigquery'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'cd /opt/airflow/dbt/project_fake_store_api && '
            'dbt run --profiles-dir /opt/airflow/dbt'
        ),
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            'cd /opt/airflow/dbt/project_fake_store_api && '
            'dbt test --profiles-dir /opt/airflow/dbt'
        ),
    )

    dbt_run >> dbt_test