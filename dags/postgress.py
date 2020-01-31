import airflow
import random

from airflow.models import DAG
from airflow.operators import BashOperator, PythonOperator, DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

from airflow.hooks.postgres_hook import PostgresHook

from datetime import timedelta, datetime

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id=__name__,
    default_args=args,
    description="DAG for using " + __name__,
    schedule_interval=timedelta(hours=2, minutes=30)
)

query = 'SELECT * FROM land_registry_price_paid_uk LIMIT 10'

with dag:
    pg_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='pg_to_gcs',
        query=query,
        bucket='airflow-postgres-1234'
    )
    pg_to_gcs
