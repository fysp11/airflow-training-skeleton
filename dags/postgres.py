import airflow
import random

from airflow.models import DAG
from airflow.operators import BashOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

from datetime import timedelta, datetime

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id='postgres',
    default_args=args,
    description="DAG for using " + __name__,
    schedule_interval=timedelta(hours=2, minutes=30)
)

query = 'SELECT * FROM land_registry_price_paid_uk LIMIT 10'

with dag:
    pg_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='pg_to_gcs',
        sql=query,
        bucket='airflow-postgres-1234',
        filename='output',
        google_cloud_storage_conn_id='google_cloud_storage_default'
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "That\'s it folks!"',
    )

    pg_to_gcs >> end
