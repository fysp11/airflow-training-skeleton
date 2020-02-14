import json
import pathlib
import posixpath
import airflow
import requests

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from dags.operators.launch_library_operator import LaunchLibraryOperator

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10),
    "provide_context": True
}


def _print_stats(ds, **context):
    with open(f"/tmp/rocket_launches/ds={ds}/launches.json") as f:
        data = json.load(f)
        rockets_launched = [launch["name"] for launch in data["launches"]]
        rockets_str = ""

        if rockets_launched:
            rockets_str = f" ({' & '.join(rockets_launched)})"
            print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")
        else:
            print(f"No rockets found in {f.name}")


with DAG(
        dag_id="download_rocket_launches",
        default_args=args,
        description="DAG downloading rocket launches from Launch Library.",
        schedule_interval="0 0 * * *"):
    download_rocket_launches = LaunchLibraryOperator(
        task_id="download_rocket_launches",
        request_conn_id='launch_rockets_conn',
        endpoint='launch',
        params=dict(startdate='{{ ds }}', enddate='{{ tomorrow_ds }}'),
                #  result_bucket: 'europe-west1-training-airfl-3df4cfa2-bucket',
        result_path='testing_folder',
        result_filename='result.json',
        do_xcom_push=False
    )

    print_stats = PythonOperator(
        task_id="print_stats",
        python_callable=_print_stats,
    )

    download_rocket_launches >> print_stats
