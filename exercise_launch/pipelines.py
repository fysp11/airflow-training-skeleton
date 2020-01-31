import airflow

from airflow.models import DAG
from airflow.operators import DummyOperator

args = {}

dag = DAG(
    dag_id="test_scheduled_interval",
    default_args=args,
    description="DAG just scheduling the next step",
    schedule_interval="0 0/1 * 1/1 * ? *"
)

with dag:
    t1 = DummyOperator(task_id='t1')
    t2 = DummyOperator(task_id='t2')
    t3 = DummyOperator(task_id='t3')
    t4 = DummyOperator(task_id='t4')
    t5 = DummyOperator(task_id='t5')


t1 >> t2 >> [t3, t4] >> t5
