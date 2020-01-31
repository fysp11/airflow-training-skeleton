import airflow

from airflow.models import DAG
from airflow.operators import BashOperator, PythonOperator, DummyOperator

from datetime import timedelta

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id="context_exercise",
    default_args=args,
    description="DAG for using context",
    schedule_interval=timedelta(hours=2, minutes=30)
)

def print_exec_date(execution_date, templates_dict, **context):
    print(f"{templates_dict.get('name')} says {execution_date}!")


with dag:
    print_exec_date = PythonOperator(
        task_id='print_exec_date',
        python_callable=print_exec_date,
        provide_context=True,
        templates_dict={'name': 'Felipe'}
    )
    wait_01 = BashOperator(task_id='wait_01', bash_command="sleep 1")
    wait_05 = BashOperator(task_id='wait_05', bash_command="sleep 5")
    wait_10 = BashOperator(task_id='wait_10', bash_command="sleep 10")
    end = DummyOperator(task_id='end')


print_exec_date >> [wait_01, wait_05, wait_10] >> end
