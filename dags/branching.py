import airflow

from airflow.models import DAG
from airflow.operators import BashOperator, PythonOperator, DummyOperator, BranchPythonOperator

from datetime import timedelta, datetime

args = {
    "owner": "godatadriven",
    "start_date": airflow.utils.dates.days_ago(10)
}

dag = DAG(
    dag_id="branching_exercise",
    default_args=args,
    description="DAG for using branching",
    schedule_interval=timedelta(hours=2, minutes=30)
)

def _get_weekday(execution_date: datetime, **context):
    return execution_date.strftime('%a')


def _print_weekday(execution_date: datetime, **context):
    print(execution_date.strftime('%a'))


with dag:
    print_weekday = PythonOperator(
        task_id='print_weekday',
        python_callable=_print_weekday,
        provide_context=True,
    )
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=_get_weekday,
        provide_context=True,
    )

    users = ['bob', 'alice', 'joe']

    branches = [DummyOperator(taskid='email_' + user) for user in users]

    end = BashOperator(
        task_id='end',
        bash_command='echo "That\'s it folks!',
        trigger_rule='none_failed'
    )

    print_weekday >> branches >> end
