from datetime import datetime, timedelta
from airflow import DAG
try:
    from airflow.operators.dummy_operator import DummyOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator as DummyOperator

try:
    from airflow.operators.bash_operator import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 1),
    'provide_context': True,
    'retries': 100,
    'retry_delay': timedelta(seconds=30),
    'max_active_runs': 1,
}


dag = DAG(
    'slow_dag',
    schedule_interval=timedelta(hours=5),
    default_args=default_args,
    catchup=False,
    params={
        'labels': {
            'kind': 'slow'
        }
    }
)

dummy1 = DummyOperator(
    task_id='dummy_task_1',
    dag=dag
)

dummy2 = BashOperator(
    task_id='dummy_task_2',
    dag=dag,
    bash_command='sleep 60'
)

dummy1 >> dummy2
