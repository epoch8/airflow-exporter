from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'owner',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'provide_context': True,
    'retries': 100,
    'retry_delay': timedelta(seconds=30),
    'max_active_runs': 1,
    'schedule_interval': timedelta(seconds=5),
}


dag = DAG('dummy_dag', default_args=default_args)

dummy1 = DummyOperator(
    task_id='dummy_task_1',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='dummy_task_2',
    dag=dag
)

dummy1 >> dummy2