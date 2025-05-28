from datetime import datetime, timedelta
from airflow import DAG
from airflow.version import version as airflow_version
from packaging.version import parse as parse_version

if parse_version(airflow_version).major >= 3:
    from airflow.operators.empty import EmptyOperator as DummyOperator
    from airflow.operators.bash import BashOperator
else:
    from airflow.operators.dummy_operator import DummyOperator
    from airflow.operators.bash_operator import BashOperator


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
    dag_id='dummy_dag',
    default_args=default_args,
    catchup=False,
    params={
        'labels': {
            'env': 'test',
            'label1': 'value1',
            'label2': 'value2'
        }
    }
)

dummy1 = DummyOperator(
    task_id='dummy_task_1',
    dag=dag
)

dummy2 = DummyOperator(
    task_id='dummy_task_2',
    dag=dag
)

dummy1 >> dummy2
