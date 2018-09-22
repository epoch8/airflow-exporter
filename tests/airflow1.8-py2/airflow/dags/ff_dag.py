import os
import re

from datetime import datetime
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.operators.sensors import BaseSensorOperator


from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


class OmegaFileSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, filepath, filepattern, *args, **kwargs):
        super(OmegaFileSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.filepattern = filepattern

    def poke(self, context):
        full_path = self.filepath
        file_pattern = re.compile(self.filepattern)

        directory = os.listdir(full_path)

        for files in directory:
            if not re.match(file_pattern, files):
                return False
            else:
                context['task_instance'].xcom_push('file_name', files)
                return True


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

dag = DAG('test_sensing_for_a_file', default_args=default_args)

filepath = '/usr/local/airflow/logs/ft'
filepattern = 'ft'

sensor_task = OmegaFileSensor(
    task_id='file_sensor_task',
    filepath=filepath,
    filepattern=filepattern,
    poke_interval=3,
    dag=dag)


def process_file(**context):
    file_to_process = context['task_instance'].xcom_pull(
        key='file_name', task_ids='file_sensor_task')
    file = open(filepath + file_to_process, 'w')
    file.write('This is a test\n')
    file.write('of processing the file')
    file.close()


proccess_task = PythonOperator(
    task_id='process_the_file', python_callable=process_file, dag=dag)


sensor_task >> proccess_task
