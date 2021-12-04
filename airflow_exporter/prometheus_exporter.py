from typing import List, Tuple, Optional, Generator, NamedTuple, Dict

from dataclasses import dataclass
import itertools

from sqlalchemy import func, text, and_

from flask import Response, current_app
from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import State

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.samples import Sample

import itertools

@dataclass
class DagStatusInfo:
    dag_id: str
    status: str
    cnt: int
    owner: str

def get_dag_status_info() -> List[DagStatusInfo]:
    '''get dag info
    :return dag_info
    '''
    assert(Session is not None)

    dag_status_query = Session.query( # pylint: disable=no-member
        DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('cnt')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    sql_res = (
        Session.query( # pylint: disable=no-member
            dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.cnt,
            DagModel.owners
        )
        .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
        .join(SerializedDagModel, SerializedDagModel.dag_id == dag_status_query.c.dag_id)
        .all()
    )

    res = [
        DagStatusInfo(
            dag_id = i.dag_id,
            status = i.state,
            cnt = i.cnt,
            owner = i.owners
        )
        for i in sql_res
    ]

    return res


def get_last_dagrun_info() -> List[DagStatusInfo]:
    '''get last_dagrun info
    :return last_dagrun_info
    '''
    assert(Session is not None)

    last_run_date_by_dag_id = (
        Session.query(
            DagRun.dag_id,
            func.max(DagRun.execution_date).label('execution_date')
        )
        .select_from(DagRun)
        .group_by(DagRun.dag_id)
        .subquery()
    )

    sql_res = (
        Session.query(
            DagRun.dag_id,
            DagRun.state,
            DagModel.owners,
        )
        .select_from(last_run_date_by_dag_id)
        .join(
            DagRun,
            and_(
                DagRun.dag_id == last_run_date_by_dag_id.c.dag_id,
                DagRun.execution_date == last_run_date_by_dag_id.c.execution_date,
            ),
        )
        .join(DagModel, DagModel.dag_id == DagRun.dag_id)
        .all()
    )

    res = [
        DagStatusInfo(
            dag_id = i.dag_id,
            status = i.state,
            cnt = 1,
            owner = i.owners
        )
        for i in sql_res
    ]

    return res


@dataclass
class TaskStatusInfo:
    dag_id: str
    task_id: str
    status: str
    cnt: int
    owner: str

def get_task_status_info() -> List[TaskStatusInfo]:
    '''get task info
    :return task_info
    '''
    assert(Session is not None)

    task_status_query = Session.query( # pylint: disable=no-member
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('cnt')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    sql_res = (
        Session.query( # pylint: disable=no-member
            task_status_query.c.dag_id, task_status_query.c.task_id,
            task_status_query.c.state, task_status_query.c.cnt, DagModel.owners
        )
        .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
        .join(SerializedDagModel, SerializedDagModel.dag_id == task_status_query.c.dag_id)
        .order_by(task_status_query.c.dag_id)
        .all()
    )

    res = [
        TaskStatusInfo(
            dag_id = i.dag_id,
            task_id = i.task_id,
            status = i.state or 'none',
            cnt = i.cnt,
            owner = i.owners
        )
        for i in sql_res
    ]

    return res

@dataclass
class DagDurationInfo:
    dag_id: str
    duration: float

def get_dag_duration_info() -> List[DagDurationInfo]:
    '''get duration of currently running DagRuns
    :return dag_info
    '''
    assert(Session is not None)

    driver = Session.bind.driver # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'mysqldb':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'mysqlconnector':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'pyodbc': func.sum(func.datediff(text('second'), DagRun.start_date, func.now())),
        'default':  func.now() - DagRun.start_date
    }
    duration = durations.get(driver, durations['default'])

    sql_res = (
        Session.query( # pylint: disable=no-member
            DagRun.dag_id,
            func.max(duration).label('duration')
        )
        .group_by(DagRun.dag_id)
        .filter(DagRun.state == State.RUNNING)
        .join(SerializedDagModel, SerializedDagModel.dag_id == DagRun.dag_id)
        .all()
    )

    res = []

    for i in sql_res:
        if i.duration is not None:
            if driver in ('mysqldb', 'mysqlconnector', 'pysqlite'):
                dag_duration = i.duration
            else:
                dag_duration = i.duration.seconds

            res.append(DagDurationInfo(
                dag_id = i.dag_id,
                duration = dag_duration
            ))

    return res        


def get_dag_labels(dag_id: str) -> Dict[str, str]:
    # reuse airflow webserver dagbag
    dag = current_app.dag_bag.get_dag(dag_id)

    if dag is None:
        return dict()

    labels = dag.params.get('labels', {})

    if hasattr(labels, 'value'):
        # Airflow version 2.2.*
        labels = {k:v for k,v in labels.value.items() if not k.startswith('__')}
    else:
        # Airflow version 2.0.*, 2.1.*
        labels = labels.get('__var', {})

    return labels


def _add_gauge_metric(metric, labels, value):
    metric.samples.append(Sample(
        metric.name, labels,
        value, 
        None
    ))


class MetricsCollector(object):
    '''collection of metrics for prometheus'''

    def describe(self):
        return []

    def collect(self) -> Generator[Metric, None, None]:
        '''collect metrics'''

        # Dag Metrics and collect all labels
        dag_info = get_dag_status_info()

        dag_status_metric = GaugeMetricFamily(
            'airflow_dag_status',
            'Shows the number of dag starts with this status',
            labels=['dag_id', 'owner', 'status']
        )

        for dag in dag_info:
            labels = get_dag_labels(dag.dag_id)

            _add_gauge_metric(
                dag_status_metric,
                {
                    'dag_id': dag.dag_id,
                    'owner': dag.owner,
                    'status': dag.status,
                    **labels
                },
                dag.cnt, 
            )
        
        yield dag_status_metric

        # Last DagRun Metrics
        last_dagrun_info = get_last_dagrun_info()

        dag_last_status_metric = GaugeMetricFamily(
            'airflow_dag_last_status',
            'Shows the status of last dagrun',
            labels=['dag_id', 'owner', 'status']
        )

        for dag in last_dagrun_info:
            labels = get_dag_labels(dag.dag_id)

            for status in State.dag_states:
                _add_gauge_metric(
                    dag_last_status_metric,
                    {
                        'dag_id': dag.dag_id,
                        'owner': dag.owner,
                        'status': status,
                        **labels
                    },
                    int(dag.status == status)
                )

        yield dag_last_status_metric

        # DagRun metrics
        dag_duration_metric = GaugeMetricFamily(
            'airflow_dag_run_duration',
            'Maximum duration of currently running dag_runs for each DAG in seconds',
            labels=['dag_id']
        )
        for dag_duration in get_dag_duration_info():
            labels = get_dag_labels(dag_duration.dag_id)

            _add_gauge_metric(
                dag_duration_metric,
                {
                    'dag_id': dag_duration.dag_id,
                    **labels
                },
                dag_duration.duration
            )

        yield dag_duration_metric

        # Task metrics
        task_status_metric = GaugeMetricFamily(
            'airflow_task_status',
            'Shows the number of task starts with this status',
            labels=['dag_id', 'task_id', 'owner', 'status']
        )

        for dag_id, tasks in itertools.groupby(get_task_status_info(), lambda x: x.dag_id):
            labels = get_dag_labels(dag_id)

            for task in tasks:
                _add_gauge_metric(
                    task_status_metric,
                    {
                        'dag_id': task.dag_id,
                        'task_id': task.task_id,
                        'owner': task.owner,
                        'status': task.status,
                        **labels
                    },
                    task.cnt
                )

        yield task_status_metric


REGISTRY.register(MetricsCollector())

class RBACMetrics(FABBaseView):
    route_base = "/admin/metrics/"
    @FABexpose('/')
    def list(self):
        return Response(generate_latest(), mimetype='text')


# Metrics View for Flask app builder used in airflow with rbac enabled
RBACmetricsView = {
    "view": RBACMetrics(),
    "name": "Metrics",
    "category": "Admin"
}


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    appbuilder_views = [RBACmetricsView]
