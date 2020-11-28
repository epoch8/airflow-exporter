import typing
from typing import List, Tuple, Optional, Generator, NamedTuple, Dict

from dataclasses import dataclass
from contextlib import contextmanager
import itertools

from sqlalchemy import func
from sqlalchemy import text

from flask import Response
from flask_admin import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow import settings
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun, DagBag
from airflow.utils.state import State

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.samples import Sample


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
    dag_status_query = Session.query( # pylint: disable=no-member
        DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('cnt')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    sql_res = Session.query( # pylint: disable=no-member
        dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.cnt,
        DagModel.owners
    ).join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id).all()

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
    task_status_query = Session.query( # pylint: disable=no-member
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('cnt')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    sql_res = Session.query( # pylint: disable=no-member
        task_status_query.c.dag_id, task_status_query.c.task_id,
        task_status_query.c.state, task_status_query.c.cnt, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).order_by(task_status_query.c.dag_id).all()

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
    driver = Session.bind.driver # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'mysqldb':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
        'pyodbc': func.sum(func.datediff(text('second'), DagRun.start_date, func.now())),
        'default':  func.now() - DagRun.start_date
    }
    duration = durations.get(driver, durations['default'])

    sql_res = Session.query( # pylint: disable=no-member
        DagRun.dag_id,
        func.max(duration).label('duration')
    ).group_by(
        DagRun.dag_id
    ).filter(
        DagRun.state == State.RUNNING
    ).all()

    res = []

    for i in sql_res:
        if driver == 'mysqldb' or driver == 'pysqlite':
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
    if settings.RBAC:
        from airflow.www_rbac.views import dagbag
    else:
        from airflow.www.views import dagbag

    dag = dagbag.get_dag(dag_id)

    if dag is None:
        return dict()

    labels = dag.params.get('labels')

    if labels is None:
        return dict()

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
        # Each *MetricFamily generates two lines of comments in /metrics, try to minimize noise
        # by creating new group for each dag
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

if settings.RBAC:
    from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose
    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"
        @FABexpose('/')
        def list(self):
            return Response(generate_latest(), mimetype='text')


    # Metrics View for Flask app builder used in airflow with rbac enabled
    RBACmetricsView = {
        "view": RBACMetrics(),
        "name": "metrics",
        "category": "Admin"
    }

    www_views = []
    www_rbac_views = [RBACmetricsView]

else:
    class Metrics(BaseView):
        @expose('/')
        def index(self):
            return Response(generate_latest(), mimetype='text/plain')

    www_views = [Metrics(category="Admin", name="Metrics")]
    www_rbac_views = []


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    operators = [] # type: ignore
    hooks = [] # type: ignore
    executors = [] # type: ignore
    macros = [] # type: ignore
    admin_views = www_views
    flask_blueprints = [] # type: ignore
    menu_links = [] # type: ignore
    appbuilder_views = www_rbac_views
    appbuilder_menu_items = [] # type: ignore
