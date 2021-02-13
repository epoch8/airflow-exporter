from sqlalchemy import func
from sqlalchemy import text

from flask import Response, current_app
from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose

from airflow.plugins_manager import AirflowPlugin
from airflow import settings
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun
from airflow.utils.state import State

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily

import itertools


def get_dag_state_info():
    '''get dag info
    :return dag_info
    '''
    dag_status_query = Session.query(
        DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('count')
    ).group_by(DagRun.dag_id, DagRun.state).subquery()

    return Session.query(
        dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.count,
        DagModel.owners
    ).join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id).all()


def get_task_state_info():
    '''get task info
    :return task_info
    '''
    task_status_query = Session.query(
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('value')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()

    return Session.query(
        task_status_query.c.dag_id, task_status_query.c.task_id,
        task_status_query.c.state, task_status_query.c.value, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).order_by(task_status_query.c.dag_id).all()


def get_dag_duration_info():
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

    return Session.query(
        DagRun.dag_id,
        func.max(duration).label('duration')
    ).group_by(
        DagRun.dag_id
    ).filter(
        DagRun.state == State.RUNNING
    ).all()


def get_dag_labels(dag_id):
    # reuse airflow webserver dagbag
    dag = current_app.dag_bag.get_dag(dag_id)

    if dag is None:
        return [], []

    labels = dag.params.get('labels')

    if labels is None:
        return [], []

    labels = {k:v for k,v in labels.items() if not k.startswith('__')}
    return list(labels.keys()), list(labels.values())


class MetricsCollector(object):
    '''collection of metrics for prometheus'''

    def describe(self):
        return []

    def collect(self):
        '''collect metrics'''

        # Task metrics
        # Each *MetricFamily generates two lines of comments in /metrics, try to minimize noise
        # by creating new group for each dag
        task_info = get_task_state_info()
        for dag_id, tasks in itertools.groupby(task_info, lambda x: x.dag_id):
            k, v = get_dag_labels(dag_id)

            t_state = GaugeMetricFamily(
                'airflow_task_status',
                'Shows the number of task starts with this status',
                labels=['dag_id', 'task_id', 'owner', 'status'] + k
            )
            for task in tasks:
                t_state.add_metric([task.dag_id, task.task_id, task.owners, task.state or 'none'] + v, task.value)

            yield t_state

        # Dag Metrics
        dag_info = get_dag_state_info()
        for dag in dag_info:
            k, v = get_dag_labels(dag.dag_id)

            d_state = GaugeMetricFamily(
                'airflow_dag_status',
                'Shows the number of dag starts with this status',
                labels=['dag_id', 'owner', 'status'] + k
            )
            d_state.add_metric([dag.dag_id, dag.owners, dag.state] + v, dag.count)
            yield d_state

        # DagRun metrics
        driver = Session.bind.driver # pylint: disable=no-member
        for dag in get_dag_duration_info():
            k, v = get_dag_labels(dag.dag_id)

            dag_duration = GaugeMetricFamily(
                'airflow_dag_run_duration',
                'Maximum duration of currently running dag_runs for each DAG in seconds',
                labels=['dag_id'] + k
            )
            if driver == 'mysqldb' or driver == 'pysqlite':
                dag_duration.add_metric([dag.dag_id] + v, dag.duration)
            else:
                dag_duration.add_metric([dag.dag_id] + v, dag.duration.seconds)
            yield dag_duration


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

www_views = []
www_rbac_views = [RBACmetricsView]


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = www_views
    flask_blueprints = []
    menu_links = []
    appbuilder_views = www_rbac_views
    appbuilder_menu_items = []
