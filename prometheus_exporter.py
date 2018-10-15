from sqlalchemy import func

from flask_admin import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.www.app import csrf
from airflow.models import DagStat, TaskInstance, DagModel, DagRun
from airflow.utils.state import State

# Importing base classes that we need to derive
from prometheus_client import core, generate_latest
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from contextlib import contextmanager


@contextmanager
def session_scope(session):
    """
    Provide a transactional scope around a series of operations.
    """
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def get_dag_state_info():
    '''get dag info
    :return dag_info
    '''
    with session_scope(Session) as session:
        dag_status_query = session.query(
            DagStat.dag_id, DagStat.state, DagStat.count
        ).group_by(DagStat.dag_id, DagStat.state).subquery()
        return session.query(
            DagStat.dag_id, DagStat.state, DagStat.count,
            DagModel.owners
        ).join(DagModel, DagModel.dag_id == DagStat.dag_id).all()


def get_task_state_info():
    '''get task info
    :return task_info
    '''
    with session_scope(Session) as session:
        task_status_query = session.query(
            TaskInstance.dag_id, TaskInstance.task_id,
            TaskInstance.state, func.count(TaskInstance.dag_id).label('value')
        ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()
        return session.query(
            task_status_query.c.dag_id, task_status_query.c.task_id,
            task_status_query.c.state, task_status_query.c.value, DagModel.owners
        ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).all()


def get_dag_duration_info():
    '''get duration of currently running DagRuns
    :return dag_info
    '''
    duration = func.sum(func.now() - DagRun.start_date)

    with session_scope(Session) as session:
        return session.query(
            DagRun.dag_id,
            DagRun.run_id,
            duration.label('duration')
        ).group_by(
            DagRun.dag_id,
            DagRun.run_id
        ).filter(
            DagRun.state == State.RUNNING
        ).all()


class MetricsCollector(object):
    '''collection of metrics for prometheus'''
    def collect(self):
        '''collect metrics'''

        # Task metrics
        task_info = get_task_state_info()
        t_state = GaugeMetricFamily(
            'airflow_task_status',
            'Shows the number of task starts with this status',
            labels=['dag_id', 'task_id', 'owner', 'status']
        )
        for task in task_info:
            t_state.add_metric([task.dag_id, task.task_id, task.owners, task.state], task.value)
        yield t_state

        # Dag Metrics
        dag_info = get_dag_state_info()
        d_state = GaugeMetricFamily(
            'airflow_dag_status',
            'Shows the number of dag starts with this status',
            labels=['dag_id', 'owner', 'status']
        )
        for dag in dag_info:
            d_state.add_metric([dag.dag_id, dag.owners, dag.state], dag.count)
        yield d_state

        # DagRun metrics
        dag_duration = GaugeMetricFamily(
            'airflow_dag_run_duration',
            'Duration of currently running dag_runs in seconds',
            labels=['dag_id', 'run_id']
        )
        for dag in get_dag_duration_info():
            dag_duration.add_metric([dag.dag_id, dag.run_id], dag.duration.seconds)
        yield dag_duration



REGISTRY.register(MetricsCollector())


class Metrics(BaseView):
    @expose('/')
    def index(self):
        from flask import Response
        return Response(generate_latest(), mimetype='text')


ADMIN_VIEW = Metrics(category="Prometheus exporter", name="metrics")


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = [ADMIN_VIEW]
    flask_blueprints = []
    menu_links = []
