from sqlalchemy import func
from sqlalchemy import text

from flask import Response
from flask_admin import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow import settings
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun, DagBag
from airflow.utils.state import State

from airflow_exporter.xcom_config import load_xcom_config

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily

from contextlib import contextmanager

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
        TaskInstance.state, TaskInstance.hostname, func.count(TaskInstance.dag_id).label('value')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state, TaskInstance.hostname).subquery()

    return Session.query(
        task_status_query.c.dag_id, task_status_query.c.task_id, task_status_query.c.hostname,
        task_status_query.c.state, task_status_query.c.value, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).order_by(task_status_query.c.dag_id).all()


def get_dag_duration_info():
    '''get duration of currently running DagRuns
    :return dag_info
    '''
    driver = Session.bind.driver # pylint: disable=no-member
    durations = {
        'pysqlite': func.julianday(func.current_timestamp() - func.julianday(DagRun.start_date)) * 86400.0,
        'pymysql':  func.timestampdiff(text('second'), DagRun.start_date, func.now()),
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
    if settings.RBAC:
        from airflow.www_rbac.views import dagbag
    else:
        from airflow.www.views import dagbag

    dag = dagbag.get_dag(dag_id)

    if dag is None:
        return [], []
    
    labels = dag.params.get('labels')

    if labels is None:
        return [], []
    
    return list(labels.keys()), list(labels.values())


def get_xcom_params(task_id):
    """XCom parameters for matching task_id's for the latest run of a DAG."""
    with session_scope(Session) as session:
        max_execution_dt_query = (
            session.query(
                DagRun.dag_id,
                func.max(DagRun.execution_date).label("max_execution_dt"),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )

        query = session.query(XCom.dag_id, XCom.task_id, XCom.value).join(
            max_execution_dt_query,
            and_(
                (XCom.dag_id == max_execution_dt_query.c.dag_id),
                (
                    XCom.execution_date
                    == max_execution_dt_query.c.max_execution_dt
                ),
            ),
        )
        if task_id == "all":
            return query.all()
        else:
            return query.filter(XCom.task_id == task_id).all()


def extract_xcom_parameter(value):
    """Deserializes value stored in xcom table."""
    enable_pickling = conf.getboolean("core", "enable_xcom_pickling")
    if enable_pickling:
        value = pickle.loads(value)
        try:
            value = json.loads(value)
            return value
        except Exception:
            return {}
    else:
        try:
            return json.loads(value.decode("UTF-8"))
        except ValueError:
            log = LoggingMixin().log
            log.error(
                "Could not deserialize the XCOM value from JSON. "
                "If you are using pickles instead of JSON "
                "for XCOM, then you need to enable pickle "
                "support for XCOM in your airflow config."
            )
            return {}

class MetricsCollector(object):
    '''collection of metrics for prometheus'''

    def describe(self):
        return []

    def collect(self):
        '''collect metrics'''

		# Xcom metrics

        xcom_params = GaugeMetricFamily(
            "airflow_xcom_parameter",
            "Airflow Xcom Parameter",
            labels=["dag_id", "task_id"],
        )

        xcom_config = load_xcom_config()
        for tasks in xcom_config.get("xcom_params", []):
            for param in get_xcom_params(tasks["task_id"]):
                xcom_value = extract_xcom_parameter(param.value)
				
                xcom_params.add_metric(
                [param.dag_id, param.task_id], xcom_value])

        yield xcom_params

        # Task metrics
        # Each *MetricFamily generates two lines of comments in /metrics, try to minimize noise 
        # by creating new group for each dag
        task_info = get_task_state_info()
        for dag_id, tasks in itertools.groupby(task_info, lambda x: x.dag_id):
            k, v = get_dag_labels(dag_id)

            t_state = GaugeMetricFamily(
                'airflow_task_status',
                'Shows the number of task starts with this status',
                labels=['dag_id', 'task_id', 'owner', 'status', 'hostname'] + k
            )
            for task in tasks:
                t_state.add_metric([task.dag_id, task.task_id, task.owners, task.state or 'none', task.hostname or 'none'] + v, task.value)
            
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

            dag_duration.add_metric([dag.dag_id] + v, dag.duration)

            yield dag_duration


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
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = www_views
    flask_blueprints = []
    menu_links = []
    appbuilder_views = www_rbac_views
    appbuilder_menu_items = []
