from sys import argv as CommandLineArguments

from sqlalchemy import func
from sqlalchemy import text

from functools import wraps
from flask import Response
from flask import request, abort
from flask_admin import BaseView, expose

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun
from airflow.utils.state import State

from contextlib import contextmanager

# Importing classes used in ExporterConfiguration
from collections import OrderedDict
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin

from airflow import jobs

# Importing Flask App Builder
from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose

# Importing Prometheus client classes
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.core import GaugeMetricFamily

class MetricsCollector(object):
    ''' Prometheus Exporter '''

    def __init__(self):
        self.enabled = True
        self.log = LoggingMixin().log
        self.name = 'prometheus_exporter'
        self.config = {
            'auth_enabled': False,
            'auth_token': ''
        }
        self.exposed_metrics = [
            'scheduler',
            'config'
        ]
        self.supported_metrics = [
            'variables',
            'config',
            'scheduler',
        ]
        self._load_config()
        return
 
    def _load_config(self):
        if self.name not in configuration.conf:
            return
        params = configuration.conf[self.name]
        if 'disabled' in params:
            if params['disabled'] in ['True', True]:
                self.enabled = False
                self.log.info('%s is disabled')
                return
        if 'auth_enabled' in params:
            if params['auth_enabled'] in ['True', True]:
                self.config['auth_enabled'] = True
                self.log.info('%s bearer token authentication is enabled', self.name)
        if self.config['auth_enabled'] is True:
            if 'auth_token' in params:
                self.config['auth_token'] = str(params['auth_token']).strip()
            else:
                raise Exception(s, 'auth_enabled is True, but auth_token not found')
            if self.config['auth_token'] == '':
                raise Exception(s, 'auth_enabled is True, but auth_token is empty')
        for k in params:
            if k.startswith('expose_'):
                metric = str(k.replace('expose_', ''))
                if params[k] in [True, 'True']:
                    if metric not in self.exposed_metrics:
                        self.exposed_metrics.append(metric)
                else:
                    if metric in self.exposed_metrics:
                        self.exposed_metrics.remove(metric)
        for metric in self.exposed_metrics:
            if metric not in self.supported_metrics:
                raise Exception(s, 'metric %s is unsupported' % (metric))
            self.log.info('%s exposes %s metric', self.name, metric)
        return

    def describe(self):
        return []

    def collect(self):
        '''collect metrics'''

        # Task metrics
        task_info = self.get_task_state_info()
        t_state = GaugeMetricFamily(
            'airflow_task_status',
            'Shows the number of task starts with this status',
            labels=['dag_id', 'task_id', 'owner', 'status']
        )
        for task in task_info:
            t_state.add_metric([task.dag_id, task.task_id, task.owners, task.state or 'none'], task.value)
        yield t_state

        # Dag Metrics
        dag_info = self.get_dag_state_info()
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
        driver = Session.bind.driver
        for dag in self.get_dag_duration_info():
            if driver == 'mysqldb' or driver == 'pysqlite':
                dag_duration.add_metric([dag.dag_id, dag.run_id], dag.duration)
            else:
                dag_duration.add_metric([dag.dag_id, dag.run_id], dag.duration.seconds)
        yield dag_duration

        # Configuration metrics TODO
        if 'config' in self.exposed_metrics:
            pass

        # Variables metrics TODO
        if 'variables' in self.exposed_metrics:
            pass

        # Scheduler metrics
        if 'scheduler' in self.exposed_metrics:
            scheduler_up = GaugeMetricFamily(
                'airflow_scheduler_up',
                'Returns whether airflow scheduler is up (1) or down (0)',
                labels=[]
            )
            scheduler_status = self.get_scheduler_status()
            scheduler_up.add_metric([], scheduler_status)
            yield scheduler_up

        return


    @contextmanager
    def session_scope(self, session):
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
        return


    def get_dag_state_info(self):
        '''get dag info
        :return dag_info
        '''
        with self.session_scope(Session) as session:
            dag_status_query = session.query(
                DagRun.dag_id, DagRun.state, func.count(DagRun.state).label('count')
            ).group_by(DagRun.dag_id, DagRun.state).subquery()
            return session.query(
                dag_status_query.c.dag_id, dag_status_query.c.state, dag_status_query.c.count,
                DagModel.owners
            ).join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id).all()
        return


    def get_task_state_info(self):
        '''get task info
        :return task_info
        '''
        with self.session_scope(Session) as session:
            task_status_query = session.query(
                TaskInstance.dag_id, TaskInstance.task_id,
                TaskInstance.state, func.count(TaskInstance.dag_id).label('value')
            ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()
            return session.query(
                task_status_query.c.dag_id, task_status_query.c.task_id,
                task_status_query.c.state, task_status_query.c.value, DagModel.owners
            ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).all()
        return


    def get_dag_duration_info(self):
        '''get duration of currently running DagRuns
        :return dag_info
        '''
        driver = Session.bind.driver
        durations = {
            'pysqlite': func.sum(
                (func.julianday(func.current_timestamp()) - func.julianday(DagRun.start_date)) * 86400.0
            ),
            'mysqldb': func.sum(func.timestampdiff(text('second'), DagRun.start_date, func.now())),
            'default': func.sum(func.now() - DagRun.start_date)
        }
        duration = durations.get(driver, durations['default'])

        with self.session_scope(Session) as session:
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
        return

    def get_scheduler_status(self):
        '''get scheduler status
        :return scheduler_status
        '''
        try:
            scheduler_job = jobs.SchedulerJob.most_recent_job()
            if scheduler_job:
                latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
                if scheduler_job.is_alive():
                    return 1
        except:
            pass
        return 0

if 'airflow-webserver' in CommandLineArguments:
    prometheus_exporter = MetricsCollector()
    REGISTRY.register(prometheus_exporter)

def authenticate_scrape(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not prometheus_exporter.enabled:
            abort(503)
        if prometheus_exporter.config['auth_enabled'] == False:
            return f(*args, **kwargs)
        from flask_login import current_user
        if not current_user.is_anonymous and current_user.is_authenticated:
            return f(*args, **kwargs)
        auth_header = None
        auth_headers = ['Authorization', 'X-Auth-Token', 'X-Token', 'x-token', 'access_token']
        for h in auth_headers:
            if h in request.headers:
                auth_header = h
                break
        if not auth_header:
            abort(401)
        data = request.headers[auth_header].encode('ascii','ignore')
        token = str.replace(str(data.strip()), 'Bearer ','').replace('bearer ','')
        if token != prometheus_exporter.config['auth_token']:
            abort(401)
        return f(*args, **kwargs)
    return decorated_function

class Metrics(BaseView):
    @expose('/')
    @authenticate_scrape
    def index(self):
        return Response(generate_latest(), mimetype='text/plain')

class PrometheusMetricsView(FABBaseView):
    route_base = "/admin/metrics/"
    @FABexpose('/')
    def list(self):
        return Response(generate_latest(), mimetype='text')


class AirflowPrometheusPlugins(AirflowPlugin):
    '''plugin for show metrics'''
    name = "airflow_prometheus_plugin"
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = [Metrics(category="Admin", name="Metrics")]
    flask_blueprints = []
    menu_links = []
    appbuilder_views = [{
        "view": PrometheusMetricsView(),
        "name": "metrics",
        "category": "Admin"
    }]
    appbuilder_menu_items = []
