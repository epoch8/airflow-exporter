from airflow.plugins_manager import AirflowPlugin

from flask_admin import BaseView, expose

# Importing base classes that we need to derive
from prometheus_client import core
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_exporter.db.store import get_context
from sqlalchemy import func
import copy
from airflow.www.app import csrf
from airflow.models import DagStat, TaskInstance, DagModel



@get_context()
def get_dag_state_info(connection):
    '''get dag info
    :param connection: session in db
    :return dag_info
    '''
    dag_status_query = connection.query(
        DagStat.dag_id, DagStat.state, DagStat.count
    ).group_by(DagStat.dag_id, DagStat.state).subquery()
    return connection.query(
        DagStat.dag_id, DagStat.state, DagStat.count,
        DagModel.owners
    ).join(DagModel, DagModel.dag_id == DagStat.dag_id).all()


@get_context()
def get_task_state_info(connection):
    '''get task info
    :param connection: session in db
    :return task_info
    '''
    task_status_query = connection.query(
        TaskInstance.dag_id, TaskInstance.task_id,
        TaskInstance.state, func.count(TaskInstance.dag_id).label('value')
    ).group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state).subquery()
    return connection.query(
        task_status_query.c.dag_id, task_status_query.c.task_id,
        task_status_query.c.state, task_status_query.c.value, DagModel.owners
    ).join(DagModel, DagModel.dag_id == task_status_query.c.dag_id).all()



class MetricsCollector(object):
    '''collection of metrics for prometheus'''
    def collect(self):
        '''collect metrics'''
        task_info = get_task_state_info()
        t_state = GaugeMetricFamily(
            'airflow_task_status',
            'Shows the number of task starts with this status',
            labels=['dag_id', 'task_id', 'owner', 'status']
        )
        for task in task_info:
            t_state.add_metric([task.dag_id, task.task_id, task.owners, task.state], task.value)
        yield copy.copy(t_state)

        dag_info = get_dag_state_info()
        d_state = GaugeMetricFamily(
            'airflow_dag_status',
            'Shows the number of dag starts with this status',
            labels=['dag_id', 'owner', 'status']
        )
        for dag in dag_info:
            d_state.add_metric([dag.dag_id, dag.owners, dag.state], dag.count)
        yield d_state


REGISTRY.register(MetricsCollector())


def generate_latest(registry=REGISTRY):
    '''Returns the metrics from the registry in latest text format as a string.'''
    output = []
    for metric in registry.collect():
        output.append(
            '# HELP {0} {1}'.format(
                metric.name,
                metric.documentation.replace('\\', r'\\').replace('\n', r'\n')
            )
        )
        output.append('\n# TYPE {0} {1}\n'.format(metric.name, metric.type))
        for name, labels, value in metric.samples:
            if labels:
                label_text = '{{{0}}}'.format(','.join(
                    [
                        '{0}="{1}"'.format(
                            k,
                            v.replace(
                                '\\', r'\\'
                            ).replace('\n', r'\n').replace('"', r'\"') if v else '')
                        for k, v in sorted(labels.items())
                    ]))
            else:
                label_text = ''
            output.append('{0}{1} {2}\n'.format(
                name, label_text, core._floatToGoString(value)))
    return ''.join(output).encode('utf-8')


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
