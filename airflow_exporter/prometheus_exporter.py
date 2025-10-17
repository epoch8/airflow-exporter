from typing import List, Generator, Dict

from dataclasses import dataclass

from sqlalchemy import func
from sqlalchemy import text

from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.models import TaskInstance, DagModel, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.state import State
from airflow.version import version as airflow_version

# Importing base classes that we need to derive
from prometheus_client import generate_latest, REGISTRY
from prometheus_client.registry import Collector
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.samples import Sample

import itertools

from packaging.version import parse as parse_version
import logging

log = logging.getLogger(__name__)
use_fastapi = parse_version(airflow_version).major >= 3

if use_fastapi:
    from airflow.models.dagbag import DagBag

    GLOBAL_DAGBAG = DagBag()
else:
    from flask import current_app


@dataclass
class DagStatusInfo:
    dag_id: str
    status: str
    cnt: int
    paused: str
    owner: str


def get_dag_status_info() -> List[DagStatusInfo]:
    """get dag info
    :return dag_info
    """
    assert Session is not None

    dag_status_query = (
        Session.query(  # type: ignore
            DagRun.dag_id, DagRun.state, func.count(DagRun.state).label("cnt")
        )
        .group_by(DagRun.dag_id, DagRun.state)
        .subquery()
    )

    sql_res = (
        Session.query(  # type: ignore
            dag_status_query.c.dag_id,
            dag_status_query.c.state,
            dag_status_query.c.cnt,
            DagModel.is_paused,
            DagModel.owners,
        )
        .join(DagModel, DagModel.dag_id == dag_status_query.c.dag_id)
        .join(
            SerializedDagModel, SerializedDagModel.dag_id == dag_status_query.c.dag_id
        )
        .all()
    )

    res = [
        DagStatusInfo(dag_id=i.dag_id, status=i.state, cnt=i.cnt, paused=str(i.is_paused).lower(), owner=i.owners)
        for i in sql_res
    ]

    return res


def get_last_dagrun_info() -> List[DagStatusInfo]:
    """get last_dagrun info
    :return last_dagrun_info
    """
    assert Session is not None

    if use_fastapi:
        last_dagrun_query = Session.query(  # type: ignore
            DagRun.dag_id,
            DagRun.state,
            func.row_number()
            .over(partition_by=DagRun.dag_id, order_by=DagRun.logical_date.desc())  # type: ignore
            .label("row_number"),
        ).subquery()
    else:
        last_dagrun_query = Session.query(  # type: ignore
            DagRun.dag_id,
            DagRun.state,
            func.row_number()
            .over(partition_by=DagRun.dag_id, order_by=DagRun.execution_date.desc())
            .label("row_number"),
        ).subquery()

    sql_res = (
        Session.query(  # type: ignore
            last_dagrun_query.c.dag_id,
            last_dagrun_query.c.state,
            last_dagrun_query.c.row_number,
            DagModel.is_paused,
            DagModel.owners,
        )
        .filter(last_dagrun_query.c.row_number == 1)
        .join(DagModel, DagModel.dag_id == last_dagrun_query.c.dag_id)
        .join(
            SerializedDagModel, SerializedDagModel.dag_id == last_dagrun_query.c.dag_id
        )
        .all()
    )

    res = [
        DagStatusInfo(dag_id=i.dag_id, status=i.state, cnt=1, paused="true" if i.is_paused else "false", owner=i.owners)
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
    """get task info
    :return task_info
    """
    assert Session is not None

    task_status_query = (
        Session.query(  # type: ignore
            TaskInstance.dag_id,
            TaskInstance.task_id,
            TaskInstance.state,
            func.count(TaskInstance.dag_id).label("cnt"),
        )
        .group_by(TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.state)
        .subquery()
    )

    sql_res = (
        Session.query(  # type: ignore
            task_status_query.c.dag_id,
            task_status_query.c.task_id,
            task_status_query.c.state,
            task_status_query.c.cnt,
            DagModel.owners,
        )
        .join(DagModel, DagModel.dag_id == task_status_query.c.dag_id)
        .join(
            SerializedDagModel, SerializedDagModel.dag_id == task_status_query.c.dag_id
        )
        .order_by(task_status_query.c.dag_id)
        .all()
    )

    res = [
        TaskStatusInfo(
            dag_id=i.dag_id,
            task_id=i.task_id,
            status=i.state or "none",
            cnt=i.cnt,
            owner=i.owners,
        )
        for i in sql_res
    ]

    return res


@dataclass
class DagDurationInfo:
    dag_id: str
    duration: float


def get_dag_duration_info() -> List[DagDurationInfo]:
    """get duration of currently running DagRuns
    :return dag_info
    """
    assert Session is not None

    driver = Session.bind.driver  # type: ignore
    durations = {
        "pysqlite": func.julianday(
            func.current_timestamp() - func.julianday(DagRun.start_date)
        )
        * 86400.0,
        "mysqldb": func.timestampdiff(text("second"), DagRun.start_date, func.now()),
        "mysqlconnector": func.timestampdiff(
            text("second"), DagRun.start_date, func.now()
        ),
        "pyodbc": func.sum(
            func.datediff(text("second"), DagRun.start_date, func.now())
        ),
        "default": func.now() - DagRun.start_date,
    }
    duration = durations.get(driver, durations["default"])

    sql_res = (
        Session.query(  # type: ignore
            DagRun.dag_id, func.max(duration).label("duration")
        )
        .group_by(DagRun.dag_id)
        .filter(DagRun.state == State.RUNNING)
        .join(SerializedDagModel, SerializedDagModel.dag_id == DagRun.dag_id)
        .all()
    )

    res = []

    for i in sql_res:
        if i.duration is not None:
            if driver in ("mysqldb", "mysqlconnector", "pysqlite"):
                dag_duration = i.duration
            else:
                dag_duration = i.duration.seconds

            res.append(DagDurationInfo(dag_id=i.dag_id, duration=dag_duration))

    return res


def get_dag_labels(dag_id: str) -> Dict[str, str]:
    # reuse airflow webserver dagbag
    dag = (
        GLOBAL_DAGBAG.get_dag(dag_id)
        if use_fastapi
        else current_app.dag_bag.get_dag(dag_id)  # type: ignore
    )

    if dag is None:
        return dict()

    labels = dag.params.get("labels", {})

    if hasattr(labels, "items"):
        # Airflow version 2.3+
        labels = {k: v for k, v in labels.items() if not k.startswith("__")}
    elif hasattr(labels, "value"):
        # Airflow version 2.2.*
        labels = {k: v for k, v in labels.value.items() if not k.startswith("__")}
    else:
        # Airflow version 2.0.*, 2.1.*
        labels = labels.get("__var", {})

    return labels


def _add_gauge_metric(metric, labels, value):
    metric.samples.append(Sample(metric.name, labels, value, None))


class MetricsCollector(Collector):
    """collection of metrics for prometheus"""

    def describe(self):
        return []

    def collect(self) -> Generator[Metric, None, None]:
        """collect metrics"""

        # Dag Metrics and collect all labels
        dag_info = get_dag_status_info()

        dag_status_metric = GaugeMetricFamily(
            "airflow_dag_status",
            "Shows the number of dag starts with this status",
            labels=["dag_id", "owner", "status", "paused"],
        )

        for dag in dag_info:
            labels = get_dag_labels(dag.dag_id)

            _add_gauge_metric(
                dag_status_metric,
                {
                    "dag_id": dag.dag_id,
                    "owner": dag.owner,
                    "status": dag.status,
                    "paused": dag.paused,
                    **labels,
                },
                dag.cnt,
            )

        yield dag_status_metric

        # Last DagRun Metrics
        last_dagrun_info = get_last_dagrun_info()

        dag_last_status_metric = GaugeMetricFamily(
            "airflow_dag_last_status",
            "Shows the status of last dagrun",
            labels=["dag_id", "owner", "status", "paused"],
        )

        for dag in last_dagrun_info:
            labels = get_dag_labels(dag.dag_id)

            for status in State.dag_states:
                _add_gauge_metric(
                    dag_last_status_metric,
                    {
                        "dag_id": dag.dag_id,
                        "owner": dag.owner,
                        "status": status,
                        "paused": dag.paused,
                        **labels,
                    },
                    int(dag.status == status),
                )

        yield dag_last_status_metric

        # DagRun metrics
        dag_duration_metric = GaugeMetricFamily(
            "airflow_dag_run_duration",
            "Maximum duration of currently running dag_runs for each DAG in seconds",
            labels=["dag_id"],
        )
        for dag_duration in get_dag_duration_info():
            labels = get_dag_labels(dag_duration.dag_id)

            _add_gauge_metric(
                dag_duration_metric,
                {"dag_id": dag_duration.dag_id, **labels},
                dag_duration.duration,
            )

        yield dag_duration_metric

        # Task metrics
        task_status_metric = GaugeMetricFamily(
            "airflow_task_status",
            "Shows the number of task starts with this status",
            labels=["dag_id", "task_id", "owner", "status"],
        )

        for dag_id, tasks in itertools.groupby(
            get_task_status_info(), lambda x: x.dag_id
        ):
            labels = get_dag_labels(dag_id)

            for task in tasks:
                _add_gauge_metric(
                    task_status_metric,
                    {
                        "dag_id": task.dag_id,
                        "task_id": task.task_id,
                        "owner": task.owner,
                        "status": task.status,
                        **labels,
                    },
                    task.cnt,
                )

        yield task_status_metric


REGISTRY.register(MetricsCollector())
log.info("Registered prom-export")

if use_fastapi:
    # Support for airflow 3.X using a local fastapi app
    from fastapi import FastAPI, APIRouter
    from fastapi.responses import Response

    fastapi_app = FastAPI()
    metrics_router = APIRouter()

    @metrics_router.get("/", include_in_schema=False)
    @metrics_router.get("", include_in_schema=False)
    async def metrics():
        return Response(generate_latest(), media_type="text/plain")

    fastapi_app.include_router(metrics_router, prefix="/metrics")

    FASTAPI_APP = {
        "app": fastapi_app,
        "name": "Prometheus metrics",
        "url_prefix": "/admin",
    }
else:
    # keep backward support for airflow 2.X
    from flask_appbuilder import BaseView as FABBaseView, expose as FABexpose  # type: ignore
    from flask import Response  # type: ignore

    class RBACMetrics(FABBaseView):
        route_base = "/admin/metrics/"

        @FABexpose("/")
        def list(self):
            return Response(generate_latest(), mimetype="text/plain")

    RBACmetricsView = {"view": RBACMetrics(), "name": "metrics", "category": "Admin"}


class AirflowPrometheusPlugins(AirflowPlugin):
    """plugin for show metrics"""

    name = "airflow_prometheus_plugin"
    if use_fastapi:
        fastapi_apps = [FASTAPI_APP]  # type: ignore
    else:
        appbuilder_views = [RBACmetricsView]  # type: ignore
        appbuilder_menu_items = []
