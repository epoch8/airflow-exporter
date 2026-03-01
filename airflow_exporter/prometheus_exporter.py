import itertools
import logging
from dataclasses import dataclass
from typing import Dict, Generator, List

from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.utils.state import State

# Support for airflow 3.X using a local fastapi app
from fastapi import APIRouter, FastAPI
from fastapi.responses import Response

# Importing base classes that we need to derive
from prometheus_client import REGISTRY, generate_latest
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector
from prometheus_client.samples import Sample
from sqlalchemy import func, text  # type: ignore

log = logging.getLogger(__name__)

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
        .filter(~DagModel.is_stale)
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

    last_dagrun_query = Session.query(  # type: ignore
        DagRun.dag_id,
        DagRun.state,
        func.row_number()
        .over(partition_by=DagRun.dag_id, order_by=DagRun.logical_date.desc())  # type: ignore
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
        .filter(last_dagrun_query.c.row_number == 1, ~DagModel.is_stale)
        .join(DagModel, DagModel.dag_id == last_dagrun_query.c.dag_id)
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
        .filter(~DagModel.is_stale)
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
        .filter(DagRun.state == State.RUNNING, ~DagModel.is_stale)
        .join(DagModel, DagModel.dag_id == DagRun.dag_id)
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
    serialized_dag = SerializedDagModel.get_dag(dag_id)

    if serialized_dag is None or not serialized_dag.params:
        return dict()

    # Use dump() to safely get params (suppresses validation errors)
    params_dict = serialized_dag.params.dump()
    labels = params_dict.get("labels", {})

    if hasattr(labels, "items"):
        labels = {k: v for k, v in labels.items() if not k.startswith("__")}
    else:
        labels = {}

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

class AirflowPrometheusPlugins(AirflowPlugin):
    """plugin for show metrics"""

    name = "airflow_prometheus_plugin"
    fastapi_apps = [FASTAPI_APP]  # type: ignore
