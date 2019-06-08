# Airflow prometheus exporter

![travis build status](https://travis-ci.org/epoch8/airflow-exporter.svg?branch=master)

Exposes dag and task based metrics from Airflow to a Prometheus compatible endpoint.

## Screenshots

<img src="https://epoch8.github.io/media/2018/08/03/monitoring-airflow-with-prometheus/metrics_screenshot.png" height="400" width="600"/>

## Compatibility

**Note: this version is compatible with Airflow 1.10.3+ only, see [#46](https://github.com/epoch8/airflow-exporter/issues/46) for details**

For compatibility with previous versions of Airflow use older version: [v0.5.4](https://github.com/epoch8/airflow-exporter/releases/tag/v0.5.4)

* Airflow: airflow1.10.3+
* Python: python2, python3
* DataBase: postgresql, mysql

## Install

```sh
pip install https://github.com/epoch8/airflow-exporter/archive/airflow-1.10.3.tar.gz
```

That's it. You're done.

## Metrics

Metrics will be available at 

```
http://<your_airflow_host_and_port>/admin/metrics/
```

### `airflow_task_status`

Labels:

* `dag_id`
* `task_id`
* `owner`
* `status`

Value: number of tasks in specific status.

### `airflow_dag_status`

Labels:

* `dag_id`
* `owner`
* `status`

Value: number of dags in specific status.

### `airflow_dag_run_duration`

Labels:

* `dag_id`: unique identifier for a given DAG
* `run_id`: unique identifier created each time a DAG is run

Value: duration in seconds that a DAG Run has been running for. This metric is not available for DAGs that have already completed.

## License

Distributed under the BSD license. See [LICENSE](LICENSE) for more
information.
