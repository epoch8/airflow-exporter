# Changelog
All notable changes to this project will be documented in this file.

## 0.5 - 2018-11-23

- Fix [#28](https://github.com/epoch8/airflow-exporter/issues/28): Add support to show prometheus metrics when rbac is enabled in airflow by @phani8996
- Fix [#34](https://github.com/epoch8/airflow-exporter/issues/34): fix run 1.8 and 1.9 by @cleverCat

## 0.4.4 - 2018-11-21

- Fix [#29](https://github.com/epoch8/airflow-exporter/issues/29): fix run with mysql by @cleverCat

## 0.4.3 - 2018-11-21

- Fix [#23](https://github.com/epoch8/airflow-exporter/issues/23): Airflow database CPU usage by @cleverCat

## 0.4.2 - 2018-11-19

- Fix [#13](https://github.com/epoch8/airflow-exporter/pull/20): Added test script and travis file by @hydrosquall
- Fix [#13](https://github.com/epoch8/airflow-exporter/pull/27): fix run test in travis @cleverCat

## 0.4.1 - 2018-11-13

- Fix [#24](https://github.com/epoch8/airflow-exporter/issues/24): Unsupported mime-type by @szyn

## 0.4 - 2018-10-15

- Fix [#14](https://github.com/epoch8/airflow-exporter/issues/14): Airflow 1.10 compatibility by @jmcarp
- Fix [#16](https://github.com/epoch8/airflow-exporter/issues/16): Exception during scrape

## 0.3 - 2018-09-12

- [#11](https://github.com/epoch8/airflow-exporter/pull/11): Added metric for duration of DagRuns by @hydrosquall

## 0.2 - 2018-09-10 more labels

Added:
- `owner` label in metrics `dag_status` and `task_status`
- explicit 0 metric for each state in `dag_status`

## 0.1 - 2018-08-07 initial release
