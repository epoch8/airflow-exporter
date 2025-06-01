# Changelog
All notable changes to this project will be documented in this file.

## 1.6.0

- Make airflow-exporter compatible with Airflow 3 by @drshott

## 1.5.4

- Fix the problem where customised dag.params.labels Dict cannot be considered
  in the dag labels [#118](https://github.com/epoch8/airflow-exporter/pull/118)
  by @zemin-piao
- Set the correct mimetype
  [#121](https://github.com/epoch8/airflow-exporter/pull/121) by @digitamo

## 1.5.3

- Fix Airflow 2.2.* compatiblity
  [#108](https://github.com/epoch8/airflow-exporter/issues/108)
- (post1) Restrict Airflow version to less than 3

## 1.5.2

- Fix DAG not found in serialized_dag table
  [#98](https://github.com/epoch8/airflow-exporter/issues/98) by @sawaca96

## 1.5.1

- Add compatibility with `mysqlconnector` DB connection
  [#94](https://github.com/epoch8/airflow-exporter/pull/94) by @lxxstc

## 1.5.0

- Add `airflow_dag_last_status`
  [#96](https://github.com/epoch8/airflow-exporter/pull/96) by @nvn01234

## 1.4.2

- Fix for duplicated #HELP entries

## 1.4.0 + 1.4.1

- Add support for Airflow 2.0
  [#90](https://github.com/epoch8/airflow-exporter/pull/90) by @dimon222

## 1.3.2

- Remove 'hostname' from airflow_task_status by @cansjt see
  [#77](https://github.com/epoch8/airflow-exporter/issues/77) for details

## 1.3.0

- Add 'hostname' to airflow_task_status by @forthgate
- Added pyodbc to dag duration calc by @baolsen

## 1.2.0

- Add custom labels to DAG-related metrics

## 1.1.0

- Fix [#59](https://github.com/epoch8/airflow-exporter/issues/59) Limit
  cardinality of `airflow_dag_run_duration`

## 1.0 - 2019-06-08

Breaking compatibility with Airflow versions prior to 1.10.3

- Fix [#46](https://github.com/epoch8/airflow-exporter/issues/46) Airflow 1.10.3
  compatiblity by @sockeye44
- Fix [#44](https://github.com/epoch8/airflow-exporter/issues/44) Deliver plugin
  as a python package by @maxbrunet

## 0.5.4 - 2019-03-05

- Move Metrics to Admin View

## 0.5.3 - 2019-01-11

- Fix import error for dummy operator
  [#39](https://github.com/epoch8/airflow-exporter/pull/39) by @msumit
- Fix requirements.txt issue
  [#41](https://github.com/epoch8/airflow-exporter/pull/41) by @slash-cyberpunk

## 0.5.2 - 2018-12-10

- Fix for negative values with MySQL backend by @ebartels

## 0.5.1 - 2018-11-28

- Fix [#36](https://github.com/epoch8/airflow-exporter/issues/36): Exporter
  fails with Sqlite DB used in development environment by @ryan-carlson

## 0.5 - 2018-11-23

- Fix [#28](https://github.com/epoch8/airflow-exporter/issues/28): Add support
  to show prometheus metrics when rbac is enabled in airflow by @phani8996
- Fix [#34](https://github.com/epoch8/airflow-exporter/issues/34): fix run 1.8
  and 1.9 by @cleverCat

## 0.4.4 - 2018-11-21

- Fix [#29](https://github.com/epoch8/airflow-exporter/issues/29): fix run with
  mysql by @cleverCat

## 0.4.3 - 2018-11-21

- Fix [#23](https://github.com/epoch8/airflow-exporter/issues/23): Airflow
  database CPU usage by @cleverCat

## 0.4.2 - 2018-11-19

- Fix [#13](https://github.com/epoch8/airflow-exporter/pull/20): Added test
  script and travis file by @hydrosquall
- Fix [#13](https://github.com/epoch8/airflow-exporter/pull/27): fix run test in
  travis @cleverCat

## 0.4.1 - 2018-11-13

- Fix [#24](https://github.com/epoch8/airflow-exporter/issues/24): Unsupported
  mime-type by @szyn

## 0.4 - 2018-10-15

- Fix [#14](https://github.com/epoch8/airflow-exporter/issues/14): Airflow 1.10
  compatibility by @jmcarp
- Fix [#16](https://github.com/epoch8/airflow-exporter/issues/16): Exception
  during scrape

## 0.3 - 2018-09-12

- [#11](https://github.com/epoch8/airflow-exporter/pull/11): Added metric for
  duration of DagRuns by @hydrosquall

## 0.2 - 2018-09-10 more labels

Added:
- `owner` label in metrics `dag_status` and `task_status`
- explicit 0 metric for each state in `dag_status`

## 0.1 - 2018-08-07 initial release
