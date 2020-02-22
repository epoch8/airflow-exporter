#!/bin/sh

set -e
set -x

export AIRFLOW_VERSION=1.10.9

docker-compose down -v --remove-orphans

docker-compose up -d postgresql
docker-compose run airflow /entrypoint.sh airflow initdb
docker-compose run airflow /entrypoint.sh airflow unpause dummy_dag
docker-compose run airflow /entrypoint.sh airflow unpause slow_dag
docker-compose run airflow /entrypoint.sh airflow trigger_dag dummy_dag

docker-compose -f docker-compose.yml -f docker-compose.test.yml up --abort-on-container-exit --exit-code-from=sut sut airflow
