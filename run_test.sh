#!/bin/sh

set -e
set -x

export AIRFLOW_VERSION=1.10.4

docker-compose down
docker-compose up --build -d
docker-compose exec airflow /entrypoint.sh airflow unpause dummy_dag
docker-compose exec airflow /entrypoint.sh airflow trigger_dag dummy_dag

docker-compose -f docker-compose.yml -f docker-compose.test.yml run sut