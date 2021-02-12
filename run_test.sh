#!/bin/sh

set -e
set -x

docker-compose down -v --remove-orphans

docker-compose up -d postgres
docker-compose up initdb

docker-compose run scheduler scheduler -n 1

docker-compose run scheduler dags unpause dummy_dag
docker-compose run scheduler dags unpause slow_dag
docker-compose run scheduler dags trigger dummy_dag

docker-compose -f docker-compose.yml -f docker-compose.test.yml up --abort-on-container-exit --exit-code-from=sut sut scheduler webserver
