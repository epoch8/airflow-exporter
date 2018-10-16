#!/usr/bin/env bash
# Integration test that
# 1. Waits for Airflow to come up within 60 seconds
# 2. Ensures the Metrics endpoint returned a valid response
# Depends on Curl

AIRFLOW_SLEEP_DURATION=90 # Number of seconds to wait for airflow to start
ADMIN_ENDPOINT="http://localhost:8080/admin/"
METRICS_ENDPOINT="http://localhost:8080/admin/metrics/"

# Return nonzero status code if endpoint does not return 200
CURL_FLAGS="--show-error --fail"

echo "Waiting ${AIRFLOW_SLEEP_DURATION} seconds for Airflow to start before pinging"
sleep ${AIRFLOW_SLEEP_DURATION}
curl ${CURL_FLAGS} --silent ${ADMIN_ENDPOINT} && echo 'Admin console is up'
curl ${CURL_FLAGS} ${METRICS_ENDPOINT}

# TODO: validate the contents of the CURLed data
