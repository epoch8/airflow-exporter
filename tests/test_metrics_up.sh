#!/usr/bin/env bash
# Integration test that
# 1. Waits for Airflow to come up within 120 seconds
# 2. Ensures the Metrics endpoint returned a valid response
# Depends on Curl

WAIT_DURATION=300 # Number of seconds to wait for airflow to start
BASE_URL="${AIRFLOW_BASE_URL:-http://localhost:8080}"
HEALTH_ENDPOINT="${BASE_URL}/health"
METRICS_ENDPOINT="${BASE_URL}/admin/metrics/"

# Return nonzero status code if endpoint does not return 200
CURL_FLAGS=('--silent' '--show-error' '--fail')

echo ">>> Waiting for Airflow to be ready at ${HEALTH_ENDPOINT}..."
if ! >/dev/null timeout -t "${WAIT_DURATION}" \
   sh -c "until curl -sf \"${HEALTH_ENDPOINT}\" >/dev/null; do sleep 1; done"; then
   echo ">>> Timeout: Airflow not ready after ${WAIT_DURATION} seconds."
   exit 1
fi

echo ">>> Retrieving metrics from ${METRICS_ENDPOINT}..."
curl "${CURL_FLAGS[@]}" "${METRICS_ENDPOINT}"

# TODO: validate the contents of the CURLed data
