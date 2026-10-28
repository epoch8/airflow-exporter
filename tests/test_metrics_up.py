import os
import sys
import requests
from prometheus_client.parser import text_string_to_metric_families
from requests.exceptions import ConnectionError
import time

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")

HEALTH_ENDPOINT = f"{AIRFLOW_BASE_URL}/api/v2/monitor/health"
METRICS_ENDPOINT = f"{AIRFLOW_BASE_URL}/admin/metrics/"

for i in range(120):
    try:
        res = requests.get(HEALTH_ENDPOINT)
        if res.status_code == 200:
            break
    except ConnectionError:
        pass

    time.sleep(1)
else:
    print("Airflow not ready after 120 sec")
    sys.exit(1)

res = requests.get(METRICS_ENDPOINT)
if res.status_code != 200:
    print("Metrics endpoint status is not 200")
    print(res)
    print(res.text)

    sys.exit(1)

print(res.text)

expected_labels = {"env": "test", "label1": "value1", "label2": "value2"}
dummy_dag_labels = [
    sample.labels
    for family in text_string_to_metric_families(res.text)
    for sample in family.samples
    if sample.name == "airflow_dag_status" and sample.labels["dag_id"] == "dummy_dag"
]
if not dummy_dag_labels or not all(expected_labels.items() <= labels.items() for labels in dummy_dag_labels):
    print("Dag labels are missing from airflow_dag_status")
    sys.exit(1)
