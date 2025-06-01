import os
import sys
import requests
from requests.exceptions import ConnectionError
import time

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_MAJOR_VERSION = os.environ.get("AIRFLOW_MAJOR_VERSION", "2")

if AIRFLOW_MAJOR_VERSION == "3":
    HEALTH_ENDPOINT = f"{AIRFLOW_BASE_URL}/api/v2/monitor/health"
else:
    HEALTH_ENDPOINT = f"{AIRFLOW_BASE_URL}/health"
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
