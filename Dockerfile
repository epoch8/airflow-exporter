ARG AIRFLOW_VERSION=1.10.3
FROM puckel/docker-airflow:${AIRFLOW_VERSION}

COPY ./tests/dags "${AIRFLOW_HOME}/dags"
COPY --chown=airflow:airflow . /tmp/airflow-exporter
RUN pip install --user /tmp/airflow-exporter \
  && rm -rf /tmp/airflow-exporter
