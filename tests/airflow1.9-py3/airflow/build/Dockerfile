FROM puckel/docker-airflow:1.9.0-4

USER root

RUN pip3 install prometheus_client

# Needed to write to mounted directory with proper permissions
USER ${HOST_USER_ID}
