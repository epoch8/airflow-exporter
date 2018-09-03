FROM puckel/docker-airflow:1.8.1

ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux
ENV AIRFLOW_HOME /usr/local/airflow

USER root
RUN apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys B97B0AFCAA1A47F044F244A07FCC7D46ACCC4CF8 \
    && echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update

# It is necessary for the container to build
RUN mkdir -p /usr/share/man/man1/ \
    && mkdir -p /usr/share/man/man7/

RUN apt-get install -y --no-install-recommends postgresql-common postgresql-client-9.6 postgresql-contrib-9.6

RUN pip install simplejson  && \
    pip install sqlalchemy && \
    pip install prometheus_client && \
    pip install flask-bcrypt

USER airflow

