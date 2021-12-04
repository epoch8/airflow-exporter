export RUN_DB='docker run -d
    --name db
    -p 5432:5432
    --env POSTGRES_USER=airflow
    --env POSTGRES_PASSWORD=airflow
    --env POSTGRES_DB=airflow
    postgres:12.0'

export PROVIDER="apache-airflow-providers-postgres"

export AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql://airflow:airflow@localhost/airflow"
