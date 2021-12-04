export RUN_DB='docker run -d
    --name db
    -p 3306:3306
    --env MYSQL_ROOT_PASSWORD=root
    --env MYSQL_USER=airflow
    --env MYSQL_PASSWORD=airflow
    --env MYSQL_DATABASE=airflow
    mysql:5.7
    --explicit_defaults_for_timestamp=1'

export PROVIDER="apache-airflow-providers-mysql"

export AIRFLOW__CORE__SQL_ALCHEMY_CONN="mysql://airflow:airflow@127.0.0.1:3306/airflow"
