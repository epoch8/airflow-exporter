ALTER USER postgres WITH PASSWORD 'postgres';


CREATE USER airflow WITH PASSWORD 'airflowpass';
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES ON DATABASE "airflow" to airflow;

