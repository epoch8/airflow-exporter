## Development

You can start a development environment with:

```shell
docker-compose up
```

Airflow needs to be restarted for your changes to take effect:

```shell
docker-compose restart airflow
```

## Testing

You can run the tests suite with:

```shell
docker-compose -f docker-compose.test.yml -p test_airflow-exporter up sut
```
