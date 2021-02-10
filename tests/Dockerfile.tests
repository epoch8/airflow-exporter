FROM alpine:3.8

COPY . /tests
WORKDIR /tests

RUN apk add --no-cache \
    bash \
    curl

CMD ["/tests/test_metrics_up.sh"]
