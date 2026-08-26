#!/bin/sh

KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://kafka-connect:8083}"
CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-8123}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-analytics}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"

echo "Waiting for Kafka Connect to be ready..."
while ! curl -f "${KAFKA_CONNECT_URL}/connectors"; do
    echo "Kafka Connect not ready yet, waiting..."
    sleep 10
done

echo "Kafka Connect is ready. Creating ClickHouse sink connectors..."

create_connector() {
    NAME="$1"
    TOPIC="$2"
    TABLE="$3"

    curl -i -X POST \
    -H "Accept:application/json" \
    -H "Content-Type:application/json" \
    "${KAFKA_CONNECT_URL}/connectors/" -d '{
      "name": "'"${NAME}"'",
      "config": {
        "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
        "tasks.max": "1",
        "topics": "'"${TOPIC}"'",
        "hostname": "'"${CLICKHOUSE_HOST}"'",
        "port": "'"${CLICKHOUSE_PORT}"'",
        "database": "'"${CLICKHOUSE_DATABASE}"'",
        "table": "'"${TABLE}"'",
        "username": "'"${CLICKHOUSE_USER}"'",
        "password": "'"${CLICKHOUSE_PASSWORD}"'",
        "ssl": "false",
        "jdbcConnectionProperties": "?custom_http_params=enable_http_compression=0",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "true",
        "errors.tolerance": "all",
        "errors.log.enable": "true",
        "errors.log.include.messages": "true"
      }
    }'

    echo -e "\n\nCreated ${NAME} connector"
}

create_connector "clickhouse-sink-alarms" "alarms" "alarms"
create_connector "clickhouse-sink-business" "business" "business"
create_connector "clickhouse-sink-operations" "operations" "operations"

echo "All connectors created successfully!"
