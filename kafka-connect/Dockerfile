FROM confluentinc/cp-kafka-connect-base:latest

RUN confluent-hub install confluentinc/kafka-connect-avro-converter:7.0.0 --no-prompt
RUN confluent-hub install clickhouse/clickhouse-kafka-connect:v1.3.4 --no-prompt
