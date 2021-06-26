#!/bin/bash

set -m

BOOTSTRAP_SERVERS=${BOOTSTRAP_SERVERS:-broker:9092}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}
RESTPORT=${RESTPORT:-18894}
JAR=${JAR:-"/usr/share/java/kafka-streams-examples/kafka-streams-examples-$CONFLUENT-standalone.jar"}
PIDS=()
[[ -z "$CONFIG_FILE" ]] && CONFIG_FILE_ARG="" || CONFIG_FILE_ARG="--config-file $CONFIG_FILE"
ADDITIONAL_ARGS=${ADDITIONAL_ARGS:-""}
LOG_DIR=${LOG_DIR:="logs"}
PIDS_FILE=${PIDS_FILE:=".microservices.pids"}

# check if we can write to the log directory
if [ ! -w $LOG_DIR ]; then
  echo "Unable to write to the logs dir with uid `id -u`. Logging to /tmp."
  LOG_DIR=/tmp
fi

echo "Config File arg: $CONFIG_FILE_ARG"
echo "Additional Args: $ADDITIONAL_ARGS"
echo "Starting microservices from $JAR"
echo "Connecting to cluster @ $BOOTSTRAP_SERVERS and Schema Registry @ $SCHEMA_REGISTRY_URL"

echo "Starting OrdersService"
java -cp $JAR io.confluent.examples.streams.microservices.OrdersService --bootstrap-servers $BOOTSTRAP_SERVERS --schema-registry $SCHEMA_REGISTRY_URL --port $RESTPORT $CONFIG_FILE_ARG $ADDITIONAL_ARGS >$LOG_DIR/OrdersService.log 2>&1 &
PIDS+=($!)
echo "Giving OrdersService time to start"
sleep 30

echo "Adding Inventory"
java -cp $JAR io.confluent.examples.streams.microservices.AddInventory --bootstrap-servers $BOOTSTRAP_SERVERS $CONFIG_FILE_ARG $ADDITIONAL_ARGS >$LOG_DIR/AddInventory.log 2>&1 &
sleep 5

for SERVICE in "InventoryService" "FraudService" "OrderDetailsService" "ValidationsAggregatorService" "EmailService"; do
  echo "Starting $SERVICE"
  java -cp $JAR io.confluent.examples.streams.microservices.$SERVICE --bootstrap-servers $BOOTSTRAP_SERVERS --schema-registry $SCHEMA_REGISTRY_URL $CONFIG_FILE_ARG $ADDITIONAL_ARGS >$LOG_DIR/$SERVICE.log 2>&1 &
  PIDS+=($!)
done
sleep 20

echo "Posting Orders and Payments"
java -cp $JAR io.confluent.examples.streams.microservices.PostOrdersAndPayments --bootstrap-servers $BOOTSTRAP_SERVERS --schema-registry $SCHEMA_REGISTRY_URL --order-service-url "http://localhost:$RESTPORT" $CONFIG_FILE_ARG >$LOG_DIR/PostOrdersAndPayments.log 2>&1 &

PIDS+=($!)
sleep 10

echo "Microservice processes running under PIDS: ${PIDS[@]}"
echo "${PIDS[@]}" > $PIDS_FILE 
wait $PIDS

