#!/bin/bash

# Source library
. ../utils/helper.sh

source config/demo.cfg
DELTA_CONFIGS_DIR=delta_configs
source $DELTA_CONFIGS_DIR/env.delta

check_env || exit 1
check_aws || exit 1
check_timeout || exit 1

echo -e "\n\nKinesis data:"
while read -r line ; do echo "$line" | base64 -id; echo; done <<< "$(aws kinesis get-records --region $KINESIS_REGION --shard-iterator $(aws kinesis get-shard-iterator --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON --stream-name $KINESIS_STREAM_NAME --query 'ShardIterator' --region $KINESIS_REGION) --limit 10 | jq -r '.Records[].Data')"

echo -e "\nKafka topic data:"
echo -e "confluent local consume $KAFKA_TOPIC_NAME_IN -- --cloud --from-beginning --property print.key=true --max-messages 10"
export KAFKA_LOG4J_OPTS="-Dlog4j.rootLogger=DEBUG,stdout -Dlog4j.logger.kafka=DEBUG,stdout" && timeout 10 confluent local consume $KAFKA_TOPIC_NAME_IN -- --cloud --from-beginning --property print.key=true --max-messages 10 2>/dev/null
echo -e "\nconfluent local consume $KAFKA_TOPIC_NAME_OUT2 -- --cloud --from-beginning --property print.key=true --max-messages 10"
export KAFKA_LOG4J_OPTS="-Dlog4j.rootLogger=DEBUG,stdout -Dlog4j.logger.kafka=DEBUG,stdout" && timeout 10 confluent local consume $KAFKA_TOPIC_NAME_OUT2 -- --cloud --from-beginning --property print.key=true --max-messages 10 2>/dev/null
echo -e "\nconfluent local consume $KAFKA_TOPIC_NAME_OUT1 -- --cloud --from-beginning --property print.key=true --value-format avro --property basic.auth.credentials.source=${BASIC_AUTH_CREDENTIALS_SOURCE} --property schema.registry.basic.auth.user.info=${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO} --property schema.registry.url=${SCHEMA_REGISTRY_URL} --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --max-messages 10"
export KAFKA_LOG4J_OPTS="-Dlog4j.rootLogger=DEBUG,stdout -Dlog4j.logger.kafka=DEBUG,stdout" && timeout 10 confluent local consume $KAFKA_TOPIC_NAME_OUT1 -- --cloud --from-beginning --property print.key=true --value-format avro --property basic.auth.credentials.source=${BASIC_AUTH_CREDENTIALS_SOURCE} --property schema.registry.basic.auth.user.info=${SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO} --property schema.registry.url=${SCHEMA_REGISTRY_URL} --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --max-messages 10 2>/dev/null

echo -e "\nCloud storage data:"
AVRO_VERSION=1.9.1
#if [[ ! -f avro-tools-${AVRO_VERSION}.jar ]]; then
#  curl -L http://mirror.metrocast.net/apache/avro/avro-${AVRO_VERSION}/java/avro-tools-${AVRO_VERSION}.jar --output avro-tools-${AVRO_VERSION}.jar
#fi
if [[ "$DESTINATION_STORAGE" == "s3" ]]; then
  for key in $(aws s3api list-objects --bucket $STORAGE_BUCKET_NAME | jq -r '.Contents[].Key'); do
    echo "S3 key: $key"
    #aws s3 cp s3://$STORAGE_BUCKET_NAME/$key data.avro
    #echo "java -Dlog4j.configuration="file:log4j.properties" -jar avro-tools-${AVRO_VERSION}.jar tojson data.avro"
    #java -Dlog4j.configuration="file:log4j.properties" -jar avro-tools-${AVRO_VERSION}.jar tojson data.avro
  done
else
  for path in $(gsutil ls -r "gs://$STORAGE_BUCKET_NAME/topics/*/*/*.avro"); do
    echo "GCS path: $path"
    #gsutil cp $path data.avro
    #echo "java -Dlog4j.configuration="file:log4j.properties" -jar avro-tools-${AVRO_VERSION}.jar tojson data.avro"
    #java -Dlog4j.configuration="file:log4j.properties" -jar avro-tools-${AVRO_VERSION}.jar tojson data.avro
  done
fi
