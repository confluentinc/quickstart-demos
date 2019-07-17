#!/bin/bash


################################################################################
# Overview
################################################################################
#
################################################################################

# Source library
. ../../../utils/helper.sh
. ./rbac_lib.sh

check_env || exit 1
check_cli_v2 || exit 1
check_jq || exit 1

##################################################
# Initialize
##################################################

. ../config/local-demo.env
ORIGINAL_CONFIGS_DIR=/tmp/original_configs
DELTA_CONFIGS_DIR=../delta_configs
FILENAME=ksql-server.properties
create_temp_configs $CONFLUENT_HOME/etc/ksql/$FILENAME $ORIGINAL_CONFIGS_DIR/$FILENAME $DELTA_CONFIGS_DIR/${FILENAME}.delta

# Log in to Metadata Server (MDS)
login_mds $MDS

##################################################
# Administrative Functions
# - Grant principal User:$USER_ADMIN_KSQL the ResourceOwner role to Topic:_confluent-ksql-${KSQL_SERVICE_ID}_command_topic
# - Grant principal User:$USER_ADMIN_KSQL the ResourceOwner role to Topic:${KSQL_SERVICE_ID}_ksql_processing_log
# - Start KSQL
# - Grant principal User:$USER_ADMIN_KSQL the SecurityAdmin role to the KSQL Cluster
# - Grant principal User:$USER_ADMIN_KSQL the ResourceOwner role to KsqlCluster:ksql-cluster
##################################################

# Get the Kafka cluster id
get_cluster_id_kafka

# Use the default KSQL app identifier
KSQL_SERVICE_ID=rbac-ksql

echo -e "\n# Grant principal User:$USER_ADMIN_KSQL the ResourceOwner role to Topic:_confluent-ksql-${KSQL_SERVICE_ID}_command_topic"
echo "confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role ResourceOwner --resource Topic:_confluent-ksql-${KSQL_SERVICE_ID}_command_topic --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role ResourceOwner --resource Topic:_confluent-ksql-${KSQL_SERVICE_ID}_command_topic --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Grant principal User:$USER_ADMIN_KSQL the ResourceOwner role to Topic:${KSQL_SERVICE_ID}ksql_processing_log"
echo "confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role ResourceOwner --resource Topic:${KSQL_SERVICE_ID}ksql_processing_log --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role ResourceOwner --resource Topic:${KSQL_SERVICE_ID}ksql_processing_log --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Bring up KSQL server"
confluent local start ksql-server

echo "Sleeping 10 seconds"
sleep 10

echo -e "\n# Grant principal User:$USER_ADMIN_KSQL the SecurityAdmin role to the KSQL Cluster to make requests to the MDS to learn whether the user hitting its REST API is authorized to perform certain actions"
echo "confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role SecurityAdmin --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role SecurityAdmin --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID

echo -e "\n# Grant principal User:$USER_ADMIN_KSQL the ResourceOwner role to KsqlCluster:ksql-cluster"
echo "confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role ResourceOwner --resource KsqlCluster:ksql-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
confluent iam rolebinding create --principal User:$USER_ADMIN_KSQL --role ResourceOwner --resource KsqlCluster:ksql-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID

echo -e "\n# List the role bindings for the principal User:$USER_ADMIN_KSQL"
echo "confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# List the role bindings for the principal User:$USER_ADMIN_KSQL to the KSQL cluster"
echo "confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID


##################################################
# KSQL client functions
# - Grant principal User:${USER_KSQL} the ResourceOwner role to KsqlCluster:ksql-cluster
# - Grant principal User:${USER_KSQL} the DeveloperManage role to Cluster:kafka-cluster
# - Grant principal User:${USER_KSQL} the ResourceOwner role to Group:_confluent-ksql-${KSQL_SERVICE_ID} prefix
# - Grant principal User:${USER_KSQL} the ResourceOwner role to Topic:${KSQL_SERVICE_ID}ksql_processing_log
# - Grant principal User:${USER_KSQL} the DeveloperRead role to Topic:$DATA_TOPIC
# - List the role bindings for the principal User:${USER_KSQL}
# - Grant principal User:${USER_ADMIN_KSQL} the DeveloperManage role to Cluster:kafka-cluster"
# - Grant principal User:${USER_ADMIN_KSQL} the ResourceOwner role to Group:_confluent-ksql-${KSQL_SERVICE_ID} prefix"
# - Grant principal User:${USER_ADMIN_KSQL} the DeveloperRead role to Topic:$DATA_TOPIC"
# - List the role bindings for the principal User:$USER_ADMIN_KSQL"
##################################################

DATA_TOPIC=topic3

# Because KSQL server ultimately executes all the commands that the KSQL client issues
# all permissions need to be duplicated for both ${USER_KSQL} and ${USER_ADMIN_KSQL}

echo -e "\n# Grant principal User:${USER_KSQL} the ResourceOwner role to KsqlCluster:ksql-cluster"
echo "confluent iam rolebinding create --principal User:${USER_KSQL} --role ResourceOwner --resource KsqlCluster:ksql-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
confluent iam rolebinding create --principal User:${USER_KSQL} --role ResourceOwner --resource KsqlCluster:ksql-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID

echo -e "\n# Grant principal User:${USER_KSQL} the DeveloperManage role to Cluster:kafka-cluster"
echo "confluent iam rolebinding create --principal User:${USER_KSQL} --role DeveloperManage --resource Cluster:kafka-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_KSQL} --role DeveloperManage --resource Cluster:kafka-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Grant principal User:${USER_KSQL} the ResourceOwner role to Group:_confluent-ksql-${KSQL_SERVICE_ID} prefix"
echo "confluent iam rolebinding create --principal User:${USER_KSQL} --role ResourceOwner --resource Group:_confluent-ksql-${KSQL_SERVICE_ID} --prefix --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_KSQL} --role ResourceOwner --resource Group:_confluent-ksql-${KSQL_SERVICE_ID} --prefix --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Grant principal User:${USER_KSQL} the ResourceOwner role to Topic:${KSQL_SERVICE_ID}ksql_processing_log"
echo "confluent iam rolebinding create --principal User:${USER_KSQL} --role ResourceOwner --resource Topic:${KSQL_SERVICE_ID}ksql_processing_log --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_KSQL} --role ResourceOwner --resource Topic:${KSQL_SERVICE_ID}ksql_processing_log --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Grant principal User:${USER_KSQL} the DeveloperRead role to Topic:$DATA_TOPIC"
echo "confluent iam rolebinding create --principal User:$user --role DeveloperRead --resource Topic:$DATA_TOPIC --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_KSQL} --role DeveloperRead --resource Topic:$DATA_TOPIC --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# List the role bindings for the principal User:${USER_KSQL}"
echo "confluent iam rolebinding list --principal User:${USER_KSQL} --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding list --principal User:${USER_KSQL} --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# List the role bindings for the principal User:${USER_KSQL} to the KSQL cluster"
echo "confluent iam rolebinding list --principal User:${USER_KSQL} --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
confluent iam rolebinding list --principal User:${USER_KSQL} --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID

# Already done before KSQL server startup
#echo -e "\n# Grant principal User:${USER_ADMIN_KSQL} the ResourceOwner role to KsqlCluster:ksql-cluster"
#echo "confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role ResourceOwner --resource KsqlCluster:ksql-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
#confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role ResourceOwner --resource KsqlCluster:ksql-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID

echo -e "\n# Grant principal User:${USER_ADMIN_KSQL} the DeveloperManage role to Cluster:kafka-cluster"
echo "confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role DeveloperManage --resource Cluster:kafka-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role DeveloperManage --resource Cluster:kafka-cluster --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Grant principal User:${USER_ADMIN_KSQL} the ResourceOwner role to Group:_confluent-ksql-${KSQL_SERVICE_ID} prefix"
echo "confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role ResourceOwner --resource Group:_confluent-ksql-${KSQL_SERVICE_ID} --prefix --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role ResourceOwner --resource Group:_confluent-ksql-${KSQL_SERVICE_ID} --prefix --kafka-cluster-id $KAFKA_CLUSTER_ID

# Already done before KSQL server startup
#echo -e "\n# Grant principal User:${USER_ADMIN_KSQL} the ResourceOwner role to Topic:${KSQL_SERVICE_ID}ksql_processing_log"
#echo "confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role ResourceOwner --resource Topic:${KSQL_SERVICE_ID}ksql_processing_log --kafka-cluster-id $KAFKA_CLUSTER_ID"
#confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role ResourceOwner --resource Topic:${KSQL_SERVICE_ID}ksql_processing_log --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# Grant principal User:${USER_ADMIN_KSQL} the DeveloperRead role to Topic:$DATA_TOPIC"
echo "confluent iam rolebinding create --principal User:$user --role DeveloperRead --resource Topic:$DATA_TOPIC --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding create --principal User:${USER_ADMIN_KSQL} --role DeveloperRead --resource Topic:$DATA_TOPIC --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# List the role bindings for the principal User:$USER_ADMIN_KSQL"
echo "confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID"
confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID

echo -e "\n# List the role bindings for the principal User:$USER_ADMIN_KSQL to the KSQL cluster"
echo "confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID"
confluent iam rolebinding list --principal User:$USER_ADMIN_KSQL --kafka-cluster-id $KAFKA_CLUSTER_ID --ksql-cluster-id $KSQL_SERVICE_ID

echo -e "\n# KSQL CLI: list topics and print messages from topic $DATA_TOPIC"
echo "ksql -u $USER_KSQL -p ksqlclient1 http://localhost:8088"
echo
ksql -u $USER_KSQL -p ksqlclient1 http://localhost:8088 <<EOF
list topics;
PRINT '$DATA_TOPIC' FROM BEGINNING LIMIT 3;
exit ;
EOF

echo -e "\n# KSQL CLI: create a new stream and select * from that stream"
echo "ksql -u $USER_KSQL -p ksqlclient1 http://localhost:8088"
echo
ksql -u $USER_KSQL -p ksqlclient1 http://localhost:8088 <<EOF
CREATE STREAM stream1 (id varchar) WITH (kafka_topic='$DATA_TOPIC', value_format='delimited');
SELECT * FROM stream1 LIMIT 3;
exit ;
EOF

echo -e "\n# KSQL CLI: create a new table and select * from that table"
echo "ksql -u $USER_KSQL -p ksqlclient1 http://localhost:8088"
echo
ksql -u $USER_KSQL -p ksqlclient1 http://localhost:8088 <<EOF
CREATE STREAM table1 (id varchar) WITH (kafka_topic='$DATA_TOPIC', value_format='delimited', key='id');
SELECT * FROM table1 LIMIT 3;
exit ;
EOF


##################################################
# Cleanup
##################################################

SAVE_CONFIGS_DIR=/tmp/rbac_configs
restore_configs $CONFLUENT_HOME/etc/ksql/${FILENAME} $ORIGINAL_CONFIGS_DIR/${FILENAME} $SAVE_CONFIGS_DIR/${FILENAME}.rbac
