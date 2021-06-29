#!/bin/bash

for metric in ReplicasCount InSyncReplicasCount CaughtUpReplicasCount ObserversInIsrCount
do

  echo -e "\n\n==> JMX metric: $metric \n"

  for topic in single-region multi-region-sync multi-region-async multi-region-async-op-under-min-isr multi-region-async-op-under-replicated multi-region-async-op-leader-is-observer multi-region-default
  do

    test "$(docker inspect -f '{{.State.ExitCode}}' $(docker ps -laq --filter="name=broker-west-1"))" = "0" \
      && BW1=$(docker-compose exec broker-west-1 kafka-run-class kafka.tools.JmxTool --jmx-url service:jmx:rmi:///jndi/rmi://localhost:8091/jmxrmi --object-name kafka.cluster:type=Partition,name=$metric,topic=$topic,partition=0 --one-time true | tail -n 1 | awk -F, '{print $2;}' | head -c 1) \
      || BW1=0

    test "$(docker inspect -f '{{.State.ExitCode}}' $(docker ps -laq --filter="name=broker-west-2"))" = "0" \
      && BW2=$(docker-compose exec broker-west-2 kafka-run-class kafka.tools.JmxTool --jmx-url service:jmx:rmi:///jndi/rmi://localhost:8092/jmxrmi --object-name kafka.cluster:type=Partition,name=$metric,topic=$topic,partition=0 --one-time true | tail -n 1 | awk -F, '{print $2;}' | head -c 1) \
      || BW2=0

    if [ $topic != "single-region" ]; then
    test "$(docker inspect -f '{{.State.ExitCode}}' $(docker ps -laq --filter="name=broker-east-3"))" = "0" \
      && BE3=$(docker-compose exec broker-east-3 kafka-run-class kafka.tools.JmxTool --jmx-url service:jmx:rmi:///jndi/rmi://localhost:8093/jmxrmi --object-name kafka.cluster:type=Partition,name=$metric,topic=$topic,partition=0 --one-time true | tail -n 1 | awk -F, '{print $2;}' | head -c 1) \
      || BE3=0
    fi

    if [ $topic != "single-region" ]; then
    test "$(docker inspect -f '{{.State.ExitCode}}' $(docker ps -laq --filter="name=broker-east-4"))" = "0" \
      && BE4=$(docker-compose exec broker-east-4 kafka-run-class kafka.tools.JmxTool --jmx-url service:jmx:rmi:///jndi/rmi://localhost:8094/jmxrmi --object-name kafka.cluster:type=Partition,name=$metric,topic=$topic,partition=0 --one-time true | tail -n 1 | awk -F, '{print $2;}' | head -c 1) \
      || BE4=0
    fi

    REPLICAS=$((BW1 + BW2 + BE3 + BE4))
    echo "$topic: $REPLICAS"
  done

done
