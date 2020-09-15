Create a Kafka Cluster:
```
1. zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

2. kafka-server-start.sh $KAFKA_HOME/config/server0.properties
3. kafka-server-start.sh $KAFKA_HOME/config/server1.properties
4. kafka-server-start.sh $KAFKA_HOME/config/server2.properties
```


Delete a topic
```
kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic invoices
```

List Topics
```
kafka-topics.sh --list --bootstrap-server localhost:9092
```

Kafka Consumers
---------------
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic test \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true
```
