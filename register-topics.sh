export KAFKA_HOME=/home/miyuru/software/kafka_2.8.0-0.8.0

#$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic segstat_topic --replica 1
#$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic dailyexp_topic --replica 1
$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic output_topic --replica 1
#$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic accident_topic --replica 1
#$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic accbalance_topic --replica 1
#$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic position_reports --replica 1
#$KAFKA_HOME/bin/kafka-create-topic.sh --zookeeper 155.69.146.42:2181 --topic toll_topic --replica 1