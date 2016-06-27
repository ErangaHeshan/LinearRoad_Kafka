export KAFKA_HOME=/home/miyuru/software/kafka_2.8.0-0.8.0

java -Xmx3072m -Xms3072m -cp .:lib/kafka_2.8.0-0.8.0.jar:lib/scala-library.jar:lib/log4j-1.2.15.jar:lib/metrics-core-2.2.0.jar:lib/slf4j-simple-1.6.4.jar:lib/slf4j-api-1.7.2.jar:lib/commons-logging-1.1.1.jar:bin org.linear.kafka.input.InputEventInjectorClient
