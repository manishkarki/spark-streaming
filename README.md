## How to install

- install Docker
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- in a terminal window, navigate to the folder where you downloaded this repo and run `docker-compose up` to build and start the containers - we will need them to integrate various stuff e.g. Kafka with  Spark

For Streaming in data to be used as readFromSocket, just use netcat i.e `nc -lk {portNumber}`

### integrating spark streaming with kafka
#### reading data from kafka
- docker-compose up from project dir
- cd /opt/kafka_2.13-2.6.0/
- bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sparkstreaming
- bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sparkstreaming
- write down the lines you want to send in

#### writing data to kafka
- docker exec -it rockthejvm-sparkstreaming-kafka bash  
- cd /opt/kafka_2.13-2.6.0/
- bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sparkstreaming