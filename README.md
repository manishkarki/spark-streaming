## How to install

- install Docker
- either clone the repo or download as zip
- open with IntelliJ as an SBT project
- in a terminal window, navigate to the folder where you downloaded this repo and run `docker-compose up` to build and start the containers - we will need them to integrate various stuff e.g. Kafka with  Spark

For Streaming in data to be used as readFromSocket, just use netcat i.e `nc -lk {portNumber}`