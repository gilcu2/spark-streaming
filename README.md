
Events generated with Akka are sent to kafka and processed from spark streaming   


## Run

1. Install Kafka, run server and create topic cars
1. Install spark and add bin directory to PATH
1. sbt assembly
1. cd scripts
1. In one terminal run ./simulator.sh
1. In other run: processing.sh