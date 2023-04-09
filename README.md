Kafka Producer Consumer
=======================

A simple Kafka producer & consumer example with a re-balance listener.

For running the application install a docker engine (Docker Desktop or Rancher Desktop) and execute.

```ssh
docker compose up
```

To start producing events into the kafka broker run ```Producer.java``` and to start consuming events run ```Consumer.java```
both files are pointing to ```localhost:9092```.