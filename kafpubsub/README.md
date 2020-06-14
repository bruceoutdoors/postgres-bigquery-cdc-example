# KafPubSub Image

Connects kafka with pubsub using a small streams app via Kafka Streams API.

Alternatively you can consider using a more low level connector API using [CloudPubSubConnector](https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector).

**This is totally experimental and you should not be using this in production.**

KafPubSub currently works on a per-topic level. It is possible to periodically query zoo-keeper for list of topics
 and stream accordingly, but this has not been investigated at length.

## Run Locally

```sh
# Set env for pubsub to run locally
export PUBSUB_EMULATOR_HOST=localhost:8085

mvn exec:java # quick run
mvn package   # build jar

java -jar target/kafpubsub.jar # run jar
```

## Build Docker Image

```sh
docker build . -t bruceoutdoor/kafpubsub
```

## Usage

### Java Jar

You need to set environment variables to configure this:

Required:

 - `BOOTSTRAP_SERVERS` - kafka bootstrap servers. Defaults to `localhost:9092`
 - `INPUT_TOPIC` - kafka topic; same name will be used to pubsub topic.
 - `PROJECT_ID` - GCP project ID
 - `APPLICATION_ID` - Consumer identity. Defaults to `kafpubsub`.
 - `AUTO_OFFSET_RESET_CONFIG` - Action to take when there is no initial offset in offset store or the desired offset is out of range. Defaults to `latest`.

Used for testing only:

 - `PUBSUB_EMULATOR_HOST` - set pubsub emulator endpoint. Not set by default.

### Python Script [OBSOLETE]

**This is no longer used.** But I'm just leaving it here cause it I like this script (:

Refer [docker-compose.yaml](../docker-compose.yaml) in postgres-bigquery-cdc-example:

```yaml
  kafpubsub:
    image: bruceoutdoor/kafpubsub
    networks:
      - dbz-net
    command: python kafpubsub.py --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers
    environment:
      - PUBSUB_EMULATOR_HOST=pubsub:8085
      # - LOGLEVEL=DEBUG # Debug mode notifies on all incoming messages - best to turn off unless required
```

Alternatively, you can execute the script itself (after `pip install -r requirements.txt`):

```
usage: kafpubsub.py [-h] [--project PROJECT] --topic TOPIC [--group-id GROUP_ID] [--bootstrap-server BOOTSTRAP_SERVER] [--auto-offset-reset {largest,smallest,error}]

optional arguments:
  -h, --help            show this help message and exit
  --project PROJECT     Google project ID. Optional if local environment
  --topic TOPIC         [Kafka] topic
  --group-id GROUP_ID   [Kafka] Name of the consumer group a consumer belongs to
  --bootstrap-server BOOTSTRAP_SERVER
                        [Kafka] bootstrap server
  --auto-offset-reset {largest,smallest,error}
                        [Kafka] Action to take when there is no initial offset in offset store or the desired offset is out of range
```