# KafPubSub Image

Connects kafka with pubsub using a small [python script](./kafpubsub.py). It may be a better to use a proper connector like [CloudPubSubConnector](https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector), but this is what I got working.

**This is totally experimental and you should not be using this in production.**

## Build


```sh
# Set env for pubsub to run locally
export PUBSUB_EMULATOR_HOST=localhost:8085

mvn exec:java -Dexec.args="xxx" # quick run
mvn package   # build jar
```


```sh
docker build . -t bruceoutdoor/kafpubsub
```

## Usage

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