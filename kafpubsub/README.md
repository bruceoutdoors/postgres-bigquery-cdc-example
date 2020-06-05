# KafPubSub Image

Connects kafka with pubsub using a small simple python script. It may be a better to use a proper connector like [CloudPubSubConnector](https://github.com/GoogleCloudPlatform/pubsub/tree/master/kafka-connector), but this is what I got working.

**This is totally experimental and you should not be using this in production.**

## Build

```sh
docker build . -t bruceoutdoors/kafpubsub
```

## Usage

Refer [docker-compose.yaml](../docker-compose.yaml) in postgres-bigquery-cdc-example:

```yaml
  kafpubsub:
    image: bruceoutdoors/kafpubsub
    networks:
      - dbz-net
    command: python kafpubsub.py --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers
    environment:
      - PUBSUB_EMULATOR_HOST=pubsub:8085
      # - LOGLEVEL=DEBUG # Debug mode notifies on all incoming messages - best to turn off unless required
```
