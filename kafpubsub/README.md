# KafPubSub Image

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
      - LOGLEVEL=DEBUG
```
