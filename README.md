Postgres to BigQuery CDC Pipeline Example
=========================================

A CDC pipeline that streams postgres database table changes to Bigquery via Debezium, Avro, Dataflow+Python.

TODO: the dataflow part

## Quickstart

```sh
# jq is not required, but nice to have
sudo apt install docker.io docker-compose jq

# Start debezium + example postgres db
export DEBEZIUM_VERSION=1.1
docker-compose up

# Setup/update connector
curl -i -X DELETE http://localhost:8083/connectors/inventory-connector \
&& curl -i -X POST -H "Accept:application/json" \
                  -H  "Content-Type:application/json" http://localhost:8083/connectors/ \
                  -d @register-postgres.json

# Access postgres database
psql postgresql://postgres:postgres@localhost:5432/postgres

# Start test kafka client
pip3 install -r requirements.txt
python3 kafka-client.py
```