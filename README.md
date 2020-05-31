Postgres to BigQuery CDC Pipeline Example
=========================================

A CDC pipeline that streams postgres database table changes to Bigquery via Debezium, Avro, Dataflow+Python.

TODO: the dataflow part

## Quickstart

```sh
# Start debezium + example postgres db
export DEBEZIUM_VERSION=1.1
docker-compose up

# Setup connector
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json

# Access postgres database
psql postgresql://postgres:postgres@localhost:5432/postgres

# Start test kafka client
pip3 install -r requirements.txt
python3 kafka-client.py
```