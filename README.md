# Postgres to BigQuery CDC Pipeline Example

A CDC pipeline that streams postgres database table changes to BigQuery via Debezium, PubSub, Avro, Dataflow+Python.

## TODO

- the dataflow part

## Quickstart

```sh
# Python 3.8 is not supported in Beam 2.21 and Dataflow. To install Python 3.7 in Ubuntu 20.04 you can do:
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install python3.7
virtualenv --python=python3.7 ~/py37
source ~/py37/bin/activate

# jq is not required, but nice to have
sudo apt install docker.io docker-compose jq

# Start debezium + example postgres db
docker-compose up

# Remove containers. Append --volumes to drop volumes as well
docker-compose down

# Setup/update connector
curl -i -X DELETE http://localhost:8083/connectors/inventory-connector \
&& curl -i -X POST -H "Accept:application/json" \
                   -H "Content-Type:application/json" http://localhost:8083/connectors/ \
                   -d @register-postgres.json

# Query available connector
curl -H "Accept:application/json" localhost:8083/connectors/

# See inventory customer schema in connector
curl -sH "Accept:application/json" localhost:8083/connectors/inventory-connector | jq

# See customer schema in schema registry
curl -sX GET http://localhost:8081/subjects/dbserver1.inventory.customers-value/versions/1 | jq '.schema | fromjson'
# See schema from registry from global id
curl -sX GET http://localhost:8081/schemas/ids/1 | jq  '.schema | fromjson'

# Access postgres database
psql postgresql://postgres:postgres@localhost:5432/postgres
# ...you can also access from within the docker container
docker-compose exec postgres bash -c 'psql -U postgres postgres'

# Start test kafka client
pip install -r requirements.txt
python kafka-client.py

# -----------------------------------------------------------------------------------

# Set env for pubsub to run locally
export PUBSUB_EMULATOR_HOST=localhost:8085

# Start pubsub client
python pubsub-client.py


# Kotlin ----------------------------------------------------------------------------

# Direct Runner
mvn compile exec:java \
    -Dexec.mainClass=bruceoutdoors.beam.examples.PostgresCDCBigQuery \
    -Dexec.args="--project crafty-apex-264713
                " 

# Dataflow Runner
mvn compile exec:java \
    -P dataflow-runner \
    -Dexec.mainClass=bruceoutdoors.beam.examples.PostgresCDCBigQuery \
    -Dexec.args="--project crafty-apex-264713 \
                 --region asia-east1 \
                 --temp_location gs://kakfa-testing-bucket/tmp \
                 --staging_location gs://kakfa-testing-bucket/staging \
                 --schema-registry 'http://10.140.0.4:8081' \
                 --requirements_file dataflow-requirements.txt
                " 

# Python -----------------------------------------------------------------------------

# Direct Runner (You may want to comment out BigQuery task)
python postgres-bigquery-beam.py \
    --project crafty-apex-264713

# Run in job in Dataflow (can't get this to work...):
python postgres-bigquery-beam.py \
    --runner DataflowRunner \
    --project crafty-apex-264713 \
    --region asia-east1 \
    --temp_location gs://kakfa-testing-bucket/tmp \
    --staging_location gs://kakfa-testing-bucket/staging \
    --schema-registry 'http://10.140.0.4:8081' \
    --requirements_file dataflow-requirements.txt

```

## Helpful References

- [`confluent_kafka` API](https://docs.confluent.io/current/clients/confluent-kafka-python/)

## Related

 - [Kafka Connect BigQuery Connector](https://github.com/wepay/kafka-connect-bigquery) and its [announcement post](https://wecode.wepay.com/posts/kafka-bigquery-connector).
 - [MySQL → Airflow → GCS → BigQuery pipeline (WePay)](https://wecode.wepay.com/posts/bigquery-wepay)
