from __future__ import absolute_import
import argparse
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, BigQueryDisposition, WriteToBigQuery
from apache_beam.io.external.kafka import ReadFromKafka
from simple_avro_deserializer import SimpleAvroDeserializer
import logging


def avro_to_row(schema_registry):
    serialize = SimpleAvroDeserializer(schema_registry)

    def convert(msg):
        try:
            dat = serialize(msg)
        except Exception as e:
            logging.warning(f'Serialize Error! ({e}) - Payload: {msg}')
            return

        logging.info(f'Payload: {dat}')
        return dat

    return convert

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--schema-registry',
        dest='schema_registry',
        default='http://127.0.0.1:8081',
        help='Schema registry endpoint. Defaults to local endpoint.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--job_name=dbz-test-example',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    project_id = 'crafty-apex-264713'
    kafka_topic = 'dbserver1.inventory.customers'
    pubsub_topic = f'projects/{project_id}/topics/{kafka_topic}'

    with beam.Pipeline(options=pipeline_options) as p:
        (
            # p | 'Read from PubSub' >>
            #         ReadFromPubSub(topic=pubsub_topic)
            p | 'Read from Kafka' >>
                    ReadFromKafka(consumer_config={"bootstrap.servers": "http://localhost:9092"},
                                  topics=[kafka_topic])
              | '2 Second Window' >>
                    beam.WindowInto(window.FixedWindows(2))
              | 'Avro to Row' >>
                    beam.FlatMap(logging.info)
              # | 'Write to BigQuery' >>
              #       WriteToBigQuery(
              #           'crafty-apex-264713:inventory.customers',
              #           schema='id:INT64,'
              #                  'first_name:STRING,'
              #                  'last_time:STRING,'
              #                  'email:STRING,'
              #                  '__op:STRING,'
              #                  '__source_ts_ms:INT64,'
              #                  '__lsn:INT64',
              #           create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
              #           write_disposition=BigQueryDisposition.WRITE_APPEND
              #       )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()