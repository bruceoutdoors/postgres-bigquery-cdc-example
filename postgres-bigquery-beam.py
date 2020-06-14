from __future__ import absolute_import
import argparse
import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, BigQueryDisposition, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryWriteFn
from simple_avro_deserializer import SimpleAvroDeserializer
import logging


def avro_to_row(schema_registry):
    serialize = SimpleAvroDeserializer(schema_registry)

    def convert(msg):
        try:
            dat = serialize(msg)
        except Exception as e:
            logging.warning(f'Serialize Error! ({e}) - Payload: {msg}')
            return []

        logging.info(f'Payload: {dat}')
        return [dat]

    return convert


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--schema-registry',
        dest='schema_registry',
        default='http://127.0.0.1:8081',
        help='Schema registry endpoint. Defaults to local endpoint.')
    parser.add_argument(
        '--failed-bq-inserts',
        dest='failed_bq_inserts',
        required=True,
        help='Bucket for writing failed inserts')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--job_name=dbz-test-example',
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    project_id = 'crafty-apex-264713'
    kafka_topic = 'dbserver1.inventory.customers'
    pubsub_topic = f'projects/{project_id}/topics/{kafka_topic}'

    with beam.Pipeline(options=pipeline_options) as p:
        bq = (p
              | 'Read from PubSub' >>
                ReadFromPubSub(topic=pubsub_topic)
              | '2 Second Window' >>
                beam.WindowInto(window.FixedWindows(2))
              | 'Avro to Row' >>
                beam.FlatMap(avro_to_row(known_args.schema_registry))
              # | 'Write to File' >>
              #       beam.io.WriteToText('args.output')
              | 'Write to BigQuery' >>
                WriteToBigQuery(
                    'crafty-apex-264713:inventory.customers',
                    schema='id:INT64,'
                           'first_name:STRING,'
                           'last_name:STRING,'
                           'email:STRING,'
                           '__op:STRING,'
                           '__source_ts_ms:INT64,'
                           '__lsn:INT64',
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=BigQueryDisposition.WRITE_APPEND
                )
              )

        # Can't get this to run in dataflow - causes job graph that is not updatable
        # In direct runner I can't get it to spit any errors
        """
        (bq[BigQueryWriteFn.FAILED_ROWS]
            | 'Write Failed Rows' >>
              beam.io.WriteToText(known_args.failed_bq_inserts)
        )
        """


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()