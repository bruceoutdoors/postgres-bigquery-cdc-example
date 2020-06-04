from __future__ import absolute_import
import argparse
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.external.kafka import ReadFromKafka
import logging
# from simple_avro_deserializer import SimpleAvroDeserializer

def format_result(key, value):
    key = key.decode("utf-8")
    value = value.decode("utf-8")
    logging.info(f'K: {key}\nV: {value}')
    return '%s: %d' % (key, value)

class LoggingDoFn(beam.DoFn):
    def process(self, element):
        logging.info(element)

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        default='output.txt',
        help='Output file to write results to.')
    parser.add_argument(
        '--bootstrap-server',
        dest='bootstrap_server',
        default='localhost:9092',
        help='Kafka instance')

    args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--job_name=pg-dbz-bq-example',
    ])

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    # pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        (p | 'Read from Kafka' >> ReadFromKafka(
                consumer_config = {
                    'bootstrap.servers' : args.bootstrap_server,
                    'auto.offset.reset': 'earliest'
                },
                topics=['dbserver1.inventory.customers'])
            | 'Format Kafka KV Pairs to Text' >> beam.Map(format_result)
            | '2 Second Window' >> beam.WindowInto(window.FixedWindows(2))
            | 'Log' >> beam.ParDo(LoggingDoFn())
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()