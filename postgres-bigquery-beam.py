from __future__ import absolute_import
import argparse
import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io import ReadFromPubSub, BigQueryDisposition, WriteToBigQuery
import logging
from datetime import date

# it's not as complicated as it sounds
def json_to_row(msg):
    dat = msg.decode('utf-8')
    logging.info(f'Payload: {dat}')
    return {
        'insert_date': date.today(),
        'json_dat': dat,
    }

def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the wordcount pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        default='output.txt',
        help='Output file to write results to.')
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
            p | 'Read from PubSub' >>
                    beam.io.ReadFromPubSub(topic=pubsub_topic)
              | '2 Second Window' >>
                    beam.WindowInto(window.FixedWindows(2))
              | 'Json -> Row' >>
                    beam.FlatMap(json_to_row)
              | 'Write to BigQuery' >>
                    WriteToBigQuery(
                        'crafty-apex-264713:inventory.customers',
                        schema='insert_date:DATETIME, json_dat:STRING',
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                        write_disposition=BigQueryDisposition.WRITE_APPEND
                    )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()