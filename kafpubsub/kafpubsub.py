import os
import logging
import argparse
from datetime import datetime
from google.cloud import pubsub
from confluent_kafka import DeserializingConsumer

def run():
    logging.basicConfig(level=os.environ.get('LOGLEVEL', 'INFO').upper())
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        dest='project',
        default='crafty-apex-264713',
        help='Google project ID. Optional if local environment')
    parser.add_argument(
        '--topic',
        dest='topic',
        required=True,
        help='[Kafka] topic')
    parser.add_argument(
        '--group-id',
        dest='group_id',
        default='kafka-pubsub',
        help='[Kafka] Name of the consumer group a consumer belongs to')
    parser.add_argument(
        '--bootstrap-server',
        dest='bootstrap_server',
        default='localhost:9092',
        help='[Kafka] bootstrap server')
    parser.add_argument(
        '--auto-offset-reset',
        dest='auto_offset_reset',
        default='smallest',
        help='[Kafka] Action to take when there is no initial offset in offset store or the desired offset is out of range',
        choices=['largest', 'smallest', 'error'])
    
    args = parser.parse_args()
    kafpubsub(args)

def kafpubsub(args):
    publisher = pubsub.PublisherClient()
    project_id = args.project
    kafka_topic = args.topic
    pubsub_topic = f'projects/{project_id}/topics/{kafka_topic}'
    
    try:
        publisher.create_topic(pubsub_topic)
    except:
        pass # I don't need an error if topic already created.
    
    consumer_conf = {'bootstrap.servers' : args.bootstrap_server,
                     'group.id'          : args.group_id,
                     'auto.offset.reset' : args.auto_offset_reset}
    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([kafka_topic])

    logging.info(f'Publish Kafka ({args.bootstrap_server}) values to pubsub...')
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            logging.debug(f'> {datetime.today()} | {msg.key()}\n')

            publisher.publish(pubsub_topic, msg.value())
        except KeyboardInterrupt:
            break

    consumer.close()

if __name__ == '__main__':
    run()