from confluent_kafka import DeserializingConsumer
from google.cloud import pubsub

if __name__ == '__main__':
    publisher = pubsub.PublisherClient()
    project_id = 'craftycoconuts'
    kafka_topic = 'dbserver1.inventory.customers'
    pubsub_topic = f'projects/{project_id}/topics/{kafka_topic}'
    
    # publisher.create_topic(pubsub_topic)
    
    consumer_conf = {'bootstrap.servers' : 'localhost:9092',
                     'group.id'          : 'mygroup',
                     'auto.offset.reset' : "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['dbserver1.inventory.customers'])

    print('Publish kafka values to pubsub...')
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            # We don't use msg.key()
            print('Pushed:', msg.value())
            publisher.publish(pubsub_topic, msg.value())
        except KeyboardInterrupt:
            break

    consumer.close()