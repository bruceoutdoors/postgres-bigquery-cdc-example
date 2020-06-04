from confluent_kafka import DeserializingConsumer
from google.cloud import pubsub
from confluent_kafka.serialization import StringDeserializer

if __name__ == '__main__':
    publisher = pubsub.PublisherClient()
    project_id = 'crafty-apex-264713'
    kafka_topic = 'dbserver1.inventory.customers'
    pubsub_topic = f'projects/{project_id}/topics/{kafka_topic}'
    
    try:
        publisher.create_topic(pubsub_topic)
    except:
        pass # I don't need an error if topic already created.
    
    consumer_conf = {'bootstrap.servers' : 'localhost:9092',
                     'group.id'          : 'kafka-pubsub',
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
            print(f'Pushed: {msg.value()}\n')

            publisher.publish(pubsub_topic, msg.value())
        except KeyboardInterrupt:
            break

    consumer.close()