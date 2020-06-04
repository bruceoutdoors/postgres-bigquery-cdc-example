from confluent_kafka import DeserializingConsumer
from confluent_kafka.serialization import StringDeserializer

if __name__ == '__main__':
    consumer_conf = {'bootstrap.servers' : 'localhost:9092',
                     'key.deserializer'  : StringDeserializer,
                     'group.id'          : 'mygroup',
                     'auto.offset.reset' : "earliest"}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe(['dbserver1.inventory.customers'])

    print('Listening...')
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            print(msg.value())
        except KeyboardInterrupt:
            break

    consumer.close()