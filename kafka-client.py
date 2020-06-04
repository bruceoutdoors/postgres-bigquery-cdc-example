from confluent_kafka import DeserializingConsumer
from simple_avro_deserializer import SimpleAvroDeserializer

if __name__ == '__main__':
    serializer = SimpleAvroDeserializer('http://127.0.0.1:8081')
    consumer_conf = {'bootstrap.servers' : 'localhost:9092',
                     'value.deserializer': serializer,
                     'key.deserializer'  : serializer,
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

            # We don't use msg.key()
            print(msg.value())
        except KeyboardInterrupt:
            break

    consumer.close()