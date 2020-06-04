from google.cloud import pubsub
from simple_avro_deserializer import SimpleAvroDeserializer
from confluent_kafka.avro.serializer import SerializerError

subscriber = pubsub.SubscriberClient()
serialize = SimpleAvroDeserializer('http://localhost:8081')

def process_pubsub(msg):
    if msg.data is not None:
        # we receive only values
        try:
            data = serialize(msg.data)
        except SerializerError as e:
            data = f'SerializerError: {e}\nPaylod: {msg.data}'

        print(data)


if __name__ == '__main__':
    topic_path = subscriber.topic_path('craftycoconuts', 'dbserver1.inventory.customers')
    sub_path = subscriber.subscription_path('craftycoconuts', 'sub-slot-1')

    # sub = subscriber.create_subscription(sub_path, topic_path)

    print('Listening...')
    future = subscriber.subscribe(sub_path, process_pubsub)

    future.result() # wait...

