from google.cloud import pubsub
from simple_avro_deserializer import SimpleAvroDeserializer
from confluent_kafka.avro.serializer import SerializerError
from google.api_core.exceptions import AlreadyExists
import json

subscriber = pubsub.SubscriberClient()
serialize = SimpleAvroDeserializer('http://localhost:8081')

def comsume_message(msg):
    if msg.data is not None:
        # we receive only values
        try:
            data = serialize(msg.data)
        except SerializerError as e:
            data = f'SerializerError: {e}\nPaylod: {msg.data}'
        print(data)

if __name__ == '__main__':
    project = 'crafty-apex-264713'
    topic = 'dbserver1.inventory.customers'
    subscription_slot = 'sub-slot-1'

    pubsub_topic = f'projects/{project}/topics/{topic}'
    sub_name = f'projects/{project}/subscriptions/{subscription_slot}'

    try:
        sub = subscriber.create_subscription(name=sub_name, topic=pubsub_topic)
    except AlreadyExists:
        pass

    print('PubSub Client Listening...')
    future = subscriber.subscribe(sub_name, comsume_message)

    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()

