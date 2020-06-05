from google.cloud import pubsub
from confluent_kafka.avro.serializer import SerializerError
from google.api_core.exceptions import AlreadyExists
import json

subscriber = pubsub.SubscriberClient()

def comsume_message(msg):
    val = msg.data.decode('utf-8')

    try:
        val = json.loads(val)
        print(f'Payload: {val}\n')
    except json.decoder.JSONDecodeError:
        print(f'Invalid json: {val}\n')

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

