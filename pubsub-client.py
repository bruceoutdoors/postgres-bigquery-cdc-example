from google.cloud import pubsub
from confluent_kafka.avro.serializer import SerializerError
from google.api_core.exceptions import AlreadyExists

subscriber = pubsub.SubscriberClient()

if __name__ == '__main__':
    project_id = 'crafty-apex-264713'
    topic_path = subscriber.topic_path(project_id, 'dbserver1.inventory.customers')
    sub_path = subscriber.subscription_path(project_id, 'sub-slot-1')

    try:
        sub = subscriber.create_subscription(sub_path, topic_path)
    except AlreadyExists:
        pass

    print('Listening...')
    future = subscriber.subscribe(sub_path, print)

    future.result() # wait...

