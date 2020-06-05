from google.cloud import pubsub
from confluent_kafka.avro.serializer import SerializerError

subscriber = pubsub.SubscriberClient()

if __name__ == '__main__':
    project_id = 'crafty-apex-264713'
    topic_path = subscriber.topic_path(project_id, 'dbserver1.inventory.customers')
    sub_path = subscriber.subscription_path(project_id, 'sub-slot-1')

    sub = subscriber.create_subscription(sub_path, topic_path)

    print('Listening...')
    future = subscriber.subscribe(sub_path, print)

    future.result() # wait...

