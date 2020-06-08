from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from confluent_kafka.serialization import Deserializer

class SimpleAvroDeserializer(Deserializer):
    def __init__(self, schema_registry_url):
        schema_registry = CachedSchemaRegistryClient({ 'url': schema_registry_url })
        self._serializer = MessageSerializer(schema_registry, None, None)

    def __call__(self, value, ctx=None):
        if value is None:
            return None

        if ctx is not None and ctx.field == 'key':
            decoded = self._serializer.decode_message(value, is_key=True)
        else:
            decoded = self._serializer.decode_message(value, is_key=False)

        return decoded