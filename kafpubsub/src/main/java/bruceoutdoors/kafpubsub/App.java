package bruceoutdoors.kafpubsub;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ManagedChannel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.api.gax.rpc.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger log = LoggerFactory.getLogger("KafPubSub");

    public static final String INPUT_TOPIC = System.getenv().getOrDefault("INPUT_TOPIC", "dbserver1.inventory.customers");
    public static final String PROJECT_ID = System.getenv().getOrDefault("PROJECT_ID", "crafty-apex-264713");

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", "kafpubsub"));
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, System.getenv().getOrDefault("AUTO_OFFSET_RESET_CONFIG", "latest"));
        return props;
    }

    public static Publisher initPubSubPub() throws IOException {
        final TopicName topic = TopicName.of(PROJECT_ID, INPUT_TOPIC);
        final Publisher.Builder pubBuilder = Publisher.newBuilder(topic);
        TopicAdminClient topicAdminClient;

        final String emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        if (emulatorHost != null) {
            final ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost)
                                                                .usePlaintext()
                                                                .build();
            final TransportChannelProvider channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            pubBuilder.setChannelProvider(channelProvider)
                      .setCredentialsProvider(NoCredentialsProvider.create());
            topicAdminClient = TopicAdminClient.create(TopicAdminSettings.newBuilder()
                                                                         .setTransportChannelProvider(channelProvider)
                                                                         .setCredentialsProvider(NoCredentialsProvider.create())
                                                                         .build());
        } else {
            topicAdminClient = TopicAdminClient.create();
        }

        final Publisher publisher = pubBuilder.build();

        try {
            topicAdminClient.createTopic(topic);
            log.info("PubSub Topic created: " + topic.getTopic());
        } catch (final AlreadyExistsException e) {
            log.info("Topic \"" + topic.getTopic() + "\" already exists. Skipping creation.");
        }

        return publisher;
    }

    public static void main(final String[] args) throws IOException {
        final Publisher publisher = initPubSubPub();

        final Properties props = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<byte[], byte[]> source = builder.stream(INPUT_TOPIC);

        source.foreach((k, v) -> {
            final ByteString kafkaVal = ByteString.copyFrom(v);
            final PubsubMessage msg = PubsubMessage.newBuilder()
                                                   .setData(kafkaVal)
                                                   .build();
            publisher.publish(msg);
        });

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            log.info("Publish Kafka values to pubsub...");
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
