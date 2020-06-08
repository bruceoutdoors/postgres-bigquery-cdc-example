package bruceoutdoors.kafpubsub;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminClient;
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

public class App {
    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String PROJECT_ID = "crafty-apex-264713";

    static Properties getStreamsConfig() {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafpubsub");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());

        // setting offset reset to earliest so that we can re-run the demo code with the
        // same pre-loaded data
        // Note: To re-run the demo, you need to use the offset reset tool:
        // https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    public static void main(String[] args) throws IOException
    {
        TopicName topic = TopicName.of(PROJECT_ID, INPUT_TOPIC);
        Publisher.Builder pubBuilder = Publisher.newBuilder(topic);

        String emulatorHost = System.getenv("PUBSUB_EMULATOR_HOST");
        if (emulatorHost != null) {
            ManagedChannel channel = ManagedChannelBuilder.forTarget(emulatorHost)
                    .usePlaintext()
                    .build();
            TransportChannelProvider channelProvider =
                    FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
            pubBuilder.setChannelProvider(channelProvider)
                    .setCredentialsProvider(NoCredentialsProvider.create());
        }

        final Publisher publisher = pubBuilder.build();

        TopicAdminClient topicAdminClient = TopicAdminClient.create();
        topicAdminClient.createTopic(topic);

        final Properties props = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<byte[], byte[]> source = builder.stream(INPUT_TOPIC);

        source.foreach((k, v) -> {
            ByteString kafkaVal = ByteString.copyFrom(v);
            PubsubMessage msg = PubsubMessage.newBuilder().setData(kafkaVal).build();
            publisher.publish(msg);
        });

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Publish Kafka values to pubsub...");
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
