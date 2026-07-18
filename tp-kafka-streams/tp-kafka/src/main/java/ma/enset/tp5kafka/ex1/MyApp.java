package ma.enset.tp5kafka.ex1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MyApp {

    public static void main(String[] args) {

        // Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-cleaner-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Lecture du topic
        KStream<String, String> sourceStream = builder.stream("text-input");

        // Nettoyage
        KStream<String, String> cleanedStream =
                sourceStream.mapValues(TextCleanerApp::clean);

        // Messages valides
        KStream<String, String> validStream =
                cleanedStream.filter((key, value) -> TextCleanerApp.isValid(value));

        // Messages invalides
        KStream<String, String> invalidStream =
                cleanedStream.filter((key, value) -> !TextCleanerApp.isValid(value));

        // Envoi vers les topics
        validStream.to("text-clean");
        invalidStream.mapValues(value -> "Message rejeté : " + value)
                .to("text-dead-letter");

        // Démarrage
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        try {
            streams.start();
            System.out.println("TextCleanerApp is running...");
            latch.await();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }

        System.exit(0);
    }
}