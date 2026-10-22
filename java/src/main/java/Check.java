import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

// Runs the docs example (Events.java, copied verbatim from the MSK docs page),
// then consumes the topic to prove the published message arrived.
public class Check {
    public static void main(String[] args) throws Exception {
        long started = System.currentTimeMillis();
        System.out.println("mechanism=" + System.getenv("KAFKA_SASL_MECHANISM") + " brokers=" + System.getenv("KAFKA_BROKERS"));

        Events.main(args);
        System.out.println("Events.main ok: topic 'events' exists and 'hello' was published");

        Properties props = Events.kafkaProperties();
        props.put("group.id", "msk-java-check-" + UUID.randomUUID());
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("events"));
            long deadline = System.currentTimeMillis() + 60_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(2))) {
                    if ("hello".equals(r.value()) && r.timestamp() >= started - 5_000) {
                        System.out.printf("PASS: consumed 'hello' from partition %d offset %d, %d ms after start%n",
                            r.partition(), r.offset(), System.currentTimeMillis() - started);
                        return;
                    }
                }
            }
        }
        System.out.println("FAIL: published 'hello' but did not consume it within 60s");
        System.exit(1);
    }
}
