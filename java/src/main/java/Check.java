import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

// Runs the docs example (Events.java, copied verbatim from the MSK docs page),
// then consumes the topic to prove the published message arrived. Served by
// App at GET /check.
public class Check {
    static synchronized String runCheck() {
        long started = System.currentTimeMillis();
        StringBuilder out = new StringBuilder();
        out.append("time:      ").append(Instant.now()).append('\n');
        out.append("mechanism: ").append(System.getenv("KAFKA_SASL_MECHANISM")).append('\n');
        out.append("region:    ").append(System.getenv("KAFKA_REGION")).append('\n');
        out.append("brokers:   ").append(System.getenv("KAFKA_BROKERS")).append("\n\n");

        try {
            Events.main(new String[0]);
            out.append("ok    Events.main: topic 'events' exists and 'hello' was published\n");

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
                            out.append(String.format("ok    consumed 'hello' from partition %d offset %d%n", r.partition(), r.offset()));
                            out.append(String.format("%nPASS in %d ms%n", System.currentTimeMillis() - started));
                            return out.toString();
                        }
                    }
                }
            }
            out.append("\nFAIL: published 'hello' but did not consume it within 60s\n");
        } catch (Throwable e) {
            out.append("\nFAIL: ").append(e).append('\n');
            for (Throwable c = e.getCause(); c != null; c = c.getCause()) {
                out.append("  caused by: ").append(c).append('\n');
            }
        }
        return out.toString();
    }
}
