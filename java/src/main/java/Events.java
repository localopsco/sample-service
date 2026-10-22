import java.util.List;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Events {
    static Properties kafkaProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_BROKERS"));
        props.put("security.protocol", "SASL_SSL");

        if ("OAUTHBEARER".equals(System.getenv("KAFKA_SASL_MECHANISM"))) {
            // Serverless: sign a token with the service's IAM role
            System.setProperty("aws.region", System.getenv("KAFKA_REGION"));
            String handler = "software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler";
            props.put("sasl.mechanism", "OAUTHBEARER");
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            props.put("sasl.login.callback.handler.class", handler);
            props.put("sasl.client.callback.handler.class", handler);
        } else {
            // Provisioned: username & password
            props.put("sasl.mechanism", "SCRAM-SHA-512");
            props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                System.getenv("KAFKA_USERNAME"), System.getenv("KAFKA_PASSWORD")));
        }
        return props;
    }

    public static void main(String[] args) throws Exception {
        Properties props = kafkaProperties();

        try (Admin admin = Admin.create(props)) {
            if (!admin.listTopics().names().get().contains("events")) {
                // Optional.empty() uses the cluster's default replication; serverless only accepts that.
                admin.createTopics(List.of(new NewTopic("events", Optional.of(3), Optional.empty()))).all().get();
            }
        }

        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>("events", "hello")).get();
        }
    }
}
