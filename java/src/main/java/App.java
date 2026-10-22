import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

// MSK check page, the Java twin of the Node.js /kafka page: cluster status, a
// publish form, a background consumer showing what it receives, and a
// publish -> consume round trip.
public class App {
    static final String TOPIC = env("KAFKA_TOPIC", "lops-msk-check");
    static final int KEEP = 50;

    static final Deque<Map<String, Object>> received = new ArrayDeque<>();
    static final Map<String, CompletableFuture<String>> waiters = new ConcurrentHashMap<>();
    static volatile String consumerState = "not started";
    static volatile String consumerError = null;

    static Admin admin;
    static KafkaProducer<String, String> producer;

    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    static boolean configured() {
        return !env("KAFKA_BROKERS", "").isEmpty();
    }

    // Same settings as the docs example, with the protocol taken from the
    // exported $securityProtocol so the page can also run against a local
    // PLAINTEXT broker.
    static Properties props() {
        Properties p = new Properties();
        p.put("bootstrap.servers", env("KAFKA_BROKERS", ""));
        String protocol = env("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        p.put("security.protocol", protocol);
        if (!protocol.startsWith("SASL")) {
            return p;
        }
        String mechanism = env("KAFKA_SASL_MECHANISM", "");
        if (mechanism.equals("OAUTHBEARER") || mechanism.equals("AWS_MSK_IAM")) {
            if (System.getenv("KAFKA_REGION") != null) {
                System.setProperty("aws.region", System.getenv("KAFKA_REGION"));
            }
            String handler = "software.amazon.msk.auth.iam.IAMOAuthBearerLoginCallbackHandler";
            p.put("sasl.mechanism", "OAUTHBEARER");
            p.put("sasl.jaas.config", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
            p.put("sasl.login.callback.handler.class", handler);
            p.put("sasl.client.callback.handler.class", handler);
        } else {
            p.put("sasl.mechanism", "SCRAM-SHA-512");
            p.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                env("KAFKA_USERNAME", ""), env("KAFKA_PASSWORD", "")));
        }
        return p;
    }

    static synchronized Admin admin() {
        if (admin == null) {
            admin = Admin.create(props());
        }
        return admin;
    }

    static synchronized KafkaProducer<String, String> producer() {
        if (producer == null) {
            Properties p = props();
            p.put("key.serializer", StringSerializer.class.getName());
            p.put("value.serializer", StringSerializer.class.getName());
            p.put("delivery.timeout.ms", "30000");
            p.put("request.timeout.ms", "15000");
            producer = new KafkaProducer<>(p);
        }
        return producer;
    }

    static void ensureTopic() throws Exception {
        if (admin().listTopics().names().get(20, TimeUnit.SECONDS).contains(TOPIC)) {
            return;
        }
        try {
            // Optional.empty() takes the cluster's default replication; serverless only accepts that.
            admin().createTopics(List.of(new NewTopic(TOPIC, Optional.of(3), Optional.empty()))).all().get(30, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw e;
            }
        }
    }

    static void consumeLoop() {
        if (!configured()) {
            consumerState = "KAFKA_BROKERS unset, no msk dependency";
            return;
        }
        while (true) {
            try {
                consumerState = "connecting";
                ensureTopic();
                Properties p = props();
                // One group per pod, so every replica sees every message and a
                // round trip is always consumed by the pod that published it.
                p.put("group.id", env("KAFKA_GROUP", "lops-msk-check") + "-" + InetAddress.getLocalHost().getHostName());
                p.put("auto.offset.reset", "latest");
                p.put("key.deserializer", StringDeserializer.class.getName());
                p.put("value.deserializer", StringDeserializer.class.getName());
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
                    consumer.subscribe(List.of(TOPIC));
                    consumerState = "running";
                    consumerError = null;
                    while (true) {
                        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
                            Map<String, Object> m = new HashMap<>();
                            m.put("at", Instant.now().toString());
                            m.put("partition", r.partition());
                            m.put("offset", r.offset());
                            m.put("key", r.key());
                            m.put("value", r.value());
                            synchronized (received) {
                                received.addFirst(m);
                                while (received.size() > KEEP) received.removeLast();
                            }
                            CompletableFuture<String> w = r.key() == null ? null : waiters.remove(r.key());
                            if (w != null) w.complete(r.partition() + "/" + r.offset());
                        }
                    }
                }
            } catch (Throwable e) {
                consumerState = "failed, retrying in 15s";
                consumerError = rootMessage(e);
                System.err.println("kafka consumer failed: " + e);
                try { Thread.sleep(15_000); } catch (InterruptedException ie) { return; }
            }
        }
    }

    static RecordMetadata publish(String key, String value) throws Exception {
        return producer().send(new ProducerRecord<>(TOPIC, key, value)).get(35, TimeUnit.SECONDS);
    }

    static Map<String, Object> roundtrip() throws Exception {
        String key = "roundtrip-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 6);
        CompletableFuture<String> seen = new CompletableFuture<>();
        waiters.put(key, seen);
        long started = System.currentTimeMillis();
        try {
            RecordMetadata sent = publish(key, "roundtrip check at " + Instant.now());
            String consumed;
            try {
                consumed = seen.get(20, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new Exception("published but not consumed within 20s (consumer: " + consumerState + ")");
            }
            Map<String, Object> r = new HashMap<>();
            r.put("key", key);
            r.put("published", sent.partition() + "/" + sent.offset());
            r.put("consumed", consumed);
            r.put("ms", System.currentTimeMillis() - started);
            return r;
        } finally {
            waiters.remove(key);
        }
    }

    static Map<String, Object> status() {
        Map<String, Object> s = new HashMap<>();
        if (!configured()) {
            s.put("error", "KAFKA_BROKERS unset, no msk dependency");
            return s;
        }
        try {
            DescribeClusterResult c = admin().describeCluster();
            List<String> brokers = new ArrayList<>();
            for (Node n : c.nodes().get(20, TimeUnit.SECONDS)) brokers.add(n.host() + ":" + n.port());
            s.put("brokers", brokers);
            TopicDescription t = admin().describeTopics(List.of(TOPIC)).allTopicNames().get(20, TimeUnit.SECONDS).get(TOPIC);
            s.put("topic", TOPIC);
            s.put("partitions", t.partitions().size());
            Map<TopicPartition, OffsetSpec> earliest = new HashMap<>(), latest = new HashMap<>();
            t.partitions().forEach(p -> {
                earliest.put(new TopicPartition(TOPIC, p.partition()), OffsetSpec.earliest());
                latest.put(new TopicPartition(TOPIC, p.partition()), OffsetSpec.latest());
            });
            var lo = admin().listOffsets(earliest).all().get(20, TimeUnit.SECONDS);
            var hi = admin().listOffsets(latest).all().get(20, TimeUnit.SECONDS);
            long retained = 0;
            for (var tp : hi.keySet()) retained += hi.get(tp).offset() - lo.get(tp).offset();
            s.put("messagesInTopic", retained);
        } catch (Throwable e) {
            s.put("error", rootMessage(e));
        }
        return s;
    }

    static String rootMessage(Throwable e) {
        Throwable c = e;
        while (c.getCause() != null && c.getCause() != c) c = c.getCause();
        return c == e ? String.valueOf(e) : e + " (cause: " + c + ")";
    }

    // ---- HTTP -----------------------------------------------------------

    static String esc(Object o) {
        if (o == null) return "";
        return o.toString().replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;").replace("'", "&#39;");
    }

    static String json(Object o) {
        if (o == null) return "null";
        if (o instanceof Number || o instanceof Boolean) return o.toString();
        if (o instanceof Map<?, ?> m) {
            return m.entrySet().stream().map(e -> json(e.getKey().toString()) + ":" + json(e.getValue()))
                .collect(Collectors.joining(",", "{", "}"));
        }
        if (o instanceof Iterable<?> it) {
            List<String> parts = new ArrayList<>();
            for (Object x : it) parts.add(json(x));
            return "[" + String.join(",", parts) + "]";
        }
        StringBuilder b = new StringBuilder("\"");
        for (char ch : o.toString().toCharArray()) {
            switch (ch) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (ch < 0x20) b.append(String.format("\\u%04x", (int) ch));
                    else b.append(ch);
                }
            }
        }
        return b.append('"').toString();
    }

    static Map<String, String> query(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null || raw.isEmpty()) return m;
        for (String pair : raw.split("&")) {
            String[] kv = pair.split("=", 2);
            m.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "");
        }
        return m;
    }

    static void send(HttpExchange ex, int code, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    static void redirect(HttpExchange ex, String param, String text) throws IOException {
        ex.getResponseHeaders().set("Location", "/kafka?" + param + "=" + URLEncoder.encode(text, StandardCharsets.UTF_8));
        ex.sendResponseHeaders(303, -1);
        ex.close();
    }

    static boolean wantsJson(HttpExchange ex) {
        String ct = ex.getRequestHeaders().getFirst("Content-Type");
        return (ct != null && ct.contains("json")) || query(ex.getRequestURI().getRawQuery()).containsKey("json");
    }

    static String row(String k, Object v) {
        return "<tr><td style=\"padding:6px 10px;font-weight:bold\">" + esc(k) + "</td><td style=\"padding:6px 10px\">" + esc(v) + "</td></tr>";
    }

    static String page(Map<String, Object> st, String ok, String err) {
        String mechanism = env("KAFKA_SASL_MECHANISM", "(unset)");
        boolean iam = mechanism.equals("OAUTHBEARER") || mechanism.equals("AWS_MSK_IAM");
        StringBuilder msgs = new StringBuilder();
        synchronized (received) {
            for (Map<String, Object> m : received) {
                msgs.append("<tr><td style=\"padding:4px 8px\">").append(esc(m.get("at")))
                    .append("</td><td style=\"padding:4px 8px\">").append(esc(m.get("partition") + "/" + m.get("offset")))
                    .append("</td><td style=\"padding:4px 8px\">").append(esc(m.get("key")))
                    .append("</td><td style=\"padding:4px 8px\">").append(esc(m.get("value"))).append("</td></tr>");
            }
        }
        String flash = ok != null ? "<p style=\"padding:10px;background:#e6f7ea\">" + esc(ok) + "</p>"
            : err != null ? "<p style=\"padding:10px;background:#fdecea\">" + esc(err) + "</p>" : "";
        String cluster = st.containsKey("error") ? row("Cluster", "unreachable: " + st.get("error"))
            : row("Cluster brokers", String.join(", ", (List<String>) st.get("brokers")))
            + row("Topic", st.get("topic") + ", " + st.get("partitions") + " partitions, " + st.get("messagesInTopic") + " messages retained");
        return "<!doctype html><html><head><meta charset=\"utf-8\"><title>MSK check (Java)</title>"
            + "<meta http-equiv=\"refresh\" content=\"10\"></head>"
            + "<body style=\"font-family:sans-serif;max-width:900px;margin:30px auto;padding:0 16px\">"
            + "<h1>MSK check <small style=\"font-weight:normal;color:#888\">Java, kafka-clients</small></h1>" + flash
            + "<table style=\"border-collapse:collapse\">"
            + row("Mechanism", mechanism)
            + row("Security protocol", env("KAFKA_SECURITY_PROTOCOL", "(unset)"))
            + row("Bootstrap brokers", env("KAFKA_BROKERS", "(unset)"))
            + row("Region", env("KAFKA_REGION", "(unset)"))
            + row("Username", iam ? "n/a (IAM)" : env("KAFKA_USERNAME", "(unset)"))
            + row("Password", iam ? "n/a (IAM)" : (System.getenv("KAFKA_PASSWORD") != null ? "set" : "(unset)"))
            + row("Consumer", consumerState + (consumerError != null ? " (" + consumerError + ")" : ""))
            + cluster + "</table>"
            + "<form method=\"post\" action=\"/kafka/publish\" style=\"margin:20px 0\">"
            + "<input name=\"message\" placeholder=\"message\" style=\"width:60%;padding:6px\" required> "
            + "<button type=\"submit\" style=\"padding:6px 12px\">Publish</button></form>"
            + "<form method=\"post\" action=\"/kafka/roundtrip\" style=\"margin:0 0 20px\">"
            + "<button type=\"submit\" style=\"padding:6px 12px\">Run publish &rarr; consume round trip</button></form>"
            + "<h2>Last " + KEEP + " consumed messages <small style=\"font-weight:normal;color:#888\">(page refreshes every 10s)</small></h2>"
            + "<table style=\"border-collapse:collapse;width:100%\"><tr><th align=\"left\">Received</th><th align=\"left\">Partition/offset</th>"
            + "<th align=\"left\">Key</th><th align=\"left\">Value</th></tr>"
            + (msgs.length() > 0 ? msgs : "<tr><td colspan=\"4\" style=\"padding:8px;color:#888\">nothing consumed yet</td></tr>")
            + "</table><p style=\"color:#888\">JSON: <a href=\"/kafka/status\">/kafka/status</a> &middot; "
            + "<a href=\"/kafka/messages\">/kafka/messages</a> &middot; POST /kafka/roundtrip?json &middot; "
            + "<a href=\"/check\">/check</a> runs the docs example</p></body></html>";
    }

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(env("PORT", "3000"));
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(8));

        server.createContext("/health", ex -> send(ex, 200, "text/plain", "ok\n"));

        server.createContext("/check", ex -> {
            String result = Check.runCheck();
            System.out.print(result);
            send(ex, result.contains("\nPASS") ? 200 : 500, "text/plain", result);
        });

        server.createContext("/kafka", ex -> {
            String path = ex.getRequestURI().getPath();
            String method = ex.getRequestMethod();
            try {
                if (method.equals("GET") && (path.equals("/kafka") || path.equals("/kafka/"))) {
                    Map<String, String> q = query(ex.getRequestURI().getRawQuery());
                    send(ex, 200, "text/html", page(status(), q.get("ok"), q.get("err")));
                } else if (method.equals("GET") && path.equals("/kafka/status")) {
                    Map<String, Object> s = status();
                    s.put("mechanism", env("KAFKA_SASL_MECHANISM", ""));
                    s.put("consumer", consumerState);
                    s.put("consumerError", consumerError);
                    send(ex, s.containsKey("error") ? 503 : 200, "application/json", json(s));
                } else if (method.equals("GET") && path.equals("/kafka/messages")) {
                    List<Map<String, Object>> copy;
                    synchronized (received) { copy = new ArrayList<>(received); }
                    send(ex, 200, "application/json", json(copy));
                } else if (method.equals("POST") && path.equals("/kafka/publish")) {
                    boolean j = wantsJson(ex);
                    String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    String message = query(body).getOrDefault("message", "");
                    if (message.isBlank()) message = "hello at " + Instant.now();
                    try {
                        RecordMetadata m = publish("manual-" + System.currentTimeMillis(), message);
                        if (j) send(ex, 200, "application/json", json(Map.of("partition", m.partition(), "offset", m.offset())));
                        else redirect(ex, "ok", "published to partition " + m.partition() + " at offset " + m.offset());
                    } catch (Throwable e) {
                        if (j) send(ex, 500, "application/json", json(Map.of("error", rootMessage(e))));
                        else redirect(ex, "err", "publish failed: " + rootMessage(e));
                    }
                } else if (method.equals("POST") && path.equals("/kafka/roundtrip")) {
                    boolean j = wantsJson(ex);
                    try {
                        Map<String, Object> r = roundtrip();
                        if (j) { r.put("ok", true); send(ex, 200, "application/json", json(r)); }
                        else redirect(ex, "ok", "round trip ok in " + r.get("ms") + "ms: published to " + r.get("published") + ", consumed from " + r.get("consumed"));
                    } catch (Throwable e) {
                        if (j) send(ex, 500, "application/json", json(Map.of("ok", false, "error", rootMessage(e))));
                        else redirect(ex, "err", "round trip failed: " + rootMessage(e));
                    }
                } else {
                    send(ex, 404, "text/plain", "not found\n");
                }
            } catch (Throwable e) {
                send(ex, 500, "text/plain", "error: " + rootMessage(e) + "\n");
            }
        });

        server.createContext("/", ex -> {
            if (ex.getRequestURI().getPath().equals("/")) {
                ex.getResponseHeaders().set("Location", "/kafka");
                ex.sendResponseHeaders(302, -1);
                ex.close();
            } else {
                send(ex, 404, "text/plain", "not found\n");
            }
        });

        server.start();
        System.out.println("MSK check (Java) listening on port " + port + "; open /kafka");

        Thread consumer = new Thread(App::consumeLoop, "kafka-consumer");
        consumer.setDaemon(true);
        consumer.start();
    }
}
