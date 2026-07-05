package com.cinemaabyss.events;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EventsServiceApplication {
    private static final String MOVIE_TOPIC = "movie-events";
    private static final String USER_TOPIC = "user-events";
    private static final String PAYMENT_TOPIC = "payment-events";

    private final AppConfig config;
    private final KafkaProducer<String, String> producer;

    public EventsServiceApplication(AppConfig config) {
        this.config = config;
        this.producer = new KafkaProducer<>(producerProperties(config.kafkaBrokers()));
    }

    public static void main(String[] args) throws IOException {
        AppConfig config = AppConfig.fromEnvironment();
        EventsServiceApplication application = new EventsServiceApplication(config);

        for (String topic : List.of(MOVIE_TOPIC, USER_TOPIC, PAYMENT_TOPIC)) {
            Thread consumerThread = new Thread(() -> application.consume(topic), "kafka-consumer-" + topic);
            consumerThread.setDaemon(true);
            consumerThread.start();
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(config.port()), 0);
        server.createContext("/api/events/health", application::handleHealth);
        server.createContext("/api/events/movie", exchange -> application.handleEvent(exchange, "movie", MOVIE_TOPIC));
        server.createContext("/api/events/user", exchange -> application.handleEvent(exchange, "user", USER_TOPIC));
        server.createContext("/api/events/payment", exchange -> application.handleEvent(exchange, "payment", PAYMENT_TOPIC));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(application.producer::close));
        System.out.printf("Starting Java events service on port %d with Kafka brokers %s%n",
                config.port(),
                config.kafkaBrokers());
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        sendJson(exchange, 200, "{\"status\":true}");
    }

    private void handleEvent(HttpExchange exchange, String eventType, String topic) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            sendText(exchange, 405, "Method not allowed");
            return;
        }

        String payload = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (payload.isBlank()) {
            sendText(exchange, 400, "Request body is required");
            return;
        }

        String eventId = eventType + "-" + UUID.randomUUID();
        String event = """
                {"id":"%s","type":"%s","topic":"%s","payload":%s,"created_at":"%s"}
                """.formatted(eventId, eventType, topic, payload, Instant.now());

        try {
            publishWithRetry(topic, eventId, event);
        } catch (Exception error) {
            System.err.printf("Failed to publish %s event to %s: %s%n", eventType, topic, error.getMessage());
            sendJson(exchange, 503, "{\"error\":\"Failed to publish event\"}");
            return;
        }

        System.out.printf("Published %s event %s to %s: %s%n", eventType, eventId, topic, payload);
        sendJson(exchange, 201, """
                {"status":"success","event_id":"%s","topic":"%s"}
                """.formatted(eventId, topic));
    }

    private void publishWithRetry(String topic, String key, String value) throws Exception {
        Exception lastError = null;
        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                Future<?> future = producer.send(new ProducerRecord<>(topic, key, value));
                future.get();
                producer.flush();
                return;
            } catch (Exception error) {
                lastError = error;
                System.err.printf("Kafka publish attempt %d for topic %s failed: %s%n",
                        attempt,
                        topic,
                        error.getMessage());
                Thread.sleep(attempt * 1000L);
            }
        }
        throw lastError;
    }

    private void consume(String topic) {
        Properties properties = consumerProperties(config.kafkaBrokers(), "events-service-" + topic);
        while (true) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
                consumer.subscribe(List.of(topic));
                while (true) {
                    for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                        System.out.printf("Processed Kafka event from %s partition=%d offset=%d key=%s value=%s%n",
                                topic,
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value());
                    }
                }
            } catch (Exception error) {
                System.err.printf("Consumer for topic %s will reconnect after error: %s%n", topic, error.getMessage());
                sleepQuietly(2000);
            }
        }
    }

    private void sleepQuietly(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    private static Properties producerProperties(String brokers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.RETRIES_CONFIG, "3");
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
        return properties;
    }

    private static Properties consumerProperties(String brokers, String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return properties;
    }

    private void sendJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        send(exchange, statusCode, body);
    }

    private void sendText(HttpExchange exchange, int statusCode, String body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "text/plain");
        send(exchange, statusCode, body);
    }

    private void send(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] responseBody = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBody.length);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.write(responseBody);
        }
    }
}
