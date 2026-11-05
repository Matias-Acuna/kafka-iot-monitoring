package com.kafkaiot.producer.service;

import com.kafkaiot.producer.config.ProducerKafkaConfig;
import com.kafkaiot.producer.model.SensorEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Objects;

/**
 * Publishes sensor events asynchronously to Kafka.
 */
public final class SensorEventPublisher implements AutoCloseable {
    private static final String TOPIC = "sensor-events";
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final KafkaProducer<String, String> producer;

    public SensorEventPublisher(String bootstrapServers) {
        producer = new KafkaProducer<>(ProducerKafkaConfig.createProperties(bootstrapServers));
    }

    /**
     * Uses explicit B2 overrides for acks and idempotence.
     */
    public SensorEventPublisher(String bootstrapServers, String acks,
                                boolean enableIdempotence) {
        producer = new KafkaProducer<>(ProducerKafkaConfig.createProperties(
                bootstrapServers, acks, enableIdempotence));
    }

    /**
     * Reports asynchronous completion through the callback. Validation and
     * synchronous Kafka send failures are thrown directly to the caller.
     */
    public void publish(SensorEvent event, Callback callback) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(callback, "callback must not be null");
        if (!Double.isFinite(event.temperature())) {
            throw new IllegalArgumentException("temperature must be finite for JSON");
        }

        SensorEvent sentEvent = new SensorEvent(event.eventId(), event.runId(),
                event.deviceId(), event.temperature(), event.eventTime(),
                System.currentTimeMillis());
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC, sentEvent.deviceId(), toJson(sentEvent));
        producer.send(record, callback);
    }

    private static String toJson(SensorEvent event) {
        return "{\"eventId\":" + jsonString(event.eventId())
                + ",\"runId\":" + jsonString(event.runId())
                + ",\"deviceId\":" + jsonString(event.deviceId())
                + ",\"temperature\":" + event.temperature()
                + ",\"eventTime\":" + event.eventTime()
                + ",\"sentAt\":" + event.sentAt() + "}";
    }

    private static String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder json = new StringBuilder(value.length() + 2);
        json.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (c < 0x20 || Character.isSurrogate(c)) {
                        json.append("\\u")
                                .append(HEX[(c >>> 12) & 0xf])
                                .append(HEX[(c >>> 8) & 0xf])
                                .append(HEX[(c >>> 4) & 0xf])
                                .append(HEX[c & 0xf]);
                    } else {
                        json.append(c);
                    }
                }
            }
        }
        return json.append('"').toString();
    }

    @Override
    public void close() {
        producer.close();
    }
}
