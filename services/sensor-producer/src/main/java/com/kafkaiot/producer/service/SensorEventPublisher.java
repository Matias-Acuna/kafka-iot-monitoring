package com.kafkaiot.producer.service;

import com.kafkaiot.producer.config.ProducerKafkaConfig;
import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.io.ProducerEventCsvWriter.Status;
import com.kafkaiot.producer.model.SensorEvent;
import com.kafkaiot.producer.service.WorkloadRunner.Phase;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes sensor events asynchronously to Kafka.
 */
public final class SensorEventPublisher implements AutoCloseable {
    private static final String TOPIC = "sensor-events";
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    private final Producer<String, String> producer;
    private final AtomicReference<IOException> observationFailure = new AtomicReference<>();

    // Allows broker-free lifecycle tests using Kafka's MockProducer.
    SensorEventPublisher(Producer<String, String> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

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
        publish(event, callback, null, null);
    }

    /**
     * Optionally records observations using the exact event serialized to Kafka.
     * CSV failures never replace Kafka outcomes; close() surfaces them after draining.
     * The caller owns the writer and must close it after this publisher.
     */
    public void publish(SensorEvent event, Callback callback, Phase phase, ProducerEventCsvWriter writer) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(callback, "callback must not be null");
        if (writer != null) {
            Objects.requireNonNull(phase, "phase");
        }
        if (!Double.isFinite(event.temperature())) {
            throw new IllegalArgumentException("temperature must be finite for JSON");
        }

        SensorEvent sentEvent = new SensorEvent(event.eventId(), event.runId(),
                event.deviceId(), event.temperature(), event.eventTime(),
                System.currentTimeMillis());
        ProducerRecord<String, String> record = new ProducerRecord<>(
                TOPIC, sentEvent.deviceId(), toJson(sentEvent));
        if (writer == null) {
            producer.send(record, callback);
            return;
        }
        observe(writer, sentEvent, phase, Status.ATTEMPTED, null, null, System.currentTimeMillis());
        AtomicBoolean outcomeDelivered = new AtomicBoolean();
        Callback observedCallback = (metadata, exception) -> {
            long recordedAt = System.currentTimeMillis();
            if (outcomeDelivered.compareAndSet(false, true)) {
                observe(writer, sentEvent, phase, exception == null ? Status.CONFIRMED : Status.FAILED,
                        metadata, exception, recordedAt);
                callback.onCompletion(metadata, exception);
            }
        };
        try {
            producer.send(record, observedCallback);
        } catch (RuntimeException sendException) {
            if (outcomeDelivered.compareAndSet(false, true)) {
                observe(writer, sentEvent, phase, Status.FAILED, null, sendException, System.currentTimeMillis());
            }
            throw sendException;
        }
    }

    private void observe(ProducerEventCsvWriter writer, SensorEvent event, Phase phase, Status status,
                         RecordMetadata metadata, Exception exception, long recordedAt) {
        try {
            writer.writeObservation(event, phase, recordedAt, status,
                    metadata != null && metadata.partition() >= 0 ? metadata.partition() : null,
                    metadata != null && metadata.hasOffset() ? metadata.offset() : null,
                    exception == null ? null : exception.toString());
        } catch (IOException ioException) {
            if (observationFailure.compareAndSet(null, ioException)) {
                System.err.println("Producer observation CSV write failed: " + ioException);
            }
        }
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
        try {
            producer.close();
        } catch (RuntimeException exception) {
            IOException failure = observationFailure.get();
            if (failure != null) {
                exception.addSuppressed(new UncheckedIOException("Producer observation CSV write failed", failure));
            }
            throw exception;
        }
        IOException failure = observationFailure.get();
        if (failure != null) {
            throw new UncheckedIOException("Producer observation CSV write failed", failure);
        }
    }
}
