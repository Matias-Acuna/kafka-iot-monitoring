package com.kafkaiot.measurement.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaiot.measurement.io.SensorResultCsvWriter;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.metrics.MeasurementAggregator.ReceivedResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Polls results and alerts, updating measurements on the calling thread. */
public final class MeasurementKafkaConsumer {
    private static final String RESULTS_TOPIC = "sensor-results";
    private static final String ALERTS_TOPIC = "sensor-alerts";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final Consumer<String, String> consumer;
    private final MeasurementAggregator aggregator;
    private final SensorResultCsvWriter writer;
    private final SensorResultListener resultListener = new SensorResultListener();
    private final SensorAlertListener alertListener = new SensorAlertListener();
    private final AtomicBoolean started = new AtomicBoolean();
    private final Object lifecycleLock = new Object();
    private volatile boolean shutdownRequested;
    private boolean closed;

    public MeasurementKafkaConsumer(Properties properties, MeasurementAggregator aggregator) {
        this(properties, aggregator, null);
    }

    /** The optional writer is owned by the caller and must outlive run(). */
    public MeasurementKafkaConsumer(Properties properties, MeasurementAggregator aggregator,
                                    SensorResultCsvWriter writer) {
        Objects.requireNonNull(properties, "properties must not be null");
        this.aggregator = Objects.requireNonNull(aggregator, "aggregator must not be null");
        this.writer = writer;
        consumer = new KafkaConsumer<>(properties);
    }

    // Allows polling and cleanup tests without a broker or a real Kafka client.
    MeasurementKafkaConsumer(Consumer<String, String> consumer, MeasurementAggregator aggregator,
                             SensorResultCsvWriter writer) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.aggregator = Objects.requireNonNull(aggregator, "aggregator must not be null");
        this.writer = writer;
    }

    /** Runs once, blocking until shutdown or failure, and closes on this thread. */
    public void run() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Measurement Kafka consumer can only run once");
        }
        Throwable processingFailure = null;
        try {
            if (!shutdownRequested) {
                consumer.subscribe(List.of(RESULTS_TOPIC, ALERTS_TOPIC));
            }
            while (!shutdownRequested) {
                for (ConsumerRecord<String, String> record : consumer.poll(POLL_TIMEOUT)) {
                    handleRecord(record);
                }
            }
        } catch (WakeupException exception) {
            if (!shutdownRequested) {
                processingFailure = exception;
                throw exception;
            }
        } catch (RuntimeException | Error exception) {
            processingFailure = exception;
            throw exception;
        } finally {
            synchronized (lifecycleLock) {
                closed = true;
                try {
                    consumer.close();
                } catch (RuntimeException | Error closeException) {
                    if (processingFailure == null) {
                        throw closeException;
                    }
                    if (processingFailure != closeException) {
                        processingFailure.addSuppressed(closeException);
                    }
                }
            }
        }
    }

    /** May be called from another thread; run() must be invoked to close the consumer. */
    public void shutdown() {
        synchronized (lifecycleLock) {
            shutdownRequested = true;
            if (!closed) {
                consumer.wakeup();
            }
        }
    }

    private void handleRecord(ConsumerRecord<String, String> record) {
        try {
            switch (record.topic()) {
                case RESULTS_TOPIC -> {
                    long resultReceivedAt = System.currentTimeMillis();
                    ReceivedResult received = aggregator.registerResult(
                            resultListener.onMessage(record.value()), resultReceivedAt);
                    if (writer != null) {
                        writer.writeObservation(received);
                    }
                }
                case ALERTS_TOPIC -> aggregator.registerAlert(alertListener.onMessage(record.value()));
                default -> throw new IllegalArgumentException("Unexpected topic: " + record.topic());
            }
        } catch (JsonProcessingException | RuntimeException exception) {
            throw new IllegalStateException("Failed to process record from topic " + record.topic()
                    + ", partition " + record.partition() + ", offset " + record.offset(), exception);
        } catch (IOException exception) {
            throw new IllegalStateException("Failed to write result CSV for topic " + record.topic()
                    + ", partition " + record.partition() + ", offset " + record.offset(), exception);
        }
    }
}
