package com.kafkaiot.measurement.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaiot.measurement.io.SensorResultCsvWriter;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.metrics.MeasurementAggregator.ReceivedResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Collectors;
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
    private final java.util.function.Consumer<ReadinessEvidence> readinessSink;
    private final java.util.function.Consumer<ProgressEvidence> progressSink;
    private final java.util.function.LongSupplier nanoClock;
    private final String runIdFilter;
    private long ignoredForeignResults;
    private long ignoredForeignAlerts;
    private boolean ready;
    private Long lastProgressNs;
    private long successfulPolls;
    private long emptyPolls;
    private long polledRecords;
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
        this(properties, aggregator, writer, null);
    }

    /** Optional readiness snapshots are delivered on the polling thread before record handling. */
    public MeasurementKafkaConsumer(Properties properties, MeasurementAggregator aggregator,
                                    SensorResultCsvWriter writer,
                                    java.util.function.Consumer<ReadinessEvidence> readinessSink) {
        this(properties, aggregator, writer, readinessSink, null);
    }

    public MeasurementKafkaConsumer(Properties properties, MeasurementAggregator aggregator,
                                    SensorResultCsvWriter writer,
                                    java.util.function.Consumer<ReadinessEvidence> readinessSink,
                                    java.util.function.Consumer<ProgressEvidence> progressSink) {
        this(properties, aggregator, writer, readinessSink, progressSink, null);
    }

    /** Optional run isolation affects observations only; Kafka subscription and offsets are unchanged. */
    public MeasurementKafkaConsumer(Properties properties, MeasurementAggregator aggregator,
                                    SensorResultCsvWriter writer,
                                    java.util.function.Consumer<ReadinessEvidence> readinessSink,
                                    java.util.function.Consumer<ProgressEvidence> progressSink,
                                    String runIdFilter) {
        Objects.requireNonNull(properties, "properties must not be null");
        this.aggregator = Objects.requireNonNull(aggregator, "aggregator must not be null");
        this.writer = writer;
        this.readinessSink = readinessSink;
        this.progressSink = progressSink;
        this.nanoClock = System::nanoTime;
        this.runIdFilter = validateRunIdFilter(runIdFilter);
        consumer = new KafkaConsumer<>(properties);
    }

    // Allows polling and cleanup tests without a broker or a real Kafka client.
    MeasurementKafkaConsumer(Consumer<String, String> consumer, MeasurementAggregator aggregator,
                             SensorResultCsvWriter writer) {
        this(consumer, aggregator, writer, null);
    }

    MeasurementKafkaConsumer(Consumer<String, String> consumer, MeasurementAggregator aggregator,
                             SensorResultCsvWriter writer,
                             java.util.function.Consumer<ReadinessEvidence> readinessSink) {
        this(consumer, aggregator, writer, readinessSink, null);
    }

    MeasurementKafkaConsumer(Consumer<String, String> consumer, MeasurementAggregator aggregator,
                             SensorResultCsvWriter writer,
                             java.util.function.Consumer<ReadinessEvidence> readinessSink,
                             java.util.function.Consumer<ProgressEvidence> progressSink) {
        this(consumer, aggregator, writer, readinessSink, progressSink, System::nanoTime);
    }

    // Deterministic rate-limit tests; the production clock remains System.nanoTime().
    MeasurementKafkaConsumer(Consumer<String, String> consumer, MeasurementAggregator aggregator,
                             SensorResultCsvWriter writer,
                             java.util.function.Consumer<ReadinessEvidence> readinessSink,
                             java.util.function.Consumer<ProgressEvidence> progressSink,
                             java.util.function.LongSupplier nanoClock) {
        this(consumer, aggregator, writer, readinessSink, progressSink, nanoClock, null);
    }

    MeasurementKafkaConsumer(Consumer<String, String> consumer, MeasurementAggregator aggregator,
                             SensorResultCsvWriter writer,
                             java.util.function.Consumer<ReadinessEvidence> readinessSink,
                             java.util.function.Consumer<ProgressEvidence> progressSink,
                             java.util.function.LongSupplier nanoClock, String runIdFilter) {
        this.consumer = Objects.requireNonNull(consumer, "consumer must not be null");
        this.aggregator = Objects.requireNonNull(aggregator, "aggregator must not be null");
        this.writer = writer;
        this.readinessSink = readinessSink;
        this.progressSink = progressSink;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
        this.runIdFilter = validateRunIdFilter(runIdFilter);
    }

    private static String validateRunIdFilter(String value) {
        if (value != null && value.isBlank()) throw new IllegalArgumentException("runIdFilter must not be blank");
        return value;
    }

    /** Read on the polling thread after run() returns. Foreign deliveries are not experimental observations. */
    public long getIgnoredForeignResults() { return ignoredForeignResults; }
    public long getIgnoredForeignAlerts() { return ignoredForeignAlerts; }

    /** One assignment-time snapshot, not an assertion of continuing observation coverage. */
    public record ReadinessEvidence(String evidenceType, long observedAtEpochMs, String groupId,
                                    String status, List<Integer> expectedResultPartitions,
                                    List<PartitionPosition> assignedResultPartitions) {
        public ReadinessEvidence {
            expectedResultPartitions = List.copyOf(expectedResultPartitions);
            assignedResultPartitions = List.copyOf(assignedResultPartitions);
        }
    }

    public record PartitionPosition(String topic, int partition, long startingPosition) { }

    public sealed interface ProgressEvidence permits PollEvidence, AssignmentEvidence, LifecycleEvidence { }

    /** Counts cover actual successful polls since the previous progress report, not inferred time. */
    public record PollEvidence(String evidenceType, long observedAtEpochMs, String groupId, String status,
                               long successfulPolls, long emptyPolls, long recordCount,
                               boolean lastPollEmpty) implements ProgressEvidence { }

    /** assignedPartitions is Kafka's current callback-time snapshot, possibly including revoked partitions. */
    public record AssignmentEvidence(String evidenceType, long observedAtEpochMs, String groupId, String status,
                                     List<PartitionReference> affectedPartitions,
                                     List<PartitionReference> assignedPartitions) implements ProgressEvidence {
        public AssignmentEvidence {
            affectedPartitions = List.copyOf(affectedPartitions);
            assignedPartitions = List.copyOf(assignedPartitions);
        }
    }

    /** Actual component termination/closure; never a claim of complete experimental coverage. */
    public record LifecycleEvidence(String evidenceType, long observedAtEpochMs, String groupId,
                                    String status, boolean pollingInvoked,
                                    FailureDetail failure) implements ProgressEvidence { }

    public record FailureDetail(String type, String message, String cause, List<String> suppressed) {
        public static FailureDetail from(Throwable failure) {
            return failure == null ? null : new FailureDetail(failure.getClass().getName(), failure.getMessage(),
                    failure.getCause() == null ? null : failure.getCause().toString(),
                    java.util.Arrays.stream(failure.getSuppressed()).map(Throwable::toString).toList());
        }
    }

    private static Throwable preserveFailure(Throwable primary, Throwable additional) {
        if (primary == null) return additional;
        if (primary != additional) primary.addSuppressed(additional);
        return primary;
    }

    public record PartitionReference(String topic, int partition) { }

    private static List<PartitionReference> partitionReferences(Collection<TopicPartition> partitions) {
        return partitions.stream().sorted(Comparator.comparing(TopicPartition::topic)
                .thenComparingInt(TopicPartition::partition))
                .map(partition -> new PartitionReference(partition.topic(), partition.partition())).toList();
    }

    private void assignmentInterrupted(String type, Collection<TopicPartition> partitions) {
        if (partitions.stream().anyMatch(partition -> RESULTS_TOPIC.equals(partition.topic()))) {
            ready = false;
        }
        if (progressSink != null && !closed && !shutdownRequested) {
            emit(new AssignmentEvidence(type, System.currentTimeMillis(), consumer.groupMetadata().groupId(),
                    ready ? "READY" : "NOT_READY", partitionReferences(partitions),
                    partitionReferences(consumer.assignment())));
        }
    }

    private void reportPoll(int recordCount, long observedAtEpochMs) {
        if (progressSink == null || shutdownRequested) {
            return;
        }
        successfulPolls++;
        if (recordCount == 0) emptyPolls++;
        polledRecords += recordCount;
        long now = nanoClock.getAsLong();
        if (lastProgressNs == null || now - lastProgressNs >= 1_000_000_000L) {
            emit(new PollEvidence("MEASUREMENT_CONSUMER_POLL_PROGRESS", observedAtEpochMs,
                    consumer.groupMetadata().groupId(), ready ? "READY" : "NOT_READY",
                    successfulPolls, emptyPolls, polledRecords, recordCount == 0));
            lastProgressNs = now;
            successfulPolls = 0;
            emptyPolls = 0;
            polledRecords = 0;
        }
    }

    private void emit(ProgressEvidence evidence) {
        try {
            progressSink.accept(evidence);
        } catch (RuntimeException exception) {
            ready = false;
            throw new IllegalStateException("Failed to emit Measurement Consumer polling/assignment evidence", exception);
        }
    }

    private void reportReadiness() {
        if (shutdownRequested) {
            return;
        }
        ready = false;
        try {
            Set<TopicPartition> assigned = consumer.assignment().stream()
                    .filter(partition -> RESULTS_TOPIC.equals(partition.topic())).collect(Collectors.toSet());
            var metadata = consumer.partitionsFor(RESULTS_TOPIC, POLL_TIMEOUT);
            if (metadata == null || metadata.isEmpty()) {
                throw new IllegalStateException("Result-topic partition metadata is unavailable");
            }
            Set<TopicPartition> expected = metadata.stream().map(partition -> {
                if (!RESULTS_TOPIC.equals(partition.topic()) || partition.partition() < 0) {
                    throw new IllegalStateException("Invalid result-topic partition metadata");
                }
                return new TopicPartition(partition.topic(), partition.partition());
            }).collect(Collectors.toSet());
            var positions = assigned.stream().sorted(Comparator.comparingInt(TopicPartition::partition))
                    .map(partition -> {
                        long position = consumer.position(partition, POLL_TIMEOUT);
                        if (position < 0) {
                            throw new IllegalStateException("Invalid starting position for " + partition);
                        }
                        return new PartitionPosition(partition.topic(), partition.partition(), position);
                    }).toList();
            String groupId = consumer.groupMetadata().groupId();
            if (groupId == null || groupId.isBlank()) {
                throw new IllegalStateException("Consumer groupId is unavailable");
            }
            boolean verified = assigned.equals(expected);
            if (readinessSink != null) {
                readinessSink.accept(new ReadinessEvidence("MEASUREMENT_CONSUMER_READINESS",
                        System.currentTimeMillis(), groupId, verified ? "READY" : "NOT_READY",
                        expected.stream().map(TopicPartition::partition).sorted().toList(), positions));
            }
            ready = verified;
        } catch (WakeupException exception) {
            throw exception;
        } catch (RuntimeException exception) {
            throw new IllegalStateException("Failed to verify or emit Measurement Consumer readiness", exception);
        }
    }

    /** Runs once, blocking until shutdown or failure, and closes on this thread. */
    public void run() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Measurement Kafka consumer can only run once");
        }
        Throwable processingFailure = null;
        boolean pollingInvoked = false;
        try {
            if (!shutdownRequested) {
                if (readinessSink == null && progressSink == null) {
                    consumer.subscribe(List.of(RESULTS_TOPIC, ALERTS_TOPIC));
                } else {
                    consumer.subscribe(List.of(RESULTS_TOPIC, ALERTS_TOPIC), new ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                            assignmentInterrupted("MEASUREMENT_CONSUMER_PARTITIONS_REVOKED", partitions);
                        }

                        @Override
                        public void onPartitionsLost(Collection<TopicPartition> partitions) {
                            assignmentInterrupted("MEASUREMENT_CONSUMER_PARTITIONS_LOST", partitions);
                        }

                        @Override
                        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                            reportReadiness();
                        }
                    });
                }
            }
            while (!shutdownRequested) {
                pollingInvoked = true;
                var records = consumer.poll(POLL_TIMEOUT);
                long pollObservedAt = progressSink == null ? 0 : System.currentTimeMillis();
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record);
                }
                reportPoll(records.count(), pollObservedAt);
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
            long terminatedAt = progressSink == null ? 0 : System.currentTimeMillis();
            ready = false;
            Throwable failure = processingFailure;
            String groupId = null;
            if (progressSink != null) {
                try {
                    groupId = consumer.groupMetadata().groupId();
                    emit(new LifecycleEvidence("MEASUREMENT_CONSUMER_POLLING_TERMINATED", terminatedAt,
                            groupId, processingFailure != null ? "FAILED"
                                    : pollingInvoked ? "REQUESTED_SHUTDOWN" : "NOT_STARTED",
                            pollingInvoked, FailureDetail.from(processingFailure)));
                } catch (RuntimeException | Error evidenceFailure) {
                    failure = preserveFailure(failure, evidenceFailure);
                }
            }
            Throwable closeFailure = null;
            long closedAt;
            synchronized (lifecycleLock) {
                closed = true;
                try {
                    consumer.close();
                } catch (RuntimeException | Error exception) {
                    closeFailure = exception;
                    failure = preserveFailure(failure, exception);
                }
                closedAt = progressSink == null ? 0 : System.currentTimeMillis();
            }
            if (progressSink != null) {
                try {
                    emit(new LifecycleEvidence("MEASUREMENT_CONSUMER_KAFKA_CLOSED", closedAt,
                            groupId, closeFailure == null ? "CLOSED" : "FAILED", pollingInvoked,
                            FailureDetail.from(closeFailure)));
                } catch (RuntimeException | Error evidenceFailure) {
                    failure = preserveFailure(failure, evidenceFailure);
                }
            }
            if (processingFailure == null) {
                if (failure instanceof RuntimeException exception) throw exception;
                if (failure instanceof Error error) throw error;
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
                    var result = resultListener.onMessage(record.value());
                    if (runIdFilter != null && (result.runId() == null || result.runId().isBlank())) {
                        throw new IllegalArgumentException("Result runId is required for run isolation");
                    }
                    if (runIdFilter != null && !runIdFilter.equals(result.runId())) {
                        ignoredForeignResults++;
                        return;
                    }
                    ReceivedResult received = aggregator.registerResult(result, resultReceivedAt);
                    if (writer != null) {
                        writer.writeObservation(received);
                    }
                }
                case ALERTS_TOPIC -> {
                    var alert = alertListener.onMessage(record.value());
                    if (runIdFilter != null && (alert.runId() == null || alert.runId().isBlank())) {
                        throw new IllegalArgumentException("Alert runId is required for run isolation");
                    }
                    if (runIdFilter != null && !runIdFilter.equals(alert.runId())) {
                        ignoredForeignAlerts++;
                        return;
                    }
                    aggregator.registerAlert(alert);
                }
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
