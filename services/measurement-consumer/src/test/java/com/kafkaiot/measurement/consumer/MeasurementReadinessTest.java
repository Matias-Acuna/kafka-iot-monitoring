package com.kafkaiot.measurement.consumer;

import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementReadinessTest {
    private static final TopicPartition FIRST = new TopicPartition("sensor-results", 2);
    private static final TopicPartition SECOND = new TopicPartition("sensor-results", 7);
    private static final TopicPartition ALERT = new TopicPartition("sensor-alerts", 0);

    @Test
    void subscriptionAndPollingWithoutAssignmentNeverReportReady() {
        var kafka = new ReadinessConsumer();
        var evidence = new ArrayList<MeasurementKafkaConsumer.ReadinessEvidence>();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, evidence::add);
        kafka.schedulePollTask(() -> assertEquals(List.of(), evidence));
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
        assertTrue(evidence.isEmpty());
        assertEquals(0, kafka.metadataCalls);
        assertTrue(kafka.closed());
    }

    @Test
    void emptyAssignmentReportsNotReadyWithNoInventedPositions() {
        var evidence = runAssignment(new ReadinessConsumer(), List.of());
        assertEquals("NOT_READY", evidence.status());
        assertEquals(List.of(), evidence.assignedResultPartitions());
        assertEquals(List.of(2, 7), evidence.expectedResultPartitions());
    }

    @Test
    void partialAssignmentIsNotFullReadiness() {
        var evidence = runAssignment(new ReadinessConsumer(), List.of(FIRST));
        assertEquals("NOT_READY", evidence.status());
        assertEquals(List.of(new MeasurementKafkaConsumer.PartitionPosition("sensor-results", 2, 42)),
                evidence.assignedResultPartitions());
    }

    @Test
    void completeAssignmentUsesActualMetadataAndStartingPositions() {
        var evidence = runAssignment(new ReadinessConsumer(), List.of(SECOND, FIRST, ALERT));
        assertEquals("MEASUREMENT_CONSUMER_READINESS", evidence.evidenceType());
        assertEquals("READY", evidence.status());
        assertEquals("dummy.group.id", evidence.groupId());
        assertTrue(evidence.observedAtEpochMs() > 0);
        assertEquals(List.of(2, 7), evidence.expectedResultPartitions());
        assertEquals(List.of(new MeasurementKafkaConsumer.PartitionPosition("sensor-results", 2, 42),
                new MeasurementKafkaConsumer.PartitionPosition("sensor-results", 7, 81)),
                evidence.assignedResultPartitions());
        assertThrows(UnsupportedOperationException.class, () -> evidence.expectedResultPartitions().add(99));
    }

    @Test
    void unavailablePositionFailsWithoutReadyEvidence() {
        var kafka = new ReadinessConsumer();
        kafka.positions = Map.of(FIRST, 42L);
        assertVerificationFailure(kafka, TimeoutException.class);
    }

    @Test
    void negativePositionFailsWithoutReadyEvidence() {
        var kafka = new ReadinessConsumer();
        kafka.positions = Map.of(FIRST, -1L, SECOND, 81L);
        assertVerificationFailure(kafka, IllegalStateException.class);
    }

    @Test
    void unavailableTopicMetadataFailsWithoutReadyEvidence() {
        var kafka = new ReadinessConsumer();
        kafka.updatePartitions("sensor-results", List.of());
        assertVerificationFailure(kafka, IllegalStateException.class);
    }

    @Test
    void disabledEvidenceDoesNotQueryReadinessMetadataOrPositions() {
        var kafka = new ReadinessConsumer();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null);
        kafka.schedulePollTask(() -> kafka.rebalance(List.of(FIRST)));
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
        assertEquals(0, kafka.metadataCalls);
        assertEquals(0, kafka.positionCalls);
        assertTrue(kafka.closed());
    }

    @Test
    void enabledEvidencePrecedesRecordHandlingAndPreservesResultAndAlertProcessing() {
        var kafka = new ReadinessConsumer();
        var aggregator = new MeasurementAggregator();
        var evidence = new ArrayList<MeasurementKafkaConsumer.ReadinessEvidence>();
        var consumer = new MeasurementKafkaConsumer(kafka, aggregator, null, snapshot -> {
            assertEquals(0, aggregator.getReceivedResultCount());
            assertSame(Thread.currentThread(), kafka.pollThread);
            evidence.add(snapshot);
        });
        String result = """
                {"eventId":"e1","runId":"r1","deviceId":"d1","eventTime":1,
                 "sentAt":2,"ruleType":"HIGH_TEMPERATURE","triggered":true}
                """;
        String alert = """
                {"eventId":"e1","runId":"r1","deviceId":"d1","eventTime":1,
                 "sentAt":2,"ruleType":"HIGH_TEMPERATURE"}
                """;
        kafka.schedulePollTask(() -> {
            kafka.updateBeginningOffsets(Map.of(FIRST, 42L, SECOND, 81L, ALERT, 0L));
            kafka.rebalance(List.of(FIRST, SECOND, ALERT));
            kafka.addRecord(new ConsumerRecord<>("sensor-results", 2, 42, "d1", result));
            kafka.addRecord(new ConsumerRecord<>("sensor-results", 2, 43, "d1", result));
            kafka.addRecord(new ConsumerRecord<>("sensor-alerts", 0, 0, "d1", alert));
        });
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
        assertEquals(1, evidence.size());
        assertEquals(42, evidence.getFirst().assignedResultPartitions().getFirst().startingPosition());
        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(1, aggregator.getDuplicateResultCount());
        assertEquals(1, aggregator.getReceivedAlertCount());
        assertTrue(kafka.closed());
    }

    @Test
    void evidenceSinkFailurePropagatesAndConsumerCloses() {
        var kafka = new ReadinessConsumer();
        var failure = new IllegalStateException("evidence sink failed");
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null,
                snapshot -> { throw failure; });
        kafka.schedulePollTask(() -> kafka.rebalance(List.of(FIRST, SECOND)));
        var thrown = assertThrows(IllegalStateException.class, consumer::run);
        assertSame(failure, thrown.getCause());
        assertTrue(kafka.closed());
    }

    private static MeasurementKafkaConsumer.ReadinessEvidence runAssignment(
            ReadinessConsumer kafka, List<TopicPartition> assignment) {
        var evidence = new ArrayList<MeasurementKafkaConsumer.ReadinessEvidence>();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, evidence::add);
        kafka.schedulePollTask(() -> kafka.rebalance(assignment));
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
        assertEquals(1, evidence.size());
        return evidence.getFirst();
    }

    private static void assertVerificationFailure(ReadinessConsumer kafka, Class<?> causeType) {
        var evidence = new ArrayList<MeasurementKafkaConsumer.ReadinessEvidence>();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, evidence::add);
        kafka.schedulePollTask(() -> kafka.rebalance(List.of(FIRST, SECOND)));
        var failure = assertThrows(IllegalStateException.class, consumer::run);
        assertTrue(failure.getMessage().contains("readiness"));
        assertTrue(causeType.isInstance(failure.getCause()));
        assertTrue(evidence.isEmpty());
        assertTrue(kafka.closed());
    }

    private static final class ReadinessConsumer extends MockConsumer<String, String> {
        private Map<TopicPartition, Long> positions = Map.of(FIRST, 42L, SECOND, 81L);
        private int metadataCalls;
        private int positionCalls;
        private Thread pollThread;

        private ReadinessConsumer() {
            super(OffsetResetStrategy.EARLIEST);
            updatePartitions("sensor-results", List.of(
                    new PartitionInfo("sensor-results", 2, null, null, null),
                    new PartitionInfo("sensor-results", 7, null, null, null)));
            updateBeginningOffsets(Map.of(FIRST, 42L, SECOND, 81L, ALERT, 0L));
        }

        @Override
        public synchronized org.apache.kafka.clients.consumer.ConsumerRecords<String, String> poll(Duration timeout) {
            pollThread = Thread.currentThread();
            return super.poll(timeout);
        }

        @Override
        public synchronized List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
            metadataCalls++;
            return super.partitionsFor(topic);
        }

        @Override
        public synchronized long position(TopicPartition partition, Duration timeout) {
            positionCalls++;
            if (!positions.containsKey(partition)) {
                throw new TimeoutException("starting position unavailable");
            }
            return positions.get(partition);
        }
    }
}
