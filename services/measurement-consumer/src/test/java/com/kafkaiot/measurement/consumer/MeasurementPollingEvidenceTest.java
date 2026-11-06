package com.kafkaiot.measurement.consumer;

import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementPollingEvidenceTest {
    private static final TopicPartition RESULT = new TopicPartition("sensor-results", 0);
    private static final TopicPartition ALERT = new TopicPartition("sensor-alerts", 0);

    @Test
    void successfulEmptyPollIsProgressAndNotFailure() {
        var fixture = new Fixture();
        fixture.kafka.schedulePollTask(() -> fixture.kafka.rebalance(List.of(RESULT, ALERT)));
        fixture.run();
        var poll = fixture.polls().getFirst();
        assertEquals("MEASUREMENT_CONSUMER_POLL_PROGRESS", poll.evidenceType());
        assertEquals("READY", poll.status());
        assertEquals(1, poll.successfulPolls());
        assertEquals(1, poll.emptyPolls());
        assertEquals(0, poll.recordCount());
        assertTrue(poll.lastPollEmpty());
        assertTrue(poll.observedAtEpochMs() > 0);
        assertEquals("dummy.group.id", poll.groupId());
    }

    @Test
    void pollsWithoutAssignmentAreSuccessfulButNotReady() {
        var fixture = new Fixture();
        fixture.kafka.schedulePollTask(() -> { });
        fixture.run();
        assertTrue(fixture.readiness.isEmpty());
        assertEquals("NOT_READY", fixture.polls().getFirst().status());
    }

    @Test
    void pollWithRecordsPreservesResultsDuplicatesAndAlerts() {
        var fixture = new Fixture();
        fixture.kafka.schedulePollTask(() -> {
            fixture.kafka.rebalance(List.of(RESULT, ALERT));
            String result = """
                    {"eventId":"e1","runId":"r1","deviceId":"d1","eventTime":1,"sentAt":2,
                     "ruleType":"HIGH_TEMPERATURE","triggered":true}
                    """;
            String alert = """
                    {"eventId":"e1","runId":"r1","deviceId":"d1","eventTime":1,"sentAt":2,
                     "ruleType":"HIGH_TEMPERATURE"}
                    """;
            fixture.kafka.addRecord(new ConsumerRecord<>("sensor-results", 0, 0, "d1", result));
            fixture.kafka.addRecord(new ConsumerRecord<>("sensor-results", 0, 1, "d1", result));
            fixture.kafka.addRecord(new ConsumerRecord<>("sensor-alerts", 0, 0, "d1", alert));
        });
        fixture.run();
        var poll = fixture.polls().getFirst();
        assertEquals(3, poll.recordCount());
        assertEquals(0, poll.emptyPolls());
        assertFalse(poll.lastPollEmpty());
        assertEquals(2, fixture.aggregator.getReceivedResultCount());
        assertEquals(1, fixture.aggregator.getDuplicateResultCount());
        assertEquals(1, fixture.aggregator.getReceivedAlertCount());
    }

    @Test
    void monotonicRateLimitAccumulatesRealPollsWithoutFillingTimeGaps() {
        var fixture = new Fixture();
        fixture.kafka.schedulePollTask(() -> fixture.nanoTime.set(0));
        fixture.kafka.schedulePollTask(() -> fixture.nanoTime.set(1));
        fixture.kafka.schedulePollTask(() -> fixture.nanoTime.set(999_999_999));
        fixture.kafka.schedulePollTask(() -> fixture.nanoTime.set(1_000_000_000));
        fixture.kafka.schedulePollTask(() -> fixture.nanoTime.set(9_000_000_000L));
        fixture.run();
        assertEquals(3, fixture.polls().size());
        assertEquals(1, fixture.polls().get(0).successfulPolls());
        assertEquals(3, fixture.polls().get(1).successfulPolls());
        assertEquals(1, fixture.polls().get(2).successfulPolls());
        assertEquals(3, fixture.polls().get(1).emptyPolls());
    }

    @Test
    void resultRevocationInvalidatesReadinessUntilVerifiedAssignmentRecovery() {
        assertInvalidationAndRecovery(false);
    }

    @Test
    void resultLossIsDistinctAndInvalidatesReadinessUntilRecovery() {
        assertInvalidationAndRecovery(true);
    }

    @Test
    void alertOnlyRevocationDoesNotInvalidateResultReadiness() {
        var fixture = new Fixture();
        fixture.kafka.schedulePollTask(() -> fixture.kafka.rebalance(List.of(RESULT, ALERT)));
        fixture.kafka.schedulePollTask(() -> fixture.kafka.listener.onPartitionsRevoked(List.of(ALERT)));
        fixture.run();
        assertEquals("READY", fixture.assignments().getFirst().status());
        assertEquals(List.of(new MeasurementKafkaConsumer.PartitionReference("sensor-alerts", 0)),
                fixture.assignments().getFirst().affectedPartitions());
    }

    @Test
    void disabledEvidenceAddsNoListenerOrReadinessQueries() {
        var kafka = new EvidenceConsumer();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null);
        kafka.schedulePollTask(() -> { });
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
        assertFalse(kafka.listenerInstalled);
        assertTrue(kafka.closed());
    }

    @Test
    void pollEvidenceFailurePropagatesAndClosesConsumer() {
        var kafka = new EvidenceConsumer();
        var failure = new IllegalStateException("progress sink failed");
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null,
                evidence -> { }, evidence -> { throw failure; });
        kafka.schedulePollTask(() -> { });
        var thrown = assertThrows(IllegalStateException.class, consumer::run);
        assertSame(failure, thrown.getCause());
        assertTrue(thrown.getMessage().contains("evidence"));
        assertTrue(kafka.closed());
    }

    @Test
    void revocationEvidenceFailureIsNotSilentlyIgnored() {
        var kafka = new EvidenceConsumer();
        var failure = new IllegalStateException("assignment sink failed");
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null,
                evidence -> { }, evidence -> {
                    if (evidence instanceof MeasurementKafkaConsumer.AssignmentEvidence) throw failure;
                });
        kafka.schedulePollTask(() -> kafka.rebalance(List.of(RESULT)));
        kafka.schedulePollTask(() -> kafka.listener.onPartitionsRevoked(List.of(RESULT)));
        var thrown = assertThrows(IllegalStateException.class, consumer::run);
        assertSame(failure, thrown.getCause());
        assertTrue(kafka.closed());
    }

    private static void assertInvalidationAndRecovery(boolean lost) {
        var fixture = new Fixture();
        fixture.kafka.schedulePollTask(() -> fixture.kafka.rebalance(List.of(RESULT, ALERT)));
        fixture.kafka.schedulePollTask(() -> {
            fixture.nanoTime.set(1_000_000_000L);
            if (lost) fixture.kafka.listener.onPartitionsLost(List.of(RESULT));
            else fixture.kafka.listener.onPartitionsRevoked(List.of(RESULT));
        });
        fixture.kafka.schedulePollTask(() -> {
            fixture.nanoTime.set(2_000_000_000L);
            fixture.kafka.rebalance(List.of(RESULT, ALERT));
        });
        fixture.run();
        assertEquals(1, fixture.assignments().size());
        var assignment = fixture.assignments().getFirst();
        assertEquals(lost ? "MEASUREMENT_CONSUMER_PARTITIONS_LOST" : "MEASUREMENT_CONSUMER_PARTITIONS_REVOKED",
                assignment.evidenceType());
        assertEquals("NOT_READY", assignment.status());
        assertEquals(List.of(new MeasurementKafkaConsumer.PartitionReference("sensor-results", 0)),
                assignment.affectedPartitions());
        assertTrue(assignment.assignedPartitions().contains(
                new MeasurementKafkaConsumer.PartitionReference("sensor-results", 0)));
        assertTrue(assignment.observedAtEpochMs() > 0);
        assertEquals(List.of("READY", "NOT_READY", "READY"),
                fixture.polls().stream().map(MeasurementKafkaConsumer.PollEvidence::status).toList());
        assertEquals(2, fixture.readiness.size());
        assertEquals("READY", fixture.readiness.get(1).status());
        assertEquals(0, fixture.readiness.get(1).assignedResultPartitions().getFirst().startingPosition());
    }

    private static final class Fixture {
        private final EvidenceConsumer kafka = new EvidenceConsumer();
        private final MeasurementAggregator aggregator = new MeasurementAggregator();
        private final AtomicLong nanoTime = new AtomicLong();
        private final List<MeasurementKafkaConsumer.ReadinessEvidence> readiness = new ArrayList<>();
        private final List<MeasurementKafkaConsumer.ProgressEvidence> progress = new ArrayList<>();
        private final MeasurementKafkaConsumer consumer = new MeasurementKafkaConsumer(
                kafka, aggregator, null, readiness::add, progress::add, nanoTime::get);

        private void run() {
            kafka.schedulePollTask(consumer::shutdown);
            consumer.run();
            assertTrue(kafka.closed());
        }

        private List<MeasurementKafkaConsumer.PollEvidence> polls() {
            return progress.stream().filter(MeasurementKafkaConsumer.PollEvidence.class::isInstance)
                    .map(MeasurementKafkaConsumer.PollEvidence.class::cast).toList();
        }

        private List<MeasurementKafkaConsumer.AssignmentEvidence> assignments() {
            return progress.stream().filter(MeasurementKafkaConsumer.AssignmentEvidence.class::isInstance)
                    .map(MeasurementKafkaConsumer.AssignmentEvidence.class::cast).toList();
        }
    }

    private static final class EvidenceConsumer extends MockConsumer<String, String> {
        private ConsumerRebalanceListener listener;
        private boolean listenerInstalled;

        private EvidenceConsumer() {
            super(OffsetResetStrategy.EARLIEST);
            updatePartitions("sensor-results", List.of(new PartitionInfo("sensor-results", 0, null, null, null)));
            updateBeginningOffsets(Map.of(RESULT, 0L, ALERT, 0L));
        }

        @Override
        public synchronized void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
            this.listener = listener;
            listenerInstalled = true;
            super.subscribe(topics, listener);
        }
    }
}
