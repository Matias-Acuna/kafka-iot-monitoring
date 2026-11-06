package com.kafkaiot.measurement.consumer;

import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementLifecycleEvidenceTest {
    @Test
    void requestedShutdownReportsTerminationBeforeActualKafkaClosure() {
        var events = new ArrayList<MeasurementKafkaConsumer.ProgressEvidence>();
        var kafka = new ClosingConsumer();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, null, event -> {
            if (event instanceof MeasurementKafkaConsumer.LifecycleEvidence lifecycle) {
                assertEquals("MEASUREMENT_CONSUMER_KAFKA_CLOSED".equals(lifecycle.evidenceType()), kafka.didClose);
                assertTrue(lifecycle.observedAtEpochMs() > 0);
            }
            events.add(event);
        });
        kafka.schedulePollTask(() -> { });
        kafka.schedulePollTask(consumer::shutdown);
        consumer.run();
        var lifecycle = lifecycle(events);
        assertEquals(2, lifecycle.size());
        assertEquals("REQUESTED_SHUTDOWN", lifecycle.get(0).status());
        assertTrue(lifecycle.get(0).pollingInvoked());
        assertEquals("CLOSED", lifecycle.get(1).status());
        assertSame(Thread.currentThread(), kafka.closeThread);
    }

    @Test
    void shutdownBeforePollingIsExplicitlyNotStarted() {
        var events = new ArrayList<MeasurementKafkaConsumer.ProgressEvidence>();
        var kafka = new ClosingConsumer();
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, null, events::add);
        consumer.shutdown();
        consumer.run();
        assertEquals("NOT_STARTED", lifecycle(events).getFirst().status());
        assertEquals("CLOSED", lifecycle(events).get(1).status());
    }

    @Test
    void pollingFailurePreservesOriginalAndReportsFailedTermination() {
        var events = new ArrayList<MeasurementKafkaConsumer.ProgressEvidence>();
        var kafka = new ClosingConsumer();
        var failure = new IllegalStateException("poll failed");
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, null, events::add);
        kafka.schedulePollTask(() -> { throw failure; });
        assertSame(failure, assertThrows(IllegalStateException.class, consumer::run));
        assertEquals("FAILED", lifecycle(events).getFirst().status());
        assertEquals("poll failed", lifecycle(events).getFirst().failure().message());
        assertEquals("CLOSED", lifecycle(events).get(1).status());
        assertTrue(kafka.didClose);
    }

    @Test
    void kafkaCleanupFailureIsExplicitAndPreservesProcessingFailure() {
        var events = new ArrayList<MeasurementKafkaConsumer.ProgressEvidence>();
        var kafka = new ClosingConsumer();
        var failure = new IllegalStateException("processing failed");
        var closeFailure = new IllegalStateException("Kafka close failed");
        kafka.closeFailure = closeFailure;
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, null, events::add);
        kafka.schedulePollTask(() -> { throw failure; });
        assertSame(failure, assertThrows(IllegalStateException.class, consumer::run));
        assertEquals(List.of(closeFailure), List.of(failure.getSuppressed()));
        var closed = lifecycle(events).get(1);
        assertEquals("FAILED", closed.status());
        assertEquals("Kafka close failed", closed.failure().message());
    }

    @Test
    void terminationOutputFailureCannotPreventKafkaClosureOrHideOtherFailures() {
        var kafka = new ClosingConsumer();
        var original = new IllegalStateException("poll failed");
        var closeFailure = new IllegalStateException("close failed");
        var outputFailure = new IllegalStateException("output failed");
        kafka.closeFailure = closeFailure;
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, null,
                event -> { throw outputFailure; });
        kafka.schedulePollTask(() -> { throw original; });
        assertSame(original, assertThrows(IllegalStateException.class, consumer::run));
        assertTrue(kafka.didClose);
        assertEquals(3, original.getSuppressed().length);
        assertTrue(List.of(original.getSuppressed()).contains(closeFailure));
    }

    @Test
    void outputFailureWithoutPollingFailureIsStillThrownAfterKafkaCloses() {
        var kafka = new ClosingConsumer();
        var outputFailure = new IllegalStateException("output failed");
        var consumer = new MeasurementKafkaConsumer(kafka, new MeasurementAggregator(), null, null,
                event -> { throw outputFailure; });
        consumer.shutdown();
        var thrown = assertThrows(IllegalStateException.class, consumer::run);
        assertSame(outputFailure, thrown.getCause());
        assertTrue(kafka.didClose);
    }

    private static List<MeasurementKafkaConsumer.LifecycleEvidence> lifecycle(
            List<MeasurementKafkaConsumer.ProgressEvidence> events) {
        return events.stream().filter(MeasurementKafkaConsumer.LifecycleEvidence.class::isInstance)
                .map(MeasurementKafkaConsumer.LifecycleEvidence.class::cast).toList();
    }

    private static final class ClosingConsumer extends MockConsumer<String, String> {
        private boolean didClose;
        private Thread closeThread;
        private RuntimeException closeFailure;

        private ClosingConsumer() {
            super(OffsetResetStrategy.EARLIEST);
        }

        @Override
        public void close() {
            closeThread = Thread.currentThread();
            super.close();
            didClose = true;
            if (closeFailure != null) throw closeFailure;
        }
    }
}
