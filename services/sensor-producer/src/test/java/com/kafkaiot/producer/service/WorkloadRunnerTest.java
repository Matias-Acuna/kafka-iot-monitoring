package com.kafkaiot.producer.service;

import com.kafkaiot.producer.generation.SensorEventGenerator;
import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.metrics.ProducerMetrics;
import com.kafkaiot.producer.service.WorkloadRunner.Phase;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadRunnerTest {
    @TempDir
    Path tempDir;

    @Test
    void preservesLegacyRunWithoutWriterAndCallbackEvent() {
        var kafka = mock(true);
        var metrics = new ProducerMetrics("run-1");
        RuntimeException stop = new RuntimeException("stop after one mocked publication");
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = runner(publisher, 0);
            assertSame(stop, assertThrows(RuntimeException.class, () -> runner.run(
                    (phase, event) -> metrics.recordAttempt(phase),
                    (phase, event, metadata, exception) -> {
                        assertEquals(0, event.sentAt());
                        assertEquals(Phase.MEASUREMENT, phase);
                        metrics.recordConfirmation(phase);
                        throw stop;
                    })));
            assertCounts(metrics.snapshot().measurement(), 1, 1, 0);
            assertEquals(1, kafka.history().size());
            assertTrue(serializedSentAt(kafka) > 0);
        }
    }

    @Test
    void recordsAttemptAndConfirmationWithSerializedSentAtAndMetadata() throws IOException {
        Path output = tempDir.resolve("confirmed.csv");
        var kafka = mock(true);
        var metrics = new ProducerMetrics("run-1");
        // Resources close in reverse order: publisher drains before writer closes.
        try (var writer = new ProducerEventCsvWriter(output);
             var publisher = new SensorEventPublisher(kafka)) {
            runOneOutcome(runner(publisher, 0), metrics, writer);
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            assertObservation(rows.get(1), "MEASUREMENT", "ATTEMPTED", serializedSentAt(kafka));
            assertObservation(rows.get(2), "MEASUREMENT", "CONFIRMED", serializedSentAt(kafka));
            assertEquals("0", rows.get(2).split(",", -1)[7]);
            assertEquals("0", rows.get(2).split(",", -1)[8]);
            assertEquals("", rows.get(2).split(",", -1)[9]);
            assertCounts(metrics.snapshot().measurement(), 1, 1, 0);
        }
    }

    @Test
    void retainsWarmUpPhaseForAsynchronousFailure() throws IOException {
        Path output = tempDir.resolve("async-failure.csv");
        var kafka = mock(false);
        var metrics = new ProducerMetrics("run-1");
        var kafkaFailure = new IllegalStateException("broker unavailable");
        try (var writer = new ProducerEventCsvWriter(output);
             var publisher = new SensorEventPublisher(kafka)) {
            runOnePendingAttempt(runner(publisher, 60), metrics, writer);
            writer.flush();
            assertEquals(2, Files.readAllLines(output).size());
            assertTrue(kafka.errorNext(kafkaFailure));
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            assertObservation(rows.get(1), "WARM_UP", "ATTEMPTED", serializedSentAt(kafka));
            assertObservation(rows.get(2), "WARM_UP", "FAILED", serializedSentAt(kafka));
            assertEquals(kafkaFailure.toString(), rows.get(2).split(",", -1)[9]);
            assertCounts(metrics.snapshot().warmUp(), 1, 0, 1);
        }
    }

    @Test
    void recordsSynchronousSendFailureAndAccountsOnce() throws IOException {
        Path output = tempDir.resolve("sync-failure.csv");
        var kafka = mock(false);
        var kafkaFailure = new IllegalStateException("send rejected");
        kafka.sendException = kafkaFailure;
        var metrics = new ProducerMetrics("run-1");
        try (var writer = new ProducerEventCsvWriter(output);
             var publisher = new SensorEventPublisher(kafka)) {
            assertSame(kafkaFailure, assertThrows(IllegalStateException.class, () -> runner(publisher, 0).run(
                    (phase, event) -> metrics.recordAttempt(phase),
                    (phase, event, metadata, exception) -> {
                        assertSame(kafkaFailure, exception);
                        metrics.recordFailure(phase, exception.toString());
                    }, writer)));
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            long sentAt = Long.parseLong(rows.get(1).split(",", -1)[5]);
            assertTrue(sentAt > 0);
            assertObservation(rows.get(1), "MEASUREMENT", "ATTEMPTED", sentAt);
            assertObservation(rows.get(2), "MEASUREMENT", "FAILED", sentAt);
            assertTrue(rows.get(2).endsWith(",,," + kafkaFailure));
            assertCounts(metrics.snapshot().measurement(), 1, 0, 1);
        }
    }

    @Test
    void recordsDelayedConfirmationWithoutDuplicateAccounting() throws IOException {
        Path output = tempDir.resolve("delayed.csv");
        var kafka = mock(false);
        var metrics = new ProducerMetrics("run-1");
        try (var writer = new ProducerEventCsvWriter(output);
             var publisher = new SensorEventPublisher(kafka)) {
            runOnePendingAttempt(runner(publisher, 0), metrics, writer);
            assertCounts(metrics.snapshot().measurement(), 1, 0, 0);
            assertTrue(kafka.completeNext());
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            assertObservation(rows.get(2), "MEASUREMENT", "CONFIRMED", serializedSentAt(kafka));
            assertCounts(metrics.snapshot().measurement(), 1, 1, 0);
        }
    }

    @Test
    void duplicateCallbacksProduceOneOutcomeObservationAndNotification() throws IOException {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("sensor-events", 2), 42, 0, 0, 0, 0);
        var kafka = new MockProducer<String, String>() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                callback.onCompletion(metadata, null);
                callback.onCompletion(metadata, null);
                return CompletableFuture.completedFuture(metadata);
            }
        };
        Path output = tempDir.resolve("duplicate.csv");
        var metrics = new ProducerMetrics("run-1");
        try (var writer = new ProducerEventCsvWriter(output);
             var publisher = new SensorEventPublisher(kafka)) {
            runOnePendingAttempt(runner(publisher, 0), metrics, writer);
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            assertTrue(rows.get(2).endsWith(",CONFIRMED,2,42,"));
            assertCounts(metrics.snapshot().measurement(), 1, 1, 0);
        }
    }

    @Test
    void callbackFailureDoesNotBecomeFailedPublicationOrSecondOutcome() throws IOException {
        Path output = tempDir.resolve("callback-error.csv");
        var kafka = mock(true);
        var metrics = new ProducerMetrics("run-1");
        try (var writer = new ProducerEventCsvWriter(output);
             var publisher = new SensorEventPublisher(kafka)) {
            // The helper deliberately throws after recording the successful outcome.
            runOneOutcome(runner(publisher, 0), metrics, writer);
            writer.flush();
            List<String> rows = Files.readAllLines(output);
            assertEquals(3, rows.size());
            assertEquals("CONFIRMED", rows.get(2).split(",", -1)[6]);
            assertCounts(metrics.snapshot().measurement(), 1, 1, 0);
        }
    }

    @Test
    void csvFailureDoesNotChangeKafkaOutcomeAndIsSurfacedAfterClosure() throws IOException {
        Path output = tempDir.resolve("closed-writer.csv");
        var writer = new ProducerEventCsvWriter(output);
        writer.close();
        var kafka = mock(true);
        var metrics = new ProducerMetrics("run-1");
        var publisher = new SensorEventPublisher(kafka);
        try {
            runOneOutcome(runner(publisher, 0), metrics, writer);
            assertCounts(metrics.snapshot().measurement(), 1, 1, 0);
            assertEquals(1, kafka.history().size());
        } finally {
            UncheckedIOException failure = assertThrows(UncheckedIOException.class, publisher::close);
            assertTrue(failure.getMessage().contains("observation CSV"));
            assertTrue(kafka.closed());
        }
    }

    @Test
    void asynchronousCsvFailureRemainsSeparateFromKafkaFailure() throws IOException {
        Path output = tempDir.resolve("outcome-io-error.csv");
        var kafka = mock(false);
        var metrics = new ProducerMetrics("run-1");
        var writer = new ProducerEventCsvWriter(output);
        var publisher = new SensorEventPublisher(kafka);
        try {
            runOnePendingAttempt(runner(publisher, 0), metrics, writer);
            writer.close();
            assertTrue(kafka.errorNext(new IllegalStateException("Kafka failure")));
            assertCounts(metrics.snapshot().measurement(), 1, 0, 1);
            assertTrue(metrics.snapshot().measurement().latestError().contains("Kafka failure"));
        } finally {
            assertThrows(UncheckedIOException.class, publisher::close);
            writer.close();
        }
    }

    @Test
    void exposesAnchorAndConfiguredBoundariesDuringRun() {
        var kafka = mock(true);
        RuntimeException stop = new RuntimeException("stop before publication");
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = new WorkloadRunner(
                    new SensorEventGenerator("run-1", 123, 0, 1), publisher, 10, 60, 300);
            assertNull(runner.getTiming());
            long before = System.nanoTime();
            assertSame(stop, assertThrows(RuntimeException.class, () -> runner.run((phase, event) -> {
                WorkloadRunner.Timing timing = runner.getTiming();
                assertNotNull(timing);
                assertTrue(timing.workloadStartNs() - before >= 0);
                assertTrue(System.nanoTime() - timing.workloadStartNs() >= timing.sampleSpanNs());
                assertTrue(timing.sampleSpanNs() >= 0);
                assertTrue(timing.workloadStartEpochMs() > 0);
                assertEquals(TimeUnit.SECONDS.toNanos(60), timing.warmUpDurationNs());
                assertEquals(TimeUnit.SECONDS.toNanos(300), timing.measurementDurationNs());
                assertEquals(TimeUnit.SECONDS.toNanos(60), timing.measurementStartOffsetNs());
                assertEquals(TimeUnit.SECONDS.toNanos(360), timing.measurementDeadlineOffsetNs());
                assertNull(timing.generationStoppedElapsedNs());
                assertNull(timing.generationStoppedEpochMs());
                assertEquals(Phase.WARM_UP, phase);
                throw stop;
            }, (phase, event, metadata, exception) -> { })));
            assertTrue(kafka.history().isEmpty());
            assertTrue(runner.getTiming().generationStoppedElapsedNs()
                    < runner.getTiming().measurementDeadlineOffsetNs());
        }
    }

    @Test
    void zeroWarmUpAndNormalTerminationRetainFixedDeadline() throws InterruptedException {
        var kafka = mock(true);
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = new WorkloadRunner(
                    new SensorEventGenerator("run-1", 123, 0, 1), publisher, 10, 0, 1);
            runner.run((phase, event, metadata, exception) -> assertEquals(Phase.MEASUREMENT, phase));
            WorkloadRunner.Timing timing = runner.getTiming();
            assertEquals(0, timing.warmUpDurationNs());
            assertEquals(0, timing.measurementStartOffsetNs());
            assertEquals(TimeUnit.SECONDS.toNanos(1), timing.measurementDurationNs());
            assertEquals(TimeUnit.SECONDS.toNanos(1), timing.measurementDeadlineOffsetNs());
            assertNotNull(timing.generationStoppedEpochMs());
            assertTrue(timing.generationStoppedElapsedNs() >= timing.measurementDeadlineOffsetNs());
            assertTrue(System.nanoTime() - timing.workloadStartNs() >= timing.generationStoppedElapsedNs());
        }
    }

    @Test
    void synchronousSendFailureRetainsExceptionAndTerminationTiming() {
        var kafka = mock(false);
        var failure = new IllegalStateException("send rejected");
        kafka.sendException = failure;
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = runner(publisher, 0);
            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> runner.run((phase, event, metadata, exception) -> assertSame(failure, exception))));
            assertNotNull(runner.getTiming().generationStoppedEpochMs());
            assertTrue(runner.getTiming().generationStoppedElapsedNs() >= runner.getTiming().sampleSpanNs());
        }
    }

    @Test
    void interruptionRetainsStatusAndTerminationTiming() {
        var kafka = mock(false);
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = runner(publisher, 0);
            Thread.currentThread().interrupt();
            try {
                assertThrows(InterruptedException.class,
                        () -> runner.run((phase, event, metadata, exception) -> { }));
                assertTrue(Thread.currentThread().isInterrupted());
                assertNotNull(runner.getTiming().generationStoppedElapsedNs());
                assertNotNull(runner.getTiming().generationStoppedEpochMs());
                assertTrue(kafka.history().isEmpty());
            } finally {
                Thread.interrupted();
            }
        }
    }

    @Test
    void snapshotsRemainImmutableAndSequentialRunsReplacePreviousTiming() {
        var kafka = mock(true);
        RuntimeException stop = new RuntimeException("stop");
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = runner(publisher, 0);
            WorkloadRunner.BeforePublishCallback beforePublish = (phase, event) -> { throw stop; };
            assertSame(stop, assertThrows(RuntimeException.class,
                    () -> runner.run(beforePublish, (phase, event, metadata, exception) -> { })));
            WorkloadRunner.Timing first = runner.getTiming();
            assertSame(stop, assertThrows(RuntimeException.class,
                    () -> runner.run(beforePublish, (phase, event, metadata, exception) -> { })));
            assertTrue(runner.getTiming().workloadStartNs() - first.workloadStartNs()
                    >= first.generationStoppedElapsedNs());
            assertNotNull(first.generationStoppedEpochMs());
        }
    }

    @Test
    void delayedPublicationSkipsMissedSlotsWithoutCatchUpBurst() {
        long intervalNs = TimeUnit.MILLISECONDS.toNanos(100);
        long[] firstReturnNs = new long[1];
        AtomicInteger sends = new AtomicInteger();
        var kafka = new MockProducer<String, String>() {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                if (sends.getAndIncrement() == 0) {
                    long delayStart = System.nanoTime();
                    long delayNs = TimeUnit.MILLISECONDS.toNanos(250);
                    while (System.nanoTime() - delayStart < delayNs) {
                        LockSupport.parkNanos(delayNs - (System.nanoTime() - delayStart));
                    }
                    firstReturnNs[0] = System.nanoTime();
                }
                return CompletableFuture.completedFuture(null);
            }
        };
        RuntimeException stop = new RuntimeException("stop before second publication");
        AtomicInteger attempts = new AtomicInteger();
        try (var publisher = new SensorEventPublisher(kafka)) {
            WorkloadRunner runner = new WorkloadRunner(
                    new SensorEventGenerator("run-1", 123, 0, 1), publisher, 10, 0, 10);
            assertSame(stop, assertThrows(RuntimeException.class, () -> runner.run((phase, event) -> {
                if (attempts.getAndIncrement() == 1) {
                    assertTrue(System.nanoTime() - firstReturnNs[0] >= intervalNs);
                    throw stop;
                }
            }, (phase, event, metadata, exception) -> { })));
            assertEquals(1, sends.get());
            assertEquals(2, attempts.get());
        }
    }

    @Test
    void timingObserverReceivesActualInitialSnapshotExactlyOnceBeforeGeneration() {
        var kafka = mock(true);
        var stop = new IllegalStateException("stop at timing observer");
        try (var publisher = new SensorEventPublisher(kafka)) {
            var runner = new WorkloadRunner(new SensorEventGenerator("run-1", 123, 0, 1),
                    publisher, 10, 60, 300);
            var calls = new AtomicInteger();
            var initial = new java.util.concurrent.atomic.AtomicReference<WorkloadRunner.Timing>();
            assertSame(stop, assertThrows(IllegalStateException.class, () -> runner.run(
                    (phase, event) -> { throw new AssertionError("must not generate"); },
                    (phase, event, metadata, exception) -> { }, null, timing -> {
                        calls.incrementAndGet();
                        assertSame(runner.getTiming(), timing);
                        initial.set(timing);
                        assertTrue(timing.sampleSpanNs() >= 0);
                        assertEquals(timing.workloadStartEpochMs() + 60_000, timing.measurementStartEpochMs());
                        assertEquals(timing.workloadStartEpochMs() + 360_000, timing.measurementDeadlineEpochMs());
                        throw stop;
                    })));
            assertEquals(1, calls.get());
            assertTrue(kafka.history().isEmpty());
            assertEquals(initial.get().workloadStartNs(), runner.getTiming().workloadStartNs());
            assertEquals(initial.get().sampleSpanNs(), runner.getTiming().sampleSpanNs());
            assertNotNull(runner.getTiming().generationStoppedElapsedNs());
        }
    }

    @Test
    void delayedTimingObserverDoesNotMoveDeadlineOrGenerateCatchUpEvents() throws InterruptedException {
        var kafka = mock(true);
        try (var publisher = new SensorEventPublisher(kafka)) {
            var runner = new WorkloadRunner(new SensorEventGenerator("run-1", 123, 0, 1),
                    publisher, 10, 0, 1);
            var calls = new AtomicInteger();
            runner.run((phase, event) -> { throw new AssertionError("deadline already passed"); },
                    (phase, event, metadata, exception) -> { }, null, timing -> {
                        calls.incrementAndGet();
                        assertEquals(timing.workloadStartEpochMs(), timing.measurementStartEpochMs());
                        assertEquals(timing.workloadStartEpochMs() + 1000, timing.measurementDeadlineEpochMs());
                        long delayStart = System.nanoTime();
                        while (System.nanoTime() - delayStart < 1_100_000_000L) {
                            LockSupport.parkNanos(1_100_000_000L - (System.nanoTime() - delayStart));
                        }
                    });
            assertEquals(1, calls.get());
            assertTrue(kafka.history().isEmpty());
            assertEquals(1_000_000_000L, runner.getTiming().measurementDeadlineOffsetNs());
            assertTrue(runner.getTiming().generationStoppedElapsedNs() >= 1_100_000_000L);
        }
    }

    private static WorkloadRunner runner(SensorEventPublisher publisher, long warmUpSeconds) {
        return new WorkloadRunner(new SensorEventGenerator("run-1", 123, 1_600_000_000_000L, 1),
                publisher, 1_000_000, warmUpSeconds, 1);
    }

    private static MockProducer<String, String> mock(boolean autoComplete) {
        return new MockProducer<>(autoComplete, null, new StringSerializer(), new StringSerializer());
    }

    private static void runOneOutcome(WorkloadRunner runner, ProducerMetrics metrics, ProducerEventCsvWriter writer) {
        RuntimeException stop = new RuntimeException("stop after one mocked outcome");
        assertSame(stop, assertThrows(RuntimeException.class, () -> runner.run(
                (phase, event) -> metrics.recordAttempt(phase),
                (phase, event, metadata, exception) -> {
                    recordOutcome(metrics, phase, exception);
                    throw stop;
                }, writer)));
    }

    private static void runOnePendingAttempt(WorkloadRunner runner, ProducerMetrics metrics,
                                              ProducerEventCsvWriter writer) {
        RuntimeException stop = new RuntimeException("stop before second mocked attempt");
        AtomicInteger generated = new AtomicInteger();
        assertSame(stop, assertThrows(RuntimeException.class, () -> runner.run(
                (phase, event) -> {
                    if (generated.getAndIncrement() != 0) {
                        throw stop;
                    }
                    metrics.recordAttempt(phase);
                }, (phase, event, metadata, exception) -> recordOutcome(metrics, phase, exception), writer)));
    }

    private static void recordOutcome(ProducerMetrics metrics, Phase phase, Exception exception) {
        if (exception == null) {
            metrics.recordConfirmation(phase);
        } else {
            metrics.recordFailure(phase, exception.toString());
        }
    }

    private static long serializedSentAt(MockProducer<String, String> kafka) {
        var matcher = Pattern.compile("\"sentAt\":(\\d+)").matcher(kafka.history().getFirst().value());
        assertTrue(matcher.find());
        return Long.parseLong(matcher.group(1));
    }

    private static void assertObservation(String row, String phase, String status, long sentAt) {
        String[] fields = row.split(",", -1);
        assertEquals(10, fields.length);
        assertEquals("run-1", fields[0]);
        assertEquals("run-1-1", fields[1]);
        assertEquals("device-001", fields[2]);
        assertEquals(phase, fields[3]);
        assertTrue(Long.parseLong(fields[4]) > 0);
        assertEquals(Long.toString(sentAt), fields[5]);
        assertEquals(status, fields[6]);
    }

    private static void assertCounts(ProducerMetrics.PhaseSnapshot snapshot, long attempted, long confirmed, long failed) {
        assertEquals(attempted, snapshot.attempted());
        assertEquals(confirmed, snapshot.confirmed());
        assertEquals(failed, snapshot.failed());
    }
}
