package com.kafkaiot.producer.metrics;

import com.kafkaiot.producer.metrics.ProducerMetrics.PhaseSnapshot;
import com.kafkaiot.producer.metrics.ProducerMetrics.Snapshot;
import com.kafkaiot.producer.service.WorkloadRunner.Phase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerMetricsTest {
    private static final String RUN_ID = "metrics-test";

    @Test
    void initialSnapshotHasRunIdPhaseIdentifiersAndEmptyMetrics() {
        Snapshot snapshot = new ProducerMetrics(RUN_ID).snapshot();

        assertEquals(RUN_ID, snapshot.runId());
        assertEquals(new PhaseSnapshot(Phase.WARM_UP, 0, 0, 0, null), snapshot.warmUp());
        assertEquals(new PhaseSnapshot(Phase.MEASUREMENT, 0, 0, 0, null), snapshot.measurement());
    }

    @Test
    void updatesAreIsolatedToTheirPhase() {
        ProducerMetrics metrics = new ProducerMetrics(RUN_ID);
        PhaseSnapshot initialMeasurement = metrics.snapshot().measurement();

        metrics.recordAttempt(Phase.WARM_UP);
        metrics.recordConfirmation(Phase.WARM_UP);
        metrics.recordFailure(Phase.WARM_UP, "warm-up error");
        PhaseSnapshot warmUp = metrics.snapshot().warmUp();
        assertEquals(new PhaseSnapshot(Phase.WARM_UP, 1, 1, 1, "warm-up error"), warmUp);
        assertEquals(initialMeasurement, metrics.snapshot().measurement());

        metrics.recordAttempt(Phase.MEASUREMENT);
        metrics.recordConfirmation(Phase.MEASUREMENT);
        metrics.recordFailure(Phase.MEASUREMENT, "measurement error");
        assertEquals(warmUp, metrics.snapshot().warmUp());
        assertEquals(new PhaseSnapshot(Phase.MEASUREMENT, 1, 1, 1, "measurement error"),
                metrics.snapshot().measurement());
    }

    @Test
    void recordingOperationsUpdateTheirCountersIndependently() {
        for (Phase phase : Phase.values()) {
            ProducerMetrics metrics = new ProducerMetrics(RUN_ID);
            metrics.recordConfirmation(phase);
            assertCounts(phaseSnapshot(metrics, phase), 0, 1, 0);

            metrics.recordFailure(phase, null);
            assertCounts(phaseSnapshot(metrics, phase), 0, 1, 1);

            metrics.recordAttempt(phase);
            assertCounts(phaseSnapshot(metrics, phase), 1, 1, 1);
            assertNull(phaseSnapshot(metrics, phase).latestError());
        }
    }

    @Test
    void latestNonNullFailureDescriptionIsRetained() {
        ProducerMetrics metrics = new ProducerMetrics(RUN_ID);
        metrics.recordFailure(Phase.MEASUREMENT, "first error");
        assertEquals("first error", metrics.snapshot().measurement().latestError());

        metrics.recordFailure(Phase.MEASUREMENT, "latest error");
        assertEquals("latest error", metrics.snapshot().measurement().latestError());

        metrics.recordFailure(Phase.MEASUREMENT, null);
        assertEquals("latest error", metrics.snapshot().measurement().latestError());
        assertCounts(metrics.snapshot().measurement(), 0, 0, 3);
    }

    @Test
    void longErrorDescriptionsAreTruncatedTo256Characters() {
        ProducerMetrics metrics = new ProducerMetrics(RUN_ID);
        String prefix = "abcd".repeat(64);
        metrics.recordFailure(Phase.WARM_UP, prefix + "discarded suffix");

        String error = metrics.snapshot().warmUp().latestError();
        assertEquals(256, error.length());
        assertEquals(prefix, error);
    }

    @Test
    void earlierSnapshotRetainsItsValuesAfterUpdates() {
        ProducerMetrics metrics = new ProducerMetrics(RUN_ID);
        metrics.recordAttempt(Phase.WARM_UP);
        metrics.recordFailure(Phase.WARM_UP, "original error");
        Snapshot earlier = metrics.snapshot();

        metrics.recordAttempt(Phase.WARM_UP);
        metrics.recordConfirmation(Phase.WARM_UP);
        metrics.recordFailure(Phase.WARM_UP, "new error");
        metrics.recordAttempt(Phase.MEASUREMENT);
        metrics.recordConfirmation(Phase.MEASUREMENT);
        metrics.recordFailure(Phase.MEASUREMENT, "measurement error");

        assertEquals(RUN_ID, earlier.runId());
        assertEquals(new PhaseSnapshot(Phase.WARM_UP, 1, 0, 1, "original error"), earlier.warmUp());
        assertEquals(new PhaseSnapshot(Phase.MEASUREMENT, 0, 0, 0, null), earlier.measurement());
        assertEquals(new PhaseSnapshot(Phase.WARM_UP, 2, 1, 2, "new error"),
                metrics.snapshot().warmUp());
        assertEquals(new PhaseSnapshot(Phase.MEASUREMENT, 1, 1, 1, "measurement error"),
                metrics.snapshot().measurement());
    }

    @Test
    void constructorRejectsNullAndBlankRunIds() {
        assertThrows(NullPointerException.class, () -> new ProducerMetrics(null));
        for (String runId : new String[]{"", " ", "\t\r\n"}) {
            assertThrows(IllegalArgumentException.class, () -> new ProducerMetrics(runId));
        }
    }

    @Test
    void recordingOperationsRejectNullPhasesWithoutChangingMetrics() {
        ProducerMetrics metrics = new ProducerMetrics(RUN_ID);
        Snapshot before = metrics.snapshot();

        assertThrows(NullPointerException.class, () -> metrics.recordAttempt(null));
        assertThrows(NullPointerException.class, () -> metrics.recordConfirmation(null));
        assertThrows(NullPointerException.class, () -> metrics.recordFailure(null, "error"));
        assertThrows(NullPointerException.class, () -> metrics.recordFailure(null, null));
        assertEquals(before, metrics.snapshot());
    }

    private static PhaseSnapshot phaseSnapshot(ProducerMetrics metrics, Phase phase) {
        Snapshot snapshot = metrics.snapshot();
        return phase == Phase.WARM_UP ? snapshot.warmUp() : snapshot.measurement();
    }

    private static void assertCounts(PhaseSnapshot snapshot, long attempted,
                                     long confirmed, long failed) {
        assertAll(
                () -> assertEquals(attempted, snapshot.attempted()),
                () -> assertEquals(confirmed, snapshot.confirmed()),
                () -> assertEquals(failed, snapshot.failed()));
    }
}
