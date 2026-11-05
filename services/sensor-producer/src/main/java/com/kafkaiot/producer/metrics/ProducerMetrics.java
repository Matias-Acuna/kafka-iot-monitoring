package com.kafkaiot.producer.metrics;

import com.kafkaiot.producer.service.WorkloadRunner.Phase;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe publication counters for one run. Record attempts before publishing
 * and exactly one confirmation or failure for each completed publication.
 * Recording an outcome does not implicitly record an attempt.
 */
public final class ProducerMetrics {
    private static final int MAX_ERROR_LENGTH = 256;

    private final String runId;
    private final PhaseCounters warmUp = new PhaseCounters();
    private final PhaseCounters measurement = new PhaseCounters();

    public ProducerMetrics(String runId) {
        this.runId = Objects.requireNonNull(runId, "runId");
        if (runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be blank");
        }
    }

    public void recordAttempt(Phase phase) {
        counters(phase).attempted.incrementAndGet();
    }

    public void recordConfirmation(Phase phase) {
        counters(phase).confirmed.incrementAndGet();
    }

    /**
     * Records one failure and an optional short error description for its phase.
     * A null description leaves the previous error description unchanged.
     */
    public void recordFailure(Phase phase, String errorDescription) {
        PhaseCounters counters = counters(phase);
        if (errorDescription != null) {
            counters.latestError.set(errorDescription.substring(
                    0, Math.min(errorDescription.length(), MAX_ERROR_LENGTH)));
        }
        counters.failed.incrementAndGet();
    }

    /**
     * Returns immutable values. Individual reads are atomic, but concurrent
     * updates may occur between reads; this is not a transactional snapshot.
     */
    public Snapshot snapshot() {
        return new Snapshot(runId, warmUp.snapshot(Phase.WARM_UP),
                measurement.snapshot(Phase.MEASUREMENT));
    }

    private PhaseCounters counters(Phase phase) {
        return switch (Objects.requireNonNull(phase, "phase")) {
            case WARM_UP -> warmUp;
            case MEASUREMENT -> measurement;
        };
    }

    public record Snapshot(String runId, PhaseSnapshot warmUp,
                           PhaseSnapshot measurement) {
    }

    /**
     * latestError is null until a failure description has been recorded.
     */
    public record PhaseSnapshot(Phase phase, long attempted, long confirmed,
                                long failed, String latestError) {
    }

    private static final class PhaseCounters {
        private final AtomicLong attempted = new AtomicLong();
        private final AtomicLong confirmed = new AtomicLong();
        private final AtomicLong failed = new AtomicLong();
        private final AtomicReference<String> latestError = new AtomicReference<>();

        private PhaseSnapshot snapshot(Phase phase) {
            return new PhaseSnapshot(phase, attempted.get(), confirmed.get(),
                    failed.get(), latestError.get());
        }
    }
}
