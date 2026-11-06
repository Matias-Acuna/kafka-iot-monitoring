package com.kafkaiot.producer.service;

import com.kafkaiot.producer.generation.SensorEventGenerator;
import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.model.SensorEvent;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runs a paced workload on the calling thread. Use sequentially with a generator
 * and publisher owned by the caller. This class neither drains nor closes them.
 */
public final class WorkloadRunner {
    public static final long DEFAULT_WARM_UP_SECONDS = 60;
    public static final long DEFAULT_MEASUREMENT_SECONDS = 300;
    private static final long NANOS_PER_SECOND = 1_000_000_000L;

    public enum Phase {
        WARM_UP,
        MEASUREMENT
    }

    /**
     * Called on the workload thread immediately before publication. If it throws,
     * the event is not published and no completion notification is delivered.
     */
    @FunctionalInterface
    public interface BeforePublishCallback {
        void beforePublish(Phase phase, SensorEvent event);
    }

    /**
     * Called on a Kafka completion thread or the workload thread, possibly before
     * publish returns or after run returns. The event
     * is the original generated event; the publisher sets sentAt on its copy.
     * Implementations should be thread-safe and avoid blocking.
     */
    @FunctionalInterface
    public interface PublicationCallback {
        void onCompletion(Phase phase, SensorEvent event,
                          RecordMetadata metadata, Exception exception);
    }

    private final SensorEventGenerator generator;
    private final SensorEventPublisher publisher;
    private final double intervalNanos;
    private final long warmUpNanos;
    private final long totalNanos;
    private volatile Timing timing;

    /** Latest immutable invocation snapshot; null before the first validated run starts. */
    public Timing getTiming() {
        return timing;
    }

    /** Local nanoseconds are process-specific; boundaries are offsets from workloadStartNs. */
    public record Timing(long workloadStartNs, long workloadStartEpochMs, long sampleSpanNs,
                         long warmUpDurationNs, long measurementDurationNs,
                         Long generationStoppedElapsedNs, Long generationStoppedEpochMs) {
        public long measurementStartOffsetNs() {
            return warmUpDurationNs;
        }

        public long measurementDeadlineOffsetNs() {
            return warmUpDurationNs + measurementDurationNs;
        }
    }

    public WorkloadRunner(SensorEventGenerator generator,
                          SensorEventPublisher publisher,
                          double targetEventsPerSecond) {
        this(generator, publisher, targetEventsPerSecond,
                DEFAULT_WARM_UP_SECONDS, DEFAULT_MEASUREMENT_SECONDS);
    }

    public WorkloadRunner(SensorEventGenerator generator,
                          SensorEventPublisher publisher,
                          double targetEventsPerSecond,
                          long warmUpSeconds, long measurementSeconds) {
        this.generator = Objects.requireNonNull(generator, "generator");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        if (!Double.isFinite(targetEventsPerSecond) || targetEventsPerSecond <= 0) {
            throw new IllegalArgumentException("targetEventsPerSecond must be finite and positive");
        }
        if (warmUpSeconds < 0 || measurementSeconds <= 0) {
            throw new IllegalArgumentException(
                    "warmUpSeconds must be non-negative and measurementSeconds positive");
        }
        intervalNanos = NANOS_PER_SECOND / targetEventsPerSecond;
        try {
            warmUpNanos = Math.multiplyExact(warmUpSeconds, NANOS_PER_SECOND);
            totalNanos = Math.multiplyExact(
                    Math.addExact(warmUpSeconds, measurementSeconds), NANOS_PER_SECOND);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("durations are too large", exception);
        }
    }

    /**
     * Generates until the measurement deadline, without waiting for outstanding
     * sends. Phase labels reflect generation time, not callback completion time.
     * Missed send slots are skipped to avoid catch-up bursts when publishing is
     * slower than the requested rate. Synchronous publish failures propagate.
     * Kafka send itself may block according to its configured max.block.ms.
     * Interruption stops generation, preserves interrupt status, and propagates.
     */
    public void run(PublicationCallback callback) throws InterruptedException {
        run((phase, event) -> { }, callback);
    }

    /**
     * Runs with a pre-publication hook and one outcome notification per attempted
     * send. Synchronous send failures are reported and then rethrown. If reporting
     * that failure also throws, the callback exception is preserved as suppressed
     * on the send exception. Callback failures never trigger another notification.
     */
    public void run(BeforePublishCallback beforePublish,
                    PublicationCallback completionCallback) throws InterruptedException {
        run(beforePublish, completionCallback, null);
    }

    /**
     * Adds optional publication observations without changing metrics callbacks.
     * Close the publisher before flushing/closing the writer to await outcomes and
     * surface CSV failures separately from publication failures.
     */
    public void run(BeforePublishCallback beforePublish, PublicationCallback completionCallback,
                    ProducerEventCsvWriter writer) throws InterruptedException {
        Objects.requireNonNull(beforePublish, "beforePublish");
        Objects.requireNonNull(completionCallback, "completionCallback");
        long n0 = System.nanoTime();
        long epochMs = System.currentTimeMillis();
        long n1 = System.nanoTime();
        long start = n0;
        double nextDueNanos = 0;
        try {
            timing = new Timing(n0, epochMs, n1 - n0, warmUpNanos, totalNanos - warmUpNanos,
                    null, null);
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Workload interrupted");
                }
                long elapsed = System.nanoTime() - start;
                if (elapsed >= totalNanos) {
                    return;
                }
                double waitNanos = Math.min(nextDueNanos, (double) totalNanos) - elapsed;
                if (waitNanos > 0) {
                    TimeUnit.NANOSECONDS.sleep((long) Math.ceil(waitNanos));
                    continue;
                }

                Phase phase = elapsed < warmUpNanos ? Phase.WARM_UP : Phase.MEASUREMENT;
                SensorEvent event = generator.nextEvent();
                AtomicBoolean outcomeDelivered = new AtomicBoolean();
                beforePublish.beforePublish(phase, event);
                try {
                    publisher.publish(event, (metadata, exception) -> {
                        if (outcomeDelivered.compareAndSet(false, true)) {
                            completionCallback.onCompletion(phase, event, metadata, exception);
                        }
                    }, phase, writer);
                } catch (RuntimeException sendException) {
                    if (outcomeDelivered.compareAndSet(false, true)) {
                        try {
                            completionCallback.onCompletion(phase, event, null, sendException);
                        } catch (RuntimeException | Error callbackException) {
                            if (callbackException != sendException) {
                                sendException.addSuppressed(callbackException);
                            }
                        }
                    }
                    throw sendException;
                }

                nextDueNanos += intervalNanos;
                long afterSend = System.nanoTime() - start;
                if (nextDueNanos < afterSend) {
                    nextDueNanos = afterSend + intervalNanos;
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw exception;
        } finally {
            long stoppedNs = System.nanoTime();
            long stoppedEpochMs = System.currentTimeMillis();
            timing = new Timing(n0, epochMs, n1 - n0, warmUpNanos, totalNanos - warmUpNanos,
                    stoppedNs - n0, stoppedEpochMs);
        }
    }
}
