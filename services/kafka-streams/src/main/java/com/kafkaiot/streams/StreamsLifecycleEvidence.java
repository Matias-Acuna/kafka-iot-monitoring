package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Optional lifecycle snapshots; readiness and closure do not certify experimental coverage. */
final class StreamsLifecycleEvidence implements KafkaStreams.StateListener {
    private final String applicationId;
    private final Consumer<Evidence> sink;
    private final AtomicBoolean failed = new AtomicBoolean();
    private final AtomicBoolean shutdownRequested = new AtomicBoolean();
    private final AtomicReference<RuntimeException> outputFailure = new AtomicReference<>();

    record Failure(String type, String message, String cause, List<String> suppressed) {
        static Failure from(Throwable failure) {
            return failure == null ? null : new Failure(failure.getClass().getName(), failure.getMessage(),
                    failure.getCause() == null ? null : failure.getCause().toString(),
                    Arrays.stream(failure.getSuppressed()).map(Throwable::toString).toList());
        }
    }

    record Evidence(String evidenceType, long observedAtEpochMs, String applicationId,
                    KafkaStreams.State previousState, KafkaStreams.State currentState,
                    String status, Failure failure) { }

    StreamsLifecycleEvidence(String applicationId, Consumer<Evidence> sink) {
        this.applicationId = Objects.requireNonNull(applicationId, "applicationId");
        this.sink = Objects.requireNonNull(sink, "sink");
    }

    void startup(KafkaStreams.State state) {
        emit("KAFKA_STREAMS_STARTUP", state, state, "NOT_READY", null);
    }

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        boolean error = newState == KafkaStreams.State.ERROR || newState == KafkaStreams.State.PENDING_ERROR;
        if (error) failed.set(true);
        String status = error ? "FAILED" : newState == KafkaStreams.State.RUNNING
                && !failed.get() && !shutdownRequested.get() ? "READY" : "NOT_READY";
        emit("KAFKA_STREAMS_STATE_CHANGED", oldState, newState, status, null);
    }

    void failure(KafkaStreams.State state, Throwable failure) {
        failed.set(true);
        emit("KAFKA_STREAMS_FAILURE", state, state, "FAILED", failure);
    }

    /** Always attempts actual close, even if emitting the shutdown request fails. */
    boolean close(BooleanSupplier close, Supplier<KafkaStreams.State> state, boolean requested) {
        RuntimeException primary = null;
        KafkaStreams.State before = null;
        try {
            before = state.get();
            if (requested) {
                shutdownRequested.set(true);
                emit("KAFKA_STREAMS_SHUTDOWN_REQUESTED", before, before, "SHUTDOWN_REQUESTED", null);
            }
        } catch (RuntimeException exception) {
            primary = exception;
        }
        boolean closed = false;
        RuntimeException closeFailure = null;
        try {
            closed = close.getAsBoolean();
        } catch (RuntimeException exception) {
            closeFailure = exception;
            primary = preserve(primary, exception);
        }
        KafkaStreams.State after = null;
        try {
            after = state.get();
        } catch (RuntimeException exception) {
            primary = preserve(primary, exception);
        }
        try {
            emit("KAFKA_STREAMS_CLOSE_RESULT", before, after,
                    closeFailure != null ? "FAILED" : closed ? "CLOSED" : "TIMEOUT", closeFailure);
        } catch (RuntimeException exception) {
            primary = preserve(primary, exception);
        }
        if (primary != null) throw primary;
        return closed;
    }

    private static RuntimeException preserve(RuntimeException primary, RuntimeException additional) {
        if (primary == null) return additional;
        if (primary != additional) primary.addSuppressed(additional);
        return primary;
    }

    private void emit(String type, KafkaStreams.State previous, KafkaStreams.State current,
                      String status, Throwable failure) {
        RuntimeException previousFailure = outputFailure.get();
        if (previousFailure != null) throw previousFailure;
        try {
            sink.accept(new Evidence(type, System.currentTimeMillis(), applicationId,
                    previous, current, status, Failure.from(failure)));
        } catch (RuntimeException exception) {
            failed.set(true);
            outputFailure.compareAndSet(null, exception);
            System.err.println("Kafka Streams lifecycle evidence failed: " + exception);
            throw exception;
        }
    }

    static void print(Object evidence) {
        try {
            System.out.println(new ObjectMapper().writeValueAsString(evidence));
            if (System.out.checkError()) {
                throw new IllegalStateException("Kafka Streams lifecycle stdout could not be written");
            }
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Kafka Streams lifecycle evidence could not be serialized", exception);
        }
    }
}
