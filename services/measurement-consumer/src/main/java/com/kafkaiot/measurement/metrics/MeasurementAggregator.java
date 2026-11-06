package com.kafkaiot.measurement.metrics;

import com.kafkaiot.measurement.model.SensorAlert;
import com.kafkaiot.measurement.model.SensorResult;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Accumulates result deliveries and alert counts for use by a single thread. */
public final class MeasurementAggregator {
    private final Set<ResultKey> seenResults = new HashSet<>();
    private final List<ReceivedResult> receivedResults = new ArrayList<>();
    private long duplicateResultCount;
    private long receivedAlertCount;

    /** Registers a delivery using the caller's Unix epoch millisecond timestamp. */
    public ReceivedResult registerResult(SensorResult result, long resultReceivedAt) {
        Objects.requireNonNull(result, "result must not be null");
        boolean duplicate = !seenResults.add(new ResultKey(result.runId(), result.eventId()));
        long latencyMs = resultReceivedAt - result.sentAt();
        ReceivedResult received = new ReceivedResult(result, resultReceivedAt, latencyMs, duplicate);
        receivedResults.add(received);
        if (duplicate) {
            duplicateResultCount++;
        }
        return received;
    }

    /** Counts an alert independently of result deliveries. */
    public void registerAlert(SensorAlert alert) {
        Objects.requireNonNull(alert, "alert must not be null");
        receivedAlertCount++;
    }

    /** Includes both unique results and duplicate deliveries. */
    public long getReceivedResultCount() {
        return receivedResults.size();
    }

    public long getUniqueResultCount() {
        return seenResults.size();
    }

    public long getDuplicateResultCount() {
        return duplicateResultCount;
    }

    public long getReceivedAlertCount() {
        return receivedAlertCount;
    }

    /** Returns an immutable snapshot in delivery order. */
    public List<ReceivedResult> getReceivedResults() {
        return List.copyOf(receivedResults);
    }

    /** Retains each delivery's original result, timestamp, latency, and duplicate flag. */
    public record ReceivedResult(
            SensorResult result,
            long resultReceivedAt,
            long latencyMs,
            boolean duplicate
    ) {
    }

    private record ResultKey(String runId, String eventId) {
    }
}
