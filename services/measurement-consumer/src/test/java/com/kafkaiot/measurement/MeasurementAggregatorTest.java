package com.kafkaiot.measurement;

import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.model.SensorAlert;
import com.kafkaiot.measurement.model.SensorResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementAggregatorTest {
    private static final long SENT_AT = 1_700_000_000_456L;

    @Test
    void countsUniqueResultWithoutCountingDuplicate() {
        var aggregator = new MeasurementAggregator();

        var received = aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 25);

        assertEquals(1, aggregator.getReceivedResultCount());
        assertEquals(1, aggregator.getUniqueResultCount());
        assertEquals(0, aggregator.getDuplicateResultCount());
        assertEquals(0, aggregator.getReceivedAlertCount());
        assertFalse(received.duplicate());
    }

    @Test
    void countsSecondDeliveryOfSameRunAndEventAsDuplicate() {
        var aggregator = new MeasurementAggregator();
        aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 25);

        var duplicate = aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 50);

        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(1, aggregator.getUniqueResultCount());
        assertEquals(1, aggregator.getDuplicateResultCount());
        assertTrue(duplicate.duplicate());
    }

    @Test
    void treatsSameEventIdInDifferentRunsAsUnique() {
        var aggregator = new MeasurementAggregator();
        aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 25);

        var received = aggregator.registerResult(result("run-2", "event-1"), SENT_AT + 50);

        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(2, aggregator.getUniqueResultCount());
        assertEquals(0, aggregator.getDuplicateResultCount());
        assertFalse(received.duplicate());
    }

    @Test
    void treatsDifferentEventIdsInSameRunAsUnique() {
        var aggregator = new MeasurementAggregator();
        aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 25);

        var received = aggregator.registerResult(result("run-1", "event-2"), SENT_AT + 50);

        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(2, aggregator.getUniqueResultCount());
        assertEquals(0, aggregator.getDuplicateResultCount());
        assertFalse(received.duplicate());
    }

    @Test
    void calculatesPositiveLatency() {
        var aggregator = new MeasurementAggregator();

        var received = aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 125);

        assertEquals(125, received.latencyMs());
    }

    @Test
    void calculatesZeroLatency() {
        var aggregator = new MeasurementAggregator();

        var received = aggregator.registerResult(result("run-1", "event-1"), SENT_AT);

        assertEquals(0, received.latencyMs());
    }

    @Test
    void preservesNegativeLatency() {
        var aggregator = new MeasurementAggregator();

        var received = aggregator.registerResult(result("run-1", "event-1"), SENT_AT - 75);

        assertEquals(-75, received.latencyMs());
        assertEquals(List.of(received), aggregator.getReceivedResults());
        assertEquals(1, aggregator.getReceivedResultCount());
    }

    @Test
    void preservesOriginalResultTimestampLatencyAndDuplicateFlagForEachDelivery() {
        var aggregator = new MeasurementAggregator();
        var original = result("run-1", "event-1");
        var redelivered = result("run-1", "event-1");

        var first = aggregator.registerResult(original, SENT_AT + 25);
        var second = aggregator.registerResult(redelivered, SENT_AT + 50);

        assertSame(original, first.result());
        assertEquals(SENT_AT + 25, first.resultReceivedAt());
        assertEquals(25, first.latencyMs());
        assertFalse(first.duplicate());
        assertSame(redelivered, second.result());
        assertEquals(SENT_AT + 50, second.resultReceivedAt());
        assertEquals(50, second.latencyMs());
        assertTrue(second.duplicate());
    }

    @Test
    void countsAlertsIndependentlyOfResults() {
        var aggregator = new MeasurementAggregator();
        var alert = new SensorAlert("event-1", "run-1", "device-001",
                1_700_000_000_123L, SENT_AT, "HIGH_TEMPERATURE");

        aggregator.registerAlert(alert);

        assertEquals(1, aggregator.getReceivedAlertCount());
        assertEquals(0, aggregator.getReceivedResultCount());
        assertEquals(0, aggregator.getUniqueResultCount());
        assertEquals(0, aggregator.getDuplicateResultCount());

        aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 25);
        aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 50);
        assertEquals(1, aggregator.getReceivedAlertCount());

        aggregator.registerAlert(alert);

        assertEquals(2, aggregator.getReceivedAlertCount());
        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(1, aggregator.getUniqueResultCount());
        assertEquals(1, aggregator.getDuplicateResultCount());
    }

    @Test
    void returnsImmutableSnapshotInDeliveryOrder() {
        var aggregator = new MeasurementAggregator();
        var first = aggregator.registerResult(result("run-1", "event-2"), SENT_AT + 50);
        var second = aggregator.registerResult(result("run-1", "event-1"), SENT_AT + 25);
        var snapshot = aggregator.getReceivedResults();

        var third = aggregator.registerResult(result("run-1", "event-2"), SENT_AT + 75);

        assertEquals(List.of(first, second), snapshot);
        assertEquals(List.of(first, second, third), aggregator.getReceivedResults());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(third));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.set(0, second));
        assertThrows(UnsupportedOperationException.class, () -> snapshot.remove(0));
        assertEquals(List.of(first, second, third), aggregator.getReceivedResults());
    }

    @Test
    void rejectsNullResultWithoutChangingState() {
        var aggregator = new MeasurementAggregator();

        assertThrows(NullPointerException.class, () -> aggregator.registerResult(null, SENT_AT));

        assertEquals(0, aggregator.getReceivedResultCount());
        assertEquals(0, aggregator.getUniqueResultCount());
        assertEquals(0, aggregator.getDuplicateResultCount());
        assertEquals(0, aggregator.getReceivedAlertCount());
        assertTrue(aggregator.getReceivedResults().isEmpty());
    }

    @Test
    void rejectsNullAlertWithoutChangingState() {
        var aggregator = new MeasurementAggregator();

        assertThrows(NullPointerException.class, () -> aggregator.registerAlert(null));

        assertEquals(0, aggregator.getReceivedAlertCount());
        assertEquals(0, aggregator.getReceivedResultCount());
        assertEquals(0, aggregator.getUniqueResultCount());
        assertEquals(0, aggregator.getDuplicateResultCount());
        assertTrue(aggregator.getReceivedResults().isEmpty());
    }

    private static SensorResult result(String runId, String eventId) {
        return new SensorResult(eventId, runId, "device-001",
                1_700_000_000_123L, SENT_AT, "HIGH_TEMPERATURE", true);
    }
}
