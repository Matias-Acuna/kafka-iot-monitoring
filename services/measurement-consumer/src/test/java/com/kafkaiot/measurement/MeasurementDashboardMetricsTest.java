package com.kafkaiot.measurement;

import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.model.SensorAlert;
import com.kafkaiot.measurement.model.SensorResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class MeasurementDashboardMetricsTest {
    private static final long SECOND = 1_700_000_000L;
    private static SensorResult result(String run, String event, long sent) {
        return new SensorResult(event, run, "device", 123, sent, "RAPID_CHANGE", false);
    }
    private static MeasurementAggregator enabled() {
        var aggregator = new MeasurementAggregator();
        aggregator.enableDashboard();
        return aggregator;
    }

    @Test
    void disabledDashboardPreservesScientificObservations() {
        var aggregator = new MeasurementAggregator();
        var received = aggregator.registerResult(result("r", "e", 10), 5);
        assertEquals(-5, received.latencyMs());
        assertEquals(1, aggregator.getReceivedResultCount());
        assertThrows(IllegalStateException.class, aggregator::dashboardSnapshot);
        assertThrows(IllegalStateException.class, aggregator::enableDashboard);
    }

    @Test
    void throughputUsesRealReceptionSecondsAndRetainsMeasuredEmptySeconds() {
        var aggregator = enabled();
        aggregator.registerResult(result("r", "a", SECOND * 1000), SECOND * 1000 + 1);
        aggregator.registerResult(result("r", "a", SECOND * 1000), SECOND * 1000 + 2);
        var snapshot = aggregator.dashboardSnapshot((SECOND + 2) * 1000);
        assertEquals(2L, snapshot.totalResults());
        assertEquals(1L, snapshot.uniqueResults());
        assertEquals(1L, snapshot.duplicateResults());
        assertEquals(2, snapshot.throughputSeries().size());
        assertEquals(2.0, snapshot.throughputSeries().getFirst().value());
        assertEquals(0.0, snapshot.resultThroughputPerSecond());
        assertEquals((SECOND + 1) * 1000, snapshot.throughputSeries().getLast().timestamp());
        assertEquals(SECOND * 1000 + 2, snapshot.lastUpdatedAt());
    }

    @Test
    void percentilesUseTypeSevenAndPreserveNegativeScientificLatencies() {
        var aggregator = enabled();
        long at = SECOND * 1000 + 100;
        aggregator.registerResult(result("r", "a", at - 10), at);
        aggregator.registerResult(result("r", "b", at - 20), at);
        aggregator.registerResult(result("r", "c", at - 30), at);
        aggregator.registerResult(result("r", "n", at + 5), at);
        aggregator.registerResult(result("r", "a", at - 10), at + 50);
        var snapshot = aggregator.dashboardSnapshot(at + 100);
        assertEquals(20.0, snapshot.latencyP50Ms());
        assertEquals(29.0, snapshot.latencyP95Ms());
        assertEquals(29.8, snapshot.latencyP99Ms(), 0.00001);
        assertEquals(1, snapshot.negativeLatencyObservations());
        assertEquals(5, aggregator.getReceivedResults().size());
        assertEquals(-5, aggregator.getReceivedResults().get(3).latencyMs());
        assertEquals(1, snapshot.latencySeries().size());
        assertNull(snapshot.resultThroughputPerSecond()); // Current second is not complete.
    }

    @Test
    void earlierDuplicateReceiptIsUsedWithoutChangingDuplicateAccounting() {
        var aggregator = enabled();
        long at = SECOND * 1000 + 500;
        var result = result("r", "e", at - 10);
        aggregator.registerResult(result, at + 100);
        aggregator.registerResult(result, at);
        var snapshot = aggregator.dashboardSnapshot(at + 200);
        assertEquals(10.0, snapshot.latencyP50Ms());
        assertEquals(1L, snapshot.duplicateResults());
        assertEquals(2, aggregator.getReceivedResults().size());
    }

    @Test
    void missingAndZeroLatencyAreDistinct() {
        var aggregator = enabled();
        assertNull(aggregator.dashboardSnapshot(SECOND * 1000).latencyP50Ms());
        long at = SECOND * 1000;
        aggregator.registerResult(result("r", "zero", at), at);
        assertEquals(0.0, aggregator.dashboardSnapshot(at).latencyP50Ms());
        var negative = enabled();
        negative.registerResult(result("r", "negative", at + 1), at);
        assertNull(negative.dashboardSnapshot(at).latencyP99Ms());
    }

    @Test
    void alertRulesAreCountedAndMultipleRunsAreNotAttributedToOneRun() {
        var aggregator = enabled();
        aggregator.registerAlert(new SensorAlert("a", "r1", "d", 1, 2, "HIGH_TEMPERATURE"));
        aggregator.registerAlert(new SensorAlert("b", "r2", "d", 1, 2, "HIGH_FREQUENCY"));
        var snapshot = aggregator.dashboardSnapshot();
        assertNull(snapshot.runId());
        assertNull(snapshot.observedRule());
        assertEquals("MULTIPLE_OR_UNKNOWN_RUNS", snapshot.scope());
        assertEquals(1L, snapshot.alertsByRule().get("HIGH_TEMPERATURE"));
        assertEquals(1L, snapshot.alertsByRule().get("HIGH_FREQUENCY"));
        assertEquals(0L, snapshot.alertsByRule().get("RAPID_CHANGE"));
        assertEquals(0L, snapshot.totalResults());
    }

    @Test
    void visualizationWindowIsBoundedWithoutDiscardingScientificHistory() {
        var aggregator = enabled();
        for (int i = 0; i < 200; i++) {
            long at = (SECOND + i) * 1000;
            aggregator.registerResult(result("r", "e" + i, at - 1), at);
        }
        var snapshot = aggregator.dashboardSnapshot((SECOND + 200) * 1000);
        assertEquals(120, snapshot.throughputSeries().size());
        assertTrue(snapshot.latencySeries().size() <= 120);
        assertEquals(200, aggregator.getReceivedResults().size());
        assertEquals(200L, snapshot.uniqueResults());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.throughputSeries().clear());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.alertsByRule().put("RAPID_CHANGE", 10L));
    }

    @Test
    void repeatedUnchangedReadsUseCachedSnapshot() {
        var aggregator = enabled();
        long at = SECOND * 1000;
        aggregator.registerResult(result("r", "e", at), at);
        var snapshot = aggregator.dashboardSnapshot(at);
        assertSame(snapshot, aggregator.dashboardSnapshot(at + 1));
        aggregator.registerResult(result("r", "f", at), at + 2);
        assertNotSame(snapshot, aggregator.dashboardSnapshot(at + 3));
    }

    @Test
    void concurrentReadsSeeConsistentCountersWhilePollingRegistersResults() throws Exception {
        var aggregator = enabled();
        long at = SECOND * 1000;
        try (var executor = Executors.newFixedThreadPool(2)) {
            var writer = executor.submit(() -> {
                for (int i = 0; i < 500; i++) aggregator.registerResult(result("r", "e" + i, at), at + i);
            });
            var reader = executor.submit(() -> {
                for (int i = 0; i < 100; i++) {
                    var snapshot = aggregator.dashboardSnapshot(at + 1000);
                    if (snapshot.totalResults() != null) {
                        assertEquals(snapshot.totalResults().longValue(), snapshot.uniqueResults() + snapshot.duplicateResults());
                    }
                }
            });
            writer.get(5, TimeUnit.SECONDS);
            reader.get(5, TimeUnit.SECONDS);
        }
        assertEquals(500, aggregator.getReceivedResultCount());
    }
}
