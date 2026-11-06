package com.kafkaiot.measurement.metrics;

import com.kafkaiot.measurement.model.SensorAlert;
import com.kafkaiot.measurement.model.SensorResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Preserves scientific observations, with optional bounded live dashboard snapshots. */
public final class MeasurementAggregator {
    private final Set<ResultKey> seenResults = new HashSet<>();
    private final List<ReceivedResult> receivedResults = new ArrayList<>();
    private long duplicateResultCount;
    private long receivedAlertCount;
    private LiveDashboard live;
    private DashboardCache dashboardCache;
    private long dashboardVersion;
    private static final int DASHBOARD_SECONDS = 120;

    /** Registers a delivery using the caller's Unix epoch millisecond timestamp. */
    public synchronized ReceivedResult registerResult(SensorResult result, long resultReceivedAt) {
        Objects.requireNonNull(result, "result must not be null");
        boolean duplicate = !seenResults.add(new ResultKey(result.runId(), result.eventId()));
        long latencyMs = resultReceivedAt - result.sentAt();
        ReceivedResult received = new ReceivedResult(result, resultReceivedAt, latencyMs, duplicate);
        receivedResults.add(received);
        if (duplicate) {
            duplicateResultCount++;
        }
        if (live != null) {
            live.result(received);
            dashboardVersion++;
        }
        return received;
    }

    /** Counts an alert independently of result deliveries. */
    public synchronized void registerAlert(SensorAlert alert) {
        Objects.requireNonNull(alert, "alert must not be null");
        receivedAlertCount++;
        if (live != null) {
            live.observe(alert.runId(), alert.ruleType(), System.currentTimeMillis());
            if ("HIGH_TEMPERATURE".equals(alert.ruleType()) || "HIGH_FREQUENCY".equals(alert.ruleType())
                    || "RAPID_CHANGE".equals(alert.ruleType())) live.alerts.merge(alert.ruleType(), 1L, Long::sum);
            dashboardVersion++;
        }
    }

    /** Includes both unique results and duplicate deliveries. */
    public synchronized long getReceivedResultCount() {
        return receivedResults.size();
    }

    public synchronized long getUniqueResultCount() {
        return seenResults.size();
    }

    public synchronized long getDuplicateResultCount() {
        return duplicateResultCount;
    }

    public synchronized long getReceivedAlertCount() {
        return receivedAlertCount;
    }

    /** Returns an immutable snapshot in delivery order. */
    public synchronized List<ReceivedResult> getReceivedResults() {
        return List.copyOf(receivedResults);
    }

    /** Enable before consumption; no history scan or dashboard work in ordinary FD6 runs. */
    public synchronized void enableDashboard() {
        if (live != null) return;
        if (!receivedResults.isEmpty() || receivedAlertCount != 0) {
            throw new IllegalStateException("Dashboard must be enabled before receiving observations");
        }
        live = new LiveDashboard();
    }

    public record ThroughputPoint(long timestamp, double value) { }
    public record LatencyPoint(long timestamp, Double p50Ms, Double p95Ms, Double p99Ms) { }
    public record DashboardSnapshot(String runId, String observedRule, Long totalResults, Long uniqueResults,
            Long duplicateResults, Long totalAlerts, Map<String, Long> alertsByRule,
            Double resultThroughputPerSecond, Double latencyP50Ms, Double latencyP95Ms, Double latencyP99Ms,
            List<ThroughputPoint> throughputSeries, List<LatencyPoint> latencySeries, Long lastUpdatedAt,
            String scope, String throughputPopulation, String latencyPopulation, long negativeLatencyObservations) { }
    private record DashboardCache(long version, long second, DashboardSnapshot snapshot) { }
    private record DashboardInput(long version, long second, String runId, String rule, String scope,
            Long updated, long results, long unique, long duplicates, long alerts, Map<String, Long> byRule,
            List<ReceivedResult> latencies, List<ThroughputPoint> throughput) { }

    public DashboardSnapshot dashboardSnapshot() {
        return dashboardSnapshot(System.currentTimeMillis());
    }

    /** Copy only the 120-second visualization window; sort outside the polling lock. */
    public DashboardSnapshot dashboardSnapshot(long nowEpochMs) {
        DashboardInput input;
        synchronized (this) {
            if (live == null) throw new IllegalStateException("Dashboard is disabled");
            long second = Math.floorDiv(nowEpochMs, 1000);
            if (dashboardCache != null && dashboardCache.version() == dashboardVersion
                    && dashboardCache.second() == second) return dashboardCache.snapshot();
            live.prune(second);
            List<ThroughputPoint> throughput = new ArrayList<>();
            if (live.firstResultSecond != null) {
                for (long at = Math.max(live.firstResultSecond, second - DASHBOARD_SECONDS); at < second; at++) {
                    throughput.add(new ThroughputPoint(at * 1000, live.deliveries.getOrDefault(at, 0L)));
                }
            }
            input = new DashboardInput(dashboardVersion, second, live.mixedRuns ? null : live.runId,
                    live.mixedRules ? null : live.rule, live.mixedRuns ? "MULTIPLE_OR_UNKNOWN_RUNS" : "SINGLE_OBSERVED_RUN",
                    live.updated, receivedResults.size(), seenResults.size(), duplicateResultCount, receivedAlertCount,
                    new HashMap<>(live.alerts), List.copyOf(live.earliest.values()), List.copyOf(throughput));
        }
        List<Long> values = new ArrayList<>();
        Map<Long, List<Long>> perSecond = new TreeMap<>();
        long negatives = 0;
        for (ReceivedResult received : input.latencies()) {
            long second = Math.floorDiv(received.resultReceivedAt(), 1000);
            List<Long> bucket = perSecond.computeIfAbsent(second, unused -> new ArrayList<>());
            if (received.latencyMs() < 0) negatives++;
            else {
                values.add(received.latencyMs());
                bucket.add(received.latencyMs());
            }
        }
        values.sort(Long::compare);
        List<LatencyPoint> latencySeries = new ArrayList<>();
        perSecond.forEach((second, bucket) -> {
            bucket.sort(Long::compare);
            latencySeries.add(new LatencyPoint(second * 1000, percentile(bucket, 50), percentile(bucket, 95), percentile(bucket, 99)));
        });
        Map<String, Long> alertsByRule = new HashMap<>();
        for (String rule : List.of("HIGH_TEMPERATURE", "HIGH_FREQUENCY", "RAPID_CHANGE")) {
            alertsByRule.put(rule, input.updated() == null ? null : input.byRule().getOrDefault(rule, 0L));
        }
        boolean available = input.updated() != null;
        DashboardSnapshot snapshot = new DashboardSnapshot(input.runId(), input.rule(),
                available ? input.results() : null, available ? input.unique() : null,
                available ? input.duplicates() : null, available ? input.alerts() : null,
                java.util.Collections.unmodifiableMap(alertsByRule),
                input.throughput().isEmpty() ? null : input.throughput().getLast().value(),
                percentile(values, 50), percentile(values, 95), percentile(values, 99),
                input.throughput(), List.copyOf(latencySeries), input.updated(),
                available ? input.scope() : "NO_OBSERVATIONS",
                "All received result deliveries, including duplicates, per completed UTC second; last 120 seconds. No experimental phase filter.",
                "Unique results whose earliest retained receipt is in the current 120-second UTC window; Type 7 over nonnegative latencyMs. Scientific exports are unchanged.",
                negatives);
        synchronized (this) {
            if (dashboardVersion == input.version()) dashboardCache = new DashboardCache(input.version(), input.second(), snapshot);
        }
        return snapshot;
    }

    private static Double percentile(List<Long> sorted, int percent) {
        if (sorted.isEmpty()) return null;
        long numerator = (long) (sorted.size() - 1) * percent;
        int lower = (int) (numerator / 100);
        long remainder = numerator % 100;
        double value = sorted.get(lower);
        return remainder == 0 ? value : value + ((double) sorted.get(lower + 1) - value) * remainder / 100.0;
    }

    private static final class LiveDashboard {
        private String runId;
        private String rule;
        private boolean mixedRuns;
        private boolean mixedRules;
        private Long updated;
        private Long firstResultSecond;
        private long windowSecond = Long.MIN_VALUE;
        private final Map<String, Long> alerts = new HashMap<>();
        private final TreeMap<Long, Long> deliveries = new TreeMap<>();
        private final TreeMap<Long, Map<ResultKey, ReceivedResult>> samples = new TreeMap<>();
        private final Map<ResultKey, ReceivedResult> earliest = new HashMap<>();

        private void observe(String observedRun, String observedRule, long at) {
            if (updated == null) { runId = observedRun; rule = observedRule; }
            mixedRuns |= observedRun == null || observedRun.isBlank() || !Objects.equals(runId, observedRun);
            mixedRules |= observedRule == null || observedRule.isBlank() || !Objects.equals(rule, observedRule);
            updated = updated == null ? at : Math.max(updated, at);
        }

        private void result(ReceivedResult received) {
            SensorResult result = received.result();
            observe(result.runId(), result.ruleType(), received.resultReceivedAt());
            long second = Math.floorDiv(received.resultReceivedAt(), 1000);
            prune(second);
            firstResultSecond = firstResultSecond == null ? second : Math.min(firstResultSecond, second);
            ResultKey key = new ResultKey(result.runId(), result.eventId());
            ReceivedResult previous = earliest.get(key);
            if (previous != null && received.resultReceivedAt() < previous.resultReceivedAt()) {
                samples.get(Math.floorDiv(previous.resultReceivedAt(), 1000)).remove(key);
                earliest.remove(key);
            }
            if (second >= windowSecond - DASHBOARD_SECONDS) deliveries.merge(second, 1L, Long::sum);
            if (second < windowSecond - DASHBOARD_SECONDS + 1) return;
            if ((!received.duplicate() || previous != null) && !earliest.containsKey(key)) {
                earliest.put(key, received);
                samples.computeIfAbsent(second, unused -> new HashMap<>()).put(key, received);
            }
        }

        private void prune(long second) {
            windowSecond = Math.max(windowSecond, second);
            // Retain 120 completed buckets plus the current partial bucket.
            long oldest = windowSecond - DASHBOARD_SECONDS;
            deliveries.headMap(oldest, false).clear();
            while (!samples.isEmpty() && samples.firstKey() < oldest + 1) {
                samples.pollFirstEntry().getValue().forEach((key, value) -> earliest.remove(key, value));
            }
        }
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
