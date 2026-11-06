package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafkaiot.streams.config.StreamsConfig;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.state.StateStoreDefinitions;
import com.kafkaiot.streams.topology.SensorTopology;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorTopologyTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long HIGH_FREQUENCY_START = 1_700_000_000_000L;
    private static final long HIGH_FREQUENCY_WINDOW_MS = 1_000L;

    @TempDir
    Path tempDir;

    @Test
    void producesOneResultPerEventAndAnAlertOnlyAboveTheThreshold()
            throws JsonProcessingException {
        SensorEvent[] events = {
                new SensorEvent("event-001", "topology-test", "device-001",
                        25.0, 1_700_000_000_100L, 1_700_000_001_100L),
                new SensorEvent("event-002", "topology-test", "device-002",
                        30.0, 1_700_000_000_200L, 1_700_000_001_200L),
                new SensorEvent("event-003", "topology-test", "device-003",
                        32.5, 1_700_000_000_300L, 1_700_000_001_300L)
        };
        boolean[] triggered = {false, false, true};

        try (TopologyTestDriver driver = newDriver()) {
            TestInputTopic<String, String> input = driver.createInputTopic("sensor-events",
                    new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> results = driver.createOutputTopic("sensor-results",
                    new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> alerts = driver.createOutputTopic("sensor-alerts",
                    new StringDeserializer(), new StringDeserializer());

            for (SensorEvent event : events) {
                input.pipeInput(event.deviceId(), OBJECT_MAPPER.writeValueAsString(event),
                        Instant.ofEpochMilli(event.eventTime()));
            }

            assertEquals(3L, results.getQueueSize());
            assertEquals(1L, alerts.getQueueSize());
            for (int i = 0; i < events.length; i++) {
                KeyValue<String, String> result = results.readKeyValue();
                assertEquals(events[i].deviceId(), result.key);
                ObjectNode expected = expectedFields(events[i]).put("triggered", triggered[i]);
                assertEquals(expected, OBJECT_MAPPER.readTree(result.value));
            }

            KeyValue<String, String> alert = alerts.readKeyValue();
            assertEquals("device-003", alert.key);
            assertEquals(expectedFields(events[2]), OBJECT_MAPPER.readTree(alert.value));
            assertTrue(results.isEmpty());
            assertTrue(alerts.isEmpty());
        }
    }

    @Test
    void rejectsNonFiniteThresholds() {
        for (double threshold : new double[]{Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> SensorTopology.build(threshold));
        }
    }

    @Test
    void malformedJsonFailsProcessingWithoutProducingOutputs() {
        try (TopologyTestDriver driver = newDriver()) {
            TestInputTopic<String, String> input = driver.createInputTopic("sensor-events",
                    new StringSerializer(), new StringSerializer());
            TestOutputTopic<String, String> results = driver.createOutputTopic("sensor-results",
                    new StringDeserializer(), new StringDeserializer());
            TestOutputTopic<String, String> alerts = driver.createOutputTopic("sensor-alerts",
                    new StringDeserializer(), new StringDeserializer());

            StreamsException failure = assertThrows(StreamsException.class,
                    () -> input.pipeInput("device-001", "{\"eventId\":",
                            Instant.ofEpochMilli(1_700_000_000_100L)));
            Throwable cause = failure;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertInstanceOf(JsonProcessingException.class, cause);
            assertTrue(results.isEmpty());
            assertTrue(alerts.isEmpty());
        }
    }

    @Test
    void highFrequencyProducesExactResultsAndRepeatedAlertsAboveThreshold()
            throws JsonProcessingException {
        SensorEvent[] events = {
                highFrequencyEvent("event-1", "run-1", "device-1", 10),
                highFrequencyEvent("event-2", "run-1", "device-1", 20),
                highFrequencyEvent("event-3", "run-1", "device-1", 30),
                highFrequencyEvent("event-4", "run-1", "device-1", 40)
        };
        try (TopologyTestDriver driver = newHighFrequencyDriver(2)) {
            assertHighFrequencyOutputs(driver, events, new boolean[]{false, false, true, true}, null);
            KeyValueStore<String, String> state = driver.getKeyValueStore(
                    StateStoreDefinitions.HIGH_FREQUENCY_STORE);
            assertTrue(state.persistent());
            assertEquals(HIGH_FREQUENCY_START / HIGH_FREQUENCY_WINDOW_MS + ":4",
                    state.get("5:run-1device-1"));
        }
    }

    @Test
    void highFrequencyResetsResultsAndAlertsAtHalfOpenWindowBoundary()
            throws JsonProcessingException {
        SensorEvent[] events = {
                highFrequencyEvent("event-1", "run-1", "device-1", 0),
                highFrequencyEvent("event-2", "run-1", "device-1", 999),
                highFrequencyEvent("event-3", "run-1", "device-1", 1_000),
                highFrequencyEvent("event-4", "run-1", "device-1", 1_001)
        };
        try (TopologyTestDriver driver = newHighFrequencyDriver(1)) {
            assertHighFrequencyOutputs(driver, events, new boolean[]{false, true, false, true}, null);
            KeyValueStore<String, String> state = driver.getKeyValueStore(
                    StateStoreDefinitions.HIGH_FREQUENCY_STORE);
            assertEquals((HIGH_FREQUENCY_START / HIGH_FREQUENCY_WINDOW_MS + 1) + ":2",
                    state.get("5:run-1device-1"));
        }
    }

    @Test
    void highFrequencyKeepsDeviceResultAndAlertCountsIndependent() throws JsonProcessingException {
        SensorEvent[] events = {
                highFrequencyEvent("event-1", "run-1", "device-1", 10),
                highFrequencyEvent("event-2", "run-1", "device-2", 20),
                highFrequencyEvent("event-3", "run-1", "device-1", 30),
                highFrequencyEvent("event-4", "run-1", "device-2", 40)
        };
        try (TopologyTestDriver driver = newHighFrequencyDriver(1)) {
            assertHighFrequencyOutputs(driver, events, new boolean[]{false, false, true, true}, null);
        }
    }

    @Test
    void highFrequencyKeepsRunsIndependentForSameDevice() throws JsonProcessingException {
        SensorEvent[] events = {
                highFrequencyEvent("event-1", "run-1", "device-1", 10),
                highFrequencyEvent("event-2", "run-2", "device-1", 20),
                highFrequencyEvent("event-3", "run-1", "device-1", 30),
                highFrequencyEvent("event-4", "run-2", "device-1", 40)
        };
        try (TopologyTestDriver driver = newHighFrequencyDriver(1)) {
            assertHighFrequencyOutputs(driver, events, new boolean[]{false, false, true, true}, null);
        }
    }

    @Test
    void highFrequencyUsesPayloadTimeForResultAndAlertWindowAssignment()
            throws JsonProcessingException {
        SensorEvent[] events = {
                highFrequencyEvent("event-1", "run-1", "device-1", 10),
                highFrequencyEvent("event-2", "run-1", "device-1", 20),
                highFrequencyEvent("event-3", "run-1", "device-1", 1_000),
                highFrequencyEvent("event-4", "run-1", "device-1", 1_001)
        };
        long[] kafkaTimestamps = {
                HIGH_FREQUENCY_START + 10_000, HIGH_FREQUENCY_START + 20_000,
                HIGH_FREQUENCY_START + 20_001, HIGH_FREQUENCY_START + 30_000
        };
        try (TopologyTestDriver driver = newHighFrequencyDriver(1)) {
            assertHighFrequencyOutputs(driver, events, new boolean[]{false, true, false, true},
                    kafkaTimestamps);
        }
    }

    @Test
    void highFrequencyTopologyRejectsNonPositiveWindowDurations() {
        for (long duration : new long[]{0, -1, Long.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class,
                    () -> SensorTopology.buildHighFrequency(duration, 1));
        }
    }

    @Test
    void highFrequencyTopologyRejectsNonPositiveCountThresholds() {
        for (int threshold : new int[]{0, -1, Integer.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class,
                    () -> SensorTopology.buildHighFrequency(HIGH_FREQUENCY_WINDOW_MS, threshold));
        }
    }

    private TopologyTestDriver newHighFrequencyDriver(int threshold) {
        Properties properties = StreamsConfig.createProperties("unused:9092", "high-frequency-topology-test");
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG, tempDir.toString());
        return new TopologyTestDriver(
                SensorTopology.buildHighFrequency(HIGH_FREQUENCY_WINDOW_MS, threshold), properties,
                Instant.ofEpochMilli(HIGH_FREQUENCY_START));
    }

    private static SensorEvent highFrequencyEvent(String eventId, String runId, String deviceId,
                                                  long offsetMs) {
        return new SensorEvent(eventId, runId, deviceId, 25.0,
                HIGH_FREQUENCY_START + offsetMs, HIGH_FREQUENCY_START + offsetMs + 100);
    }

    private static void assertHighFrequencyOutputs(TopologyTestDriver driver, SensorEvent[] events,
                                                   boolean[] triggered, long[] kafkaTimestamps)
            throws JsonProcessingException {
        TestInputTopic<String, String> input = driver.createInputTopic("sensor-events",
                new StringSerializer(), new StringSerializer());
        TestOutputTopic<String, String> results = driver.createOutputTopic("sensor-results",
                new StringDeserializer(), new StringDeserializer());
        TestOutputTopic<String, String> alerts = driver.createOutputTopic("sensor-alerts",
                new StringDeserializer(), new StringDeserializer());
        for (int i = 0; i < events.length; i++) {
            SensorEvent event = events[i];
            // Deliberately differ from deviceId to detect accidental output rekeying.
            String key = "input-key-" + event.deviceId();
            long timestamp = kafkaTimestamps == null ? event.eventTime() : kafkaTimestamps[i];
            input.pipeInput(key, OBJECT_MAPPER.writeValueAsString(event), Instant.ofEpochMilli(timestamp));

            assertEquals(1L, results.getQueueSize());
            assertEquals(triggered[i] ? 1L : 0L, alerts.getQueueSize());
            ObjectNode expectedAlert = OBJECT_MAPPER.createObjectNode()
                    .put("eventId", event.eventId()).put("runId", event.runId())
                    .put("deviceId", event.deviceId()).put("eventTime", event.eventTime())
                    .put("sentAt", event.sentAt()).put("ruleType", "HIGH_FREQUENCY");
            KeyValue<String, String> result = results.readKeyValue();
            assertEquals(key, result.key);
            assertEquals(expectedAlert.deepCopy().put("triggered", triggered[i]),
                    OBJECT_MAPPER.readTree(result.value));
            if (triggered[i]) {
                KeyValue<String, String> alert = alerts.readKeyValue();
                assertEquals(key, alert.key);
                assertEquals(expectedAlert, OBJECT_MAPPER.readTree(alert.value));
            }
            assertTrue(results.isEmpty());
            assertTrue(alerts.isEmpty());
        }
    }

    private TopologyTestDriver newDriver() {
        Properties properties = StreamsConfig.createProperties("unused:9092", "topology-test");
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG,
                tempDir.toString());
        return new TopologyTestDriver(SensorTopology.build(30.0), properties,
                Instant.ofEpochMilli(1_700_000_000_000L));
    }

    private static ObjectNode expectedFields(SensorEvent event) {
        return OBJECT_MAPPER.createObjectNode()
                .put("eventId", event.eventId())
                .put("runId", event.runId())
                .put("deviceId", event.deviceId())
                .put("eventTime", event.eventTime())
                .put("sentAt", event.sentAt())
                .put("ruleType", "HIGH_TEMPERATURE");
    }
}
