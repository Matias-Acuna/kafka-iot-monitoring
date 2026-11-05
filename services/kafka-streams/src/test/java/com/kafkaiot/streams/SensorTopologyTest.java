package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafkaiot.streams.config.StreamsConfig;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.topology.SensorTopology;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
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
