package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.streams.config.StreamsConfig;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.model.SensorResult;
import com.kafkaiot.streams.rules.RapidChangeProcessor;
import com.kafkaiot.streams.state.StateStoreDefinitions;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RapidChangeProcessorTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long START = 1_700_000_000_000L;
    private static final long D = 1_000L;
    private static final double C = 5.0;

    @TempDir
    Path tempDir;

    @Test
    void firstEventDoesNotTriggerAndPersistsMeasurement() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertTrue(store(driver).persistent());
            assertSequence(driver, new long[]{START}, new double[]{20}, new boolean[]{false});
        }
    }

    @Test
    void triggersForBothTemperatureIncreaseAndDecreaseWithinTimeLimit() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START, START + 10, START + 20},
                    new double[]{20, 30, 10}, new boolean[]{false, true, true});
        }
    }

    @Test
    void timeBoundaryIsInclusive() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START, START + D},
                    new double[]{20, 30}, new boolean[]{false, true});
        }
    }

    @Test
    void temperatureDifferenceEqualToThresholdDoesNotTrigger() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START, START + 10, START + 20},
                    new double[]{20, 25, 20}, new boolean[]{false, false, false});
        }
    }

    @Test
    void excessiveTimeGapDoesNotTriggerButUpdatesPreviousMeasurement() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START, START + D + 1, START + D + 2},
                    new double[]{20, 40, 44}, new boolean[]{false, false, false});
        }
    }

    @Test
    void nonTriggeringEventBecomesImmediatePredecessor() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            // The last event differs by 8 from the first, but only 4 from its predecessor.
            assertSequence(driver, new long[]{START, START + 10, START + 20},
                    new double[]{20, 24, 28}, new boolean[]{false, false, false});
        }
    }

    @Test
    void triggeringEventBecomesImmediatePredecessor() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START, START + 10, START + 20},
                    new double[]{20, 30, 34}, new boolean[]{false, true, false});
        }
    }

    @Test
    void isolatesDevices() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            var input = input(driver);
            var output = output(driver);
            checkDelivery(driver, input, output, event("e1", "run-1", "device-1", START, 20), false);
            checkDelivery(driver, input, output, event("e2", "run-1", "device-2", START + 10, 100), false);
            checkDelivery(driver, input, output, event("e3", "run-1", "device-1", START + 20, 24), false);
            checkDelivery(driver, input, output, event("e4", "run-1", "device-2", START + 30, 110), true);
            assertEquals((START + 20) + ":24.0", store(driver).get("5:run-1device-1"));
        }
    }

    @Test
    void isolatesRunsSharingDevice() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            var input = input(driver);
            var output = output(driver);
            checkDelivery(driver, input, output, event("e1", "run-1", "device-1", START, 20), false);
            checkDelivery(driver, input, output, event("e2", "run-2", "device-1", START + 10, 100), false);
            checkDelivery(driver, input, output, event("e3", "run-1", "device-1", START + 20, 24), false);
            checkDelivery(driver, input, output, event("e4", "run-2", "device-1", START + 30, 110), true);
            assertEquals((START + 20) + ":24.0", store(driver).get("5:run-1device-1"));
        }
    }

    @Test
    void compositeKeysDoNotCollideWhenIdentifiersContainDelimiters() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            var input = input(driver);
            var output = output(driver);
            checkDelivery(driver, input, output, event("e1", "a:b", "c", START, 20), false);
            checkDelivery(driver, input, output, event("e2", "a", "b:c", START + 10, 100), false);
            checkDelivery(driver, input, output, event("e3", "a:b", "c", START + 20, 24), false);
            assertEquals((START + 20) + ":24.0", store(driver).get("3:a:bc"));
            assertEquals((START + 10) + ":100.0", store(driver).get("1:ab:c"));
        }
    }

    @Test
    void usesPayloadEventTimeInsteadOfKafkaTimestamp() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            var input = input(driver);
            var output = output(driver);
            input.pipeInput("key", json(event("e1", "run-1", "device-1", START, 20)),
                    Instant.ofEpochMilli(START));
            input.pipeInput("key", json(event("e2", "run-1", "device-1", START + 10, 30)),
                    Instant.ofEpochMilli(START + 10_000));
            input.pipeInput("key", json(event("e3", "run-1", "device-1", START + D + 11, 50)),
                    Instant.ofEpochMilli(START + 10_001));
            assertEquals(3L, output.getQueueSize());
            assertFalse(readResult(output).triggered());
            assertTrue(readResult(output).triggered());
            assertFalse(readResult(output).triggered());
            assertTrue(output.isEmpty());
        }
    }

    @Test
    void preservesExactResultFieldsAndKafkaKeyWithoutDirectAlerts() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            var input = input(driver);
            var output = output(driver);
            checkDelivery(driver, input, output,
                    event("event-\"1", "run-1", "device-1", START, 20), false);
            checkDelivery(driver, input, output,
                    event("event-2", "run-1", "device-1", START + 10, 30), true);
            assertFalse(driver.producedTopicNames().contains("sensor-alerts"));
        }
    }

    @Test
    void rejectsNonPositiveTimeLimits() {
        for (long value : new long[]{0, -1, Long.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> new RapidChangeProcessor(value, C));
        }
    }

    @Test
    void rejectsNonPositiveOrNonfiniteTemperatureThresholds() {
        for (double value : new double[]{0, -0.0, -1, Double.NaN,
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class, () -> new RapidChangeProcessor(D, value));
        }
    }

    @Test
    void rejectsNonfiniteInputTemperaturesWithoutStateUpdateOrResult() throws Exception {
        for (double value : new double[]{Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            try (TopologyTestDriver driver = newDriver(D, C)) {
                var input = input(driver);
                var output = output(driver);
                checkDelivery(driver, input, output, event("e1", "run-1", "device-1", START, 20), false);
                String invalid = json(event("e2", "run-1", "device-1", START + 10, value));
                StreamsException failure = assertThrows(StreamsException.class,
                        () -> input.pipeInput("key", invalid, Instant.ofEpochMilli(START + 10)));
                assertCause(failure, IllegalArgumentException.class, "temperature must be finite");
                assertEquals(START + ":20.0", store(driver).get("5:run-1device-1"));
                assertTrue(output.isEmpty());
            }
        }
    }

    @Test
    void rejectsInvalidPersistedValuesWithoutReplacingThem() throws Exception {
        for (String value : new String[]{"", "broken", "1:2:3", ":20.0", "abc:20.0",
                "9223372036854775808:20.0", "1:", "1:bad", "1:NaN", "1:Infinity", "1:-Infinity"}) {
            try (TopologyTestDriver driver = newDriver(D, C)) {
                var input = input(driver);
                var output = output(driver);
                store(driver).put("5:run-1device-1", value);
                String valid = json(event("e1", "run-1", "device-1", START, 30));
                StreamsException failure = assertThrows(StreamsException.class,
                        () -> input.pipeInput("key", valid, Instant.ofEpochMilli(START)));
                assertCause(failure, IllegalStateException.class, "Invalid RAPID_CHANGE state");
                assertEquals(value, store(driver).get("5:run-1device-1"));
                assertTrue(output.isEmpty());
            }
        }
    }

    @Test
    void equalEventTimeDoesNotTriggerButUpdatesPreviousMeasurement() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START, START, START + 10},
                    new double[]{20, 100, 104}, new boolean[]{false, false, false});
        }
    }

    @Test
    void earlierEventTimeDoesNotTriggerButUpdatesPreviousMeasurement() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, C)) {
            assertSequence(driver, new long[]{START + 20, START, START + 10},
                    new double[]{20, 100, 104}, new boolean[]{false, false, false});
        }
    }

    @Test
    void overflowingPositiveTimeGapCannotTriggerAndStillUpdatesState() throws Exception {
        try (TopologyTestDriver driver = newDriver(Long.MAX_VALUE, C)) {
            var input = input(driver);
            var output = output(driver);
            // Payload times may be extreme; Kafka timestamps remain valid and deterministic.
            store(driver).put("5:run-1device-1", Long.MIN_VALUE + ":20.0");
            checkDelivery(driver, input, output,
                    event("e1", "run-1", "device-1", Long.MAX_VALUE, 100), false);
        }
    }

    @Test
    void finiteExtremeTemperaturesCanExceedThresholdWithoutNaN() throws Exception {
        try (TopologyTestDriver driver = newDriver(D, Double.MAX_VALUE)) {
            assertSequence(driver, new long[]{START, START + 10},
                    new double[]{-Double.MAX_VALUE, Double.MAX_VALUE}, new boolean[]{false, true});
        }
    }

    private TopologyTestDriver newDriver(long timeLimit, double threshold) throws IOException {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(StateStoreDefinitions.rapidChangeStoreBuilder());
        builder.<String, String>stream("sensor-events", Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new RapidChangeProcessor(timeLimit, threshold),
                        StateStoreDefinitions.RAPID_CHANGE_STORE)
                .to("sensor-results", Produced.with(Serdes.String(), Serdes.String()));
        Properties properties = StreamsConfig.createProperties("unused:9092", "rapid-change-test");
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG,
                Files.createTempDirectory(tempDir, "state-").toString());
        return new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(START));
    }

    private static void assertSequence(TopologyTestDriver driver, long[] times, double[] temperatures,
                                       boolean[] triggered) throws JsonProcessingException {
        var input = input(driver);
        var output = output(driver);
        for (int i = 0; i < times.length; i++) {
            checkDelivery(driver, input, output,
                    event("event-" + i, "run-1", "device-1", times[i], temperatures[i]), triggered[i]);
        }
    }

    private static void checkDelivery(TopologyTestDriver driver, TestInputTopic<String, String> input,
                                      TestOutputTopic<String, String> output, SensorEvent event,
                                      boolean triggered) throws JsonProcessingException {
        String key = "original-key-" + event.deviceId();
        input.pipeInput(key, json(event), Instant.ofEpochMilli(START + 50_000));
        assertEquals(1L, output.getQueueSize());
        var result = output.readKeyValue();
        var expected = OBJECT_MAPPER.createObjectNode()
                .put("eventId", event.eventId()).put("runId", event.runId())
                .put("deviceId", event.deviceId()).put("eventTime", event.eventTime())
                .put("sentAt", event.sentAt()).put("ruleType", "RAPID_CHANGE").put("triggered", triggered);
        assertEquals(key, result.key);
        assertEquals(expected, OBJECT_MAPPER.readTree(result.value));
        assertTrue(output.isEmpty());
        String stateKey = event.runId().length() + ":" + event.runId() + event.deviceId();
        assertEquals(event.eventTime() + ":" + Double.toString(event.temperature()), store(driver).get(stateKey));
    }

    private static void assertCause(Throwable failure, Class<? extends Throwable> type, String message) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause) && cause.getMessage() != null && cause.getMessage().contains(message)) {
                return;
            }
        }
        throw new AssertionError("Expected cause " + type.getSimpleName() + ": " + message, failure);
    }

    private static TestInputTopic<String, String> input(TopologyTestDriver driver) {
        return driver.createInputTopic("sensor-events", new StringSerializer(), new StringSerializer());
    }

    private static TestOutputTopic<String, String> output(TopologyTestDriver driver) {
        return driver.createOutputTopic("sensor-results", new StringDeserializer(), new StringDeserializer());
    }

    private static KeyValueStore<String, String> store(TopologyTestDriver driver) {
        return driver.getKeyValueStore(StateStoreDefinitions.RAPID_CHANGE_STORE);
    }

    private static SensorEvent event(String eventId, String runId, String deviceId, long time, double temperature) {
        return new SensorEvent(eventId, runId, deviceId, temperature, time, START + 100_000);
    }

    private static String json(SensorEvent event) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(event);
    }

    private static SensorResult readResult(TestOutputTopic<String, String> output) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(output.readValue(), SensorResult.class);
    }
}
