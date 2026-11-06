package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.streams.config.StreamsConfig;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.model.SensorResult;
import com.kafkaiot.streams.rules.HighFrequencyProcessor;
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

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HighFrequencyProcessorTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long WINDOW_MS = 1_000L;
    // Epoch-aligned to WINDOW_MS.
    private static final long START = 1_700_000_000_000L;

    @TempDir
    Path tempDir;

    @Test
    void countsFromOneAndTriggersRepeatedlyOnlyAboveThreshold() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(2)) {
            var input = input(driver);
            var output = output(driver);
            for (int i = 1; i <= 4; i++) {
                send(input, event("event-" + i, "run-1", "device-1", START + i));
            }

            assertEquals(4L, output.getQueueSize());
            for (boolean expected : new boolean[]{false, false, true, true}) {
                assertEquals(expected, readResult(output).triggered());
            }
            assertTrue(output.isEmpty());
            assertEquals(START / WINDOW_MS + ":4", store(driver).get("5:run-1device-1"));
        }
    }

    @Test
    void resetsCountWhenAdvancingAcrossWindows() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            send(input, event("event-1", "run-1", "device-1", START + 10));
            send(input, event("event-2", "run-1", "device-1", START + 20));
            send(input, event("event-3", "run-1", "device-1", START + 3 * WINDOW_MS + 10));

            assertFalse(readResult(output).triggered());
            assertTrue(readResult(output).triggered());
            assertFalse(readResult(output).triggered());
            assertEquals((START / WINDOW_MS + 3) + ":1", store(driver).get("5:run-1device-1"));
        }
    }

    @Test
    void assignsBoundaryEventToNextHalfOpenWindow() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            send(input, event("event-1", "run-1", "device-1", START));
            send(input, event("event-2", "run-1", "device-1", START + WINDOW_MS - 1));
            send(input, event("event-3", "run-1", "device-1", START + WINDOW_MS));

            assertFalse(readResult(output).triggered());
            assertTrue(readResult(output).triggered());
            assertFalse(readResult(output).triggered());
            assertEquals((START / WINDOW_MS + 1) + ":1", store(driver).get("5:run-1device-1"));
        }
    }

    @Test
    void isolatesCountsBetweenDevices() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            send(input, event("event-1", "run-1", "device-1", START + 10));
            send(input, event("event-2", "run-1", "device-2", START + 20));
            send(input, event("event-3", "run-1", "device-1", START + 30));
            send(input, event("event-4", "run-1", "device-2", START + 40));

            for (boolean expected : new boolean[]{false, false, true, true}) {
                assertEquals(expected, readResult(output).triggered());
            }
            assertEquals(2L, store(driver).approximateNumEntries());
        }
    }

    @Test
    void isolatesCountsBetweenRunsSharingDevice() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            send(input, event("event-1", "run-1", "device-1", START + 10));
            send(input, event("event-2", "run-2", "device-1", START + 20));
            send(input, event("event-3", "run-1", "device-1", START + 30));

            assertFalse(readResult(output).triggered());
            assertFalse(readResult(output).triggered());
            assertTrue(readResult(output).triggered());
            assertEquals(START / WINDOW_MS + ":2", store(driver).get("5:run-1device-1"));
            assertEquals(START / WINDOW_MS + ":1", store(driver).get("5:run-2device-1"));
        }
    }

    @Test
    void keepsCompositeKeysUnambiguousWithDelimiterCharacters() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            // Both pairs would become "a:b:c" with a delimiter-only encoding.
            send(input, event("event-1", "a:b", "c", START + 10));
            send(input, event("event-2", "a", "b:c", START + 20));
            send(input, event("event-3", "a:b", "c", START + 30));

            assertFalse(readResult(output).triggered());
            assertFalse(readResult(output).triggered());
            assertTrue(readResult(output).triggered());
            assertEquals(START / WINDOW_MS + ":2", store(driver).get("3:a:bc"));
            assertEquals(START / WINDOW_MS + ":1", store(driver).get("1:ab:c"));
        }
    }

    @Test
    void usesPayloadEventTimeInsteadOfKafkaTimestamp() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            input.pipeInput("key", json(event("event-1", "run-1", "device-1", START + 10)),
                    Instant.ofEpochMilli(START + 10 * WINDOW_MS));
            input.pipeInput("key", json(event("event-2", "run-1", "device-1", START + 20)),
                    Instant.ofEpochMilli(START + 20 * WINDOW_MS));
            input.pipeInput("key", json(event("event-3", "run-1", "device-1", START + WINDOW_MS)),
                    Instant.ofEpochMilli(START + 20 * WINDOW_MS + 1));

            assertFalse(readResult(output).triggered());
            assertTrue(readResult(output).triggered());
            assertFalse(readResult(output).triggered());
            assertEquals((START / WINDOW_MS + 1) + ":1", store(driver).get("5:run-1device-1"));
        }
    }

    @Test
    void preservesExactResultContractAndKafkaKeysWithoutPublishingAlerts()
            throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            var events = List.of(event("event-\"1", "run-1", "device-1", START + 10),
                    event("event-2", "run-1", "device-1", START + 20));
            for (int i = 0; i < events.size(); i++) {
                input.pipeInput("original-key-" + i, json(events.get(i)), Instant.ofEpochMilli(START));
            }

            assertEquals(2L, output.getQueueSize());
            for (int i = 0; i < events.size(); i++) {
                var event = events.get(i);
                var actual = output.readKeyValue();
                var expected = OBJECT_MAPPER.createObjectNode()
                        .put("eventId", event.eventId()).put("runId", event.runId())
                        .put("deviceId", event.deviceId()).put("eventTime", event.eventTime())
                        .put("sentAt", event.sentAt()).put("ruleType", "HIGH_FREQUENCY")
                        .put("triggered", i == 1);
                assertEquals("original-key-" + i, actual.key);
                assertEquals(expected, OBJECT_MAPPER.readTree(actual.value));
            }
            assertTrue(output.isEmpty());
            assertFalse(driver.producedTopicNames().contains("sensor-alerts"));
        }
    }

    @Test
    void persistsWindowIdentifierAndUpdatedCount() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(2)) {
            var input = input(driver);
            KeyValueStore<String, String> state = store(driver);
            assertTrue(state.persistent());
            send(input, event("event-1", "run-1", "device-1", START + 10));
            assertEquals(START / WINDOW_MS + ":1", state.get("5:run-1device-1"));
            send(input, event("event-2", "run-1", "device-1", START + 20));
            assertEquals(START / WINDOW_MS + ":2", state.get("5:run-1device-1"));
        }
    }

    @Test
    void rejectsNonPositiveWindowDurations() {
        for (long duration : new long[]{0, -1, Long.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> new HighFrequencyProcessor(duration, 1));
        }
    }

    @Test
    void rejectsNonPositiveCountThresholds() {
        for (int threshold : new int[]{0, -1, Integer.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new HighFrequencyProcessor(WINDOW_MS, threshold));
        }
    }

    @Test
    void rejectsEarlierWindowWithoutChangingStateOrProducingResult() throws JsonProcessingException {
        try (TopologyTestDriver driver = newDriver(1)) {
            var input = input(driver);
            var output = output(driver);
            send(input, event("event-1", "run-1", "device-1", START + WINDOW_MS));
            readResult(output);
            String earlierEvent = json(event("event-2", "run-1", "device-1", START + WINDOW_MS - 1));

            StreamsException failure = assertThrows(StreamsException.class,
                    () -> input.pipeInput("device-1", earlierEvent, Instant.ofEpochMilli(START + 10_000)));

            Throwable cause = failure;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            assertInstanceOf(IllegalStateException.class, cause);
            assertTrue(cause.getMessage().contains("Event precedes the current HIGH_FREQUENCY window"));
            assertEquals((START / WINDOW_MS + 1) + ":1", store(driver).get("5:run-1device-1"));
            assertTrue(output.isEmpty());
        }
    }

    private TopologyTestDriver newDriver(int threshold) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(StateStoreDefinitions.highFrequencyStoreBuilder());
        builder.<String, String>stream("sensor-events", Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new HighFrequencyProcessor(WINDOW_MS, threshold),
                        StateStoreDefinitions.HIGH_FREQUENCY_STORE)
                .to("sensor-results", Produced.with(Serdes.String(), Serdes.String()));
        Properties properties = StreamsConfig.createProperties("unused:9092", "high-frequency-test");
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG, tempDir.toString());
        return new TopologyTestDriver(builder.build(), properties, Instant.ofEpochMilli(START));
    }

    private static TestInputTopic<String, String> input(TopologyTestDriver driver) {
        return driver.createInputTopic("sensor-events", new StringSerializer(), new StringSerializer());
    }

    private static TestOutputTopic<String, String> output(TopologyTestDriver driver) {
        return driver.createOutputTopic("sensor-results", new StringDeserializer(), new StringDeserializer());
    }

    private static KeyValueStore<String, String> store(TopologyTestDriver driver) {
        return driver.getKeyValueStore(StateStoreDefinitions.HIGH_FREQUENCY_STORE);
    }

    private static SensorEvent event(String eventId, String runId, String deviceId, long eventTime) {
        return new SensorEvent(eventId, runId, deviceId, 25.0, eventTime, START + 50_000);
    }

    private static String json(SensorEvent event) throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(event);
    }

    private static void send(TestInputTopic<String, String> input, SensorEvent event)
            throws JsonProcessingException {
        input.pipeInput(event.deviceId(), json(event), Instant.ofEpochMilli(event.eventTime()));
    }

    private static SensorResult readResult(TestOutputTopic<String, String> output)
            throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(output.readValue(), SensorResult.class);
    }
}
