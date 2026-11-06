package com.kafkaiot.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams.State;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class StreamsLifecycleEvidenceTest {
    @Test
    void startupDoesNotDeclareReadiness() {
        var events = new ArrayList<StreamsLifecycleEvidence.Evidence>();
        var lifecycle = new StreamsLifecycleEvidence("app-1", events::add);
        lifecycle.startup(State.CREATED);
        assertEquals(1, events.size());
        assertEquals("KAFKA_STREAMS_STARTUP", events.getFirst().evidenceType());
        assertEquals("NOT_READY", events.getFirst().status());
        assertEquals("app-1", events.getFirst().applicationId());
        assertTrue(events.getFirst().observedAtEpochMs() > 0);
    }

    @Test
    void runningReadinessIsInvalidatedByRebalancingAndError() {
        var events = new ArrayList<StreamsLifecycleEvidence.Evidence>();
        var lifecycle = new StreamsLifecycleEvidence("app-1", events::add);
        lifecycle.onChange(State.RUNNING, State.REBALANCING);
        lifecycle.onChange(State.REBALANCING, State.RUNNING);
        lifecycle.onChange(State.RUNNING, State.REBALANCING);
        lifecycle.onChange(State.PENDING_ERROR, State.RUNNING);
        lifecycle.onChange(State.ERROR, State.PENDING_ERROR);
        assertEquals(List.of("READY", "NOT_READY", "READY", "FAILED", "FAILED"),
                events.stream().map(StreamsLifecycleEvidence.Evidence::status).toList());
        assertEquals(State.REBALANCING, events.getFirst().previousState());
        assertEquals(State.RUNNING, events.getFirst().currentState());
    }

    @Test
    void failureEvidenceRetainsActualExceptionInformation() {
        var events = new ArrayList<StreamsLifecycleEvidence.Evidence>();
        var lifecycle = new StreamsLifecycleEvidence("app-1", events::add);
        var failure = new IllegalStateException("startup failed", new IOException("cause"));
        failure.addSuppressed(new IllegalStateException("cleanup failed"));
        lifecycle.failure(State.CREATED, failure);
        assertEquals("FAILED", events.getFirst().status());
        assertEquals("startup failed", events.getFirst().failure().message());
        assertTrue(events.getFirst().failure().cause().contains("cause"));
        assertTrue(events.getFirst().failure().suppressed().getFirst().contains("cleanup failed"));
    }

    @Test
    void requestedShutdownAndClosedOutcomeComeFromActualCloseCall() {
        var events = new ArrayList<StreamsLifecycleEvidence.Evidence>();
        var lifecycle = new StreamsLifecycleEvidence("app-1", events::add);
        var state = new AtomicReference<>(State.RUNNING);
        assertTrue(lifecycle.close(() -> {
            assertEquals("SHUTDOWN_REQUESTED", events.getFirst().status());
            assertEquals(1, events.size());
            state.set(State.NOT_RUNNING);
            return true;
        }, state::get, true));
        assertEquals("KAFKA_STREAMS_CLOSE_RESULT", events.get(1).evidenceType());
        assertEquals("CLOSED", events.get(1).status());
        assertEquals(State.NOT_RUNNING, events.get(1).currentState());
        lifecycle.onChange(State.RUNNING, State.REBALANCING);
        assertEquals("NOT_READY", events.getLast().status());
    }

    @Test
    void closeTimeoutNeverClaimsSuccessfulClosure() {
        var events = new ArrayList<StreamsLifecycleEvidence.Evidence>();
        var lifecycle = new StreamsLifecycleEvidence("app-1", events::add);
        assertFalse(lifecycle.close(() -> false, () -> State.PENDING_SHUTDOWN, true));
        assertEquals("TIMEOUT", events.getLast().status());
        assertTrue(events.stream().noneMatch(event -> "CLOSED".equals(event.status())));
    }

    @Test
    void closeFailureIsReportedAndOriginalExceptionPropagates() {
        var events = new ArrayList<StreamsLifecycleEvidence.Evidence>();
        var lifecycle = new StreamsLifecycleEvidence("app-1", events::add);
        var failure = new IllegalStateException("close failed");
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> lifecycle.close(() -> { throw failure; }, () -> State.ERROR, false)));
        assertEquals("FAILED", events.getLast().status());
        assertEquals("close failed", events.getLast().failure().message());
        assertTrue(events.stream().noneMatch(event -> "SHUTDOWN_REQUESTED".equals(event.status())));
    }

    @Test
    void evidenceFailureCannotPreventCloseAndSecondaryFailureIsPreserved() {
        var outputFailure = new IllegalStateException("output failed");
        var closeFailure = new IllegalStateException("close failed");
        var closed = new AtomicBoolean();
        var lifecycle = new StreamsLifecycleEvidence("app-1", event -> { throw outputFailure; });
        var thrown = assertThrows(IllegalStateException.class, () -> lifecycle.close(() -> {
            closed.set(true);
            throw closeFailure;
        }, () -> State.RUNNING, true));
        assertSame(outputFailure, thrown);
        assertTrue(closed.get());
        assertEquals(List.of(closeFailure), List.of(thrown.getSuppressed()));
    }

    @Test
    void outputFailureBlocksSubsequentReadinessClaims() {
        var failure = new IllegalStateException("output failed");
        var lifecycle = new StreamsLifecycleEvidence("app-1", event -> { throw failure; });
        assertSame(failure, assertThrows(IllegalStateException.class, () -> lifecycle.startup(State.CREATED)));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> lifecycle.onChange(State.RUNNING, State.REBALANCING)));
    }

    @Test
    void stdoutContainsOneValidJsonLinePerEvent() throws Exception {
        var original = System.out;
        var bytes = new ByteArrayOutputStream();
        try (var captured = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(captured);
                new StreamsLifecycleEvidence("app-1", StreamsLifecycleEvidence::print)
                        .onChange(State.RUNNING, State.REBALANCING);
            } finally {
                System.setOut(original);
            }
        }
        assertEquals(1, bytes.toString(StandardCharsets.UTF_8).lines().count());
        var json = new ObjectMapper().readTree(bytes.toByteArray());
        assertEquals("KAFKA_STREAMS_STATE_CHANGED", json.path("evidenceType").textValue());
        assertEquals("RUNNING", json.path("currentState").textValue());
        assertEquals("REBALANCING", json.path("previousState").textValue());
        assertEquals("READY", json.path("status").textValue());
    }

    @Test
    void brokenStdoutRaisesExplicitFailure() {
        var original = System.out;
        try (var broken = new PrintStream(new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw new IOException("broken stdout");
            }
        })) {
            try {
                System.setOut(broken);
                var failure = assertThrows(IllegalStateException.class,
                        () -> new StreamsLifecycleEvidence("app-1", StreamsLifecycleEvidence::print).startup(State.CREATED));
                assertTrue(failure.getMessage().contains("stdout"));
            } finally {
                System.setOut(original);
            }
        }
    }

    @Test
    void cliOptionIsOptionalAndValidatedForAllExistingRulesWithoutStartingKafka() throws Exception {
        var parser = SensorStreamsApplication.class.getDeclaredMethod("parse", String[].class);
        parser.setAccessible(true);
        for (String rule : List.of("HIGH_TEMPERATURE", "HIGH_FREQUENCY", "RAPID_CHANGE")) {
            var arguments = new ArrayList<>(List.of("--bootstrap-servers", "unused:9092", "--application-id", "app-1",
                    "--rule", rule));
            switch (rule) {
                case "HIGH_TEMPERATURE" -> arguments.addAll(List.of("--temperature-threshold", "30"));
                case "HIGH_FREQUENCY" -> arguments.addAll(List.of("--window-duration-ms", "1000", "--count-threshold", "2"));
                case "RAPID_CHANGE" -> arguments.addAll(List.of("--max-time-difference-ms", "1000", "--temperature-change-threshold", "2"));
            }
            var disabled = (Map<?, ?>) parser.invoke(null, (Object) arguments.toArray(String[]::new));
            assertFalse(disabled.containsKey("lifecycle-stdout"));
            arguments.addAll(List.of("--lifecycle-stdout", "true"));
            var enabled = (Map<?, ?>) parser.invoke(null, (Object) arguments.toArray(String[]::new));
            assertEquals("true", enabled.get("lifecycle-stdout"));
            arguments.set(arguments.size() - 1, "false");
            assertEquals("false", ((Map<?, ?>) parser.invoke(null,
                    (Object) arguments.toArray(String[]::new))).get("lifecycle-stdout"));
            arguments.set(arguments.size() - 1, "invalid");
            var failure = assertThrows(InvocationTargetException.class,
                    () -> parser.invoke(null, (Object) arguments.toArray(String[]::new)));
            assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        }
    }
    @Test
    void configurationEvidenceUsesAppliedPropertiesAndKafkaDefaults() throws Exception {
        var properties = com.kafkaiot.streams.config.StreamsConfig.createProperties("broker:9092", "app-config");
        var evidence = SensorStreamsApplication.configurationEvidence(properties, "HIGH_TEMPERATURE",
                Map.of("temperatureThreshold", 30.0));
        assertEquals("KAFKA_STREAMS_CONFIGURATION", evidence.evidenceType());
        assertEquals("app-config", evidence.applicationId());
        assertTrue(evidence.observedAtEpochMs() > 0);
        var applied = evidence.appliedConfiguration();
        assertEquals(List.of("broker:9092"), applied.get("bootstrap.servers"));
        assertEquals("at_least_once", applied.get("processing.guarantee"));
        assertEquals(1, applied.get("num.stream.threads"));
        assertEquals(30000L, applied.get("commit.interval.ms"));
        assertEquals(0, applied.get("num.standby.replicas"));
        assertEquals(-1, applied.get("replication.factor"));
        assertEquals("org.apache.kafka.common.serialization.Serdes$StringSerde", applied.get("default.key.serde"));
        assertTrue(evidence.unavailable().contains("runtimeGeneratedClientIds"));
        var json = new ObjectMapper().readTree(new ObjectMapper().writeValueAsString(evidence));
        assertEquals(30.0, json.path("ruleParameters").path("temperatureThreshold").doubleValue());
        assertFalse(json.has("status")); // Configuration is not readiness.
    }

    @Test
    void configurationEvidencePreservesTypedParametersForEveryRule() {
        var properties = com.kafkaiot.streams.config.StreamsConfig.createProperties("broker:9092", "app");
        for (var entry : Map.<String, Map<String, Object>>of(
                "HIGH_TEMPERATURE", Map.of("temperatureThreshold", -1.5),
                "HIGH_FREQUENCY", Map.of("windowDurationMs", 1000L, "countThreshold", 10),
                "RAPID_CHANGE", Map.of("maxTimeDifferenceMs", 2000L, "temperatureChangeThreshold", 2.5)).entrySet()) {
            var evidence = SensorStreamsApplication.configurationEvidence(properties, entry.getKey(), entry.getValue());
            assertEquals(entry.getKey(), evidence.rule());
            assertEquals(entry.getValue(), evidence.ruleParameters());
        }
    }

    @Test
    void configurationEvidenceDoesNotMutatePropertiesOrLeakSecrets() throws Exception {
        var properties = com.kafkaiot.streams.config.StreamsConfig.createProperties("broker:9092", "app");
        properties.setProperty("ssl.keystore.password", "private-secret");
        properties.setProperty("custom.token", "private-token");
        var original = properties.clone();
        String json = new ObjectMapper().writeValueAsString(SensorStreamsApplication.configurationEvidence(
                properties, "HIGH_FREQUENCY", Map.of("windowDurationMs", 1000L, "countThreshold", 1)));
        assertFalse(json.contains("private-secret"));
        assertFalse(json.contains("private-token"));
        assertFalse(json.contains("ssl.keystore.password"));
        assertEquals(original, properties);
    }

    @Test
    void disabledConfigurationEvidenceDoesNotParseOrPrint() {
        var original = System.out;
        var bytes = new ByteArrayOutputStream();
        try (var captured = new PrintStream(bytes)) {
            try {
                System.setOut(captured);
                SensorStreamsApplication.emitConfiguration(null, null, null, false);
            } finally {
                System.setOut(original);
            }
        }
        assertEquals("", bytes.toString(StandardCharsets.UTF_8));
    }

    @Test
    void configurationPrecedesLifecycleStartupAndRunningReadiness() throws Exception {
        var original = System.out;
        var bytes = new ByteArrayOutputStream();
        try (var captured = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(captured);
                SensorStreamsApplication.emitConfiguration(
                        com.kafkaiot.streams.config.StreamsConfig.createProperties("broker:9092", "app"),
                        "RAPID_CHANGE", Map.of("maxTimeDifferenceMs", 1000L, "temperatureChangeThreshold", 1.0), true);
                var lifecycle = new StreamsLifecycleEvidence("app", StreamsLifecycleEvidence::print);
                lifecycle.startup(State.CREATED);
                lifecycle.onChange(State.RUNNING, State.REBALANCING);
            } finally {
                System.setOut(original);
            }
        }
        var lines = bytes.toString(StandardCharsets.UTF_8).lines().toList();
        assertEquals(3, lines.size());
        var json = new ObjectMapper();
        assertEquals("KAFKA_STREAMS_CONFIGURATION", json.readTree(lines.get(0)).path("evidenceType").textValue());
        assertEquals("NOT_READY", json.readTree(lines.get(1)).path("status").textValue());
        assertEquals("READY", json.readTree(lines.get(2)).path("status").textValue());
    }

    @Test
    void configurationOutputFailureIsExplicit() {
        var original = System.out;
        try (var broken = new PrintStream(new OutputStream() {
            @Override public void write(int value) throws IOException { throw new IOException("output failed"); }
        })) {
            try {
                System.setOut(broken);
                assertThrows(IllegalStateException.class, () -> SensorStreamsApplication.emitConfiguration(
                        com.kafkaiot.streams.config.StreamsConfig.createProperties("broker:9092", "app"),
                        "HIGH_TEMPERATURE", Map.of("temperatureThreshold", 30.0), true));
            } finally {
                System.setOut(original);
            }
        }
    }

    @Test
    void configurationJsonSerializationFailureIsExplicit() {
        var failure = assertThrows(IllegalStateException.class, () -> StreamsLifecycleEvidence.print(new Object() {
            public String getValue() { throw new IllegalStateException("cannot serialize"); }
        }));
        assertTrue(failure.getMessage().contains("serialized"));
        assertNotNull(failure.getCause());
    }

}
