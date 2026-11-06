package com.kafkaiot.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.io.RunMetadataWriter;
import com.kafkaiot.producer.service.SensorEventPublisher;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorProducerApplicationTest {
    @TempDir
    Path tempDir;

    @Test
    void acceptsOptionalEventCsvOutput() throws Exception {
        Map<String, String> options = options();
        options.put("event-csv-output", tempDir.resolve("run-1/producer-events.csv").toString());
        assertEquals(options, parse(arguments(options)));
    }

    @Test
    void preservesExistingOptionsWithoutEventExport() throws Exception {
        Map<String, String> options = options();
        assertEquals(options, parse(arguments(options)));
        assertFalse(options.containsKey("event-csv-output"));
    }

    @Test
    void rejectsMissingAndBlankEventOutputValues() {
        assertThrows(IllegalArgumentException.class,
                () -> SensorProducerApplication.main(new String[]{"--event-csv-output"}));
        assertThrows(IllegalArgumentException.class,
                () -> SensorProducerApplication.main(new String[]{"--event-csv-output", " "}));
        assertThrows(IllegalArgumentException.class,
                () -> SensorProducerApplication.main(new String[]{"--event-csv-output", "--csv-output", "x"}));
    }

    @Test
    void absentOptionCreatesNoWriterOrAdditionalFiles() throws Exception {
        assertNull(SensorProducerApplication.openEventWriter(options(), tempDir.resolve("totals.csv")));
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void rejectsIdenticalAndNormalizedOutputDestinationsBeforePublisherCreation() throws Exception {
        Map<String, String> options = options();
        Path totals = tempDir.resolve("totals.csv");
        AtomicBoolean created = new AtomicBoolean();
        for (Path eventPath : List.of(totals, tempDir.resolve("sub/../totals.csv"))) {
            options.put("event-csv-output", eventPath.toString());
            assertThrows(IllegalArgumentException.class, () -> SensorProducerApplication.execute(options,
                    (bootstrap, acks, idempotence) -> {
                        created.set(true);
                        throw new AssertionError("Publisher must not be created");
                    }));
        }
        assertFalse(created.get());
        assertFalse(Files.exists(totals));
    }

    @Test
    void rejectsOutputAliasesThroughSymbolicLinks() throws Exception {
        Path real = Files.createDirectory(tempDir.resolve("real"));
        Path alias = Files.createSymbolicLink(tempDir.resolve("alias"), real);
        Map<String, String> options = options();
        options.put("event-csv-output", alias.resolve("events.csv").toString());
        assertThrows(IllegalArgumentException.class,
                () -> SensorProducerApplication.openEventWriter(options, real.resolve("events.csv")));
    }

    @Test
    void closesPublisherBeforeEventWriterAndKeepsTotalsIndependent() throws Exception {
        Path events = tempDir.resolve("run-1/producer-events.csv");
        Map<String, String> options = options();
        options.put("event-csv-output", events.toString());
        var kafka = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer()) {
            @Override
            public void close() {
                while (completeNext()) {
                    // Deliver pending callbacks while the application's writer is still open.
                }
                super.close();
            }
        };
        SensorEventPublisher publisher = publisher(kafka);
        SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher);

        assertTrue(kafka.closed());
        List<String> rows = Files.readAllLines(events);
        assertEquals(1 + 2 * kafka.history().size(), rows.size());
        assertTrue(rows.get(1).contains(",ATTEMPTED,"));
        assertTrue(rows.stream().anyMatch(row -> row.contains(",CONFIRMED,")));
        List<String> totals = Files.readAllLines(Path.of(options.get("csv-output")));
        assertEquals("runId,phase,attempted,confirmed,failed,latestError", totals.getFirst());
        assertEquals(3, totals.size());
        assertEquals("run-1,MEASUREMENT," + kafka.history().size() + "," + kafka.history().size() + ",0,",
                totals.get(2));
        assertThrows(FileAlreadyExistsException.class, () -> new ProducerEventCsvWriter(events));
    }

    @Test
    void absentOptionPreservesOriginalTotalsOverwriteBehavior() throws Exception {
        Map<String, String> options = options();
        Path totals = Path.of(options.get("csv-output"));
        Files.writeString(totals, "previous phase totals");
        var kafka = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer());
        var publisher = publisher(kafka);
        SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher);
        assertTrue(kafka.closed());
        assertEquals("runId,phase,attempted,confirmed,failed,latestError", Files.readAllLines(totals).getFirst());
        try (var files = Files.list(tempDir)) {
            assertEquals(List.of(totals), files.toList());
        }
    }

    @Test
    void startupFailureClosesWriterAndPreservesPartialHeader() throws Exception {
        Map<String, String> options = options();
        Path events = tempDir.resolve("partial.csv");
        options.put("event-csv-output", events.toString());
        RuntimeException startupFailure = new IllegalStateException("mock startup failed");
        assertSame(startupFailure, assertThrows(IllegalStateException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> {
                    throw startupFailure;
                })));
        assertEquals(1, Files.readAllLines(events).size());
        assertFalse(Files.exists(Path.of(options.get("csv-output"))));
    }

    @Test
    void publicationFailurePreservesOriginalAndSuppressedCleanupErrorsAndPartialRows() throws Exception {
        Map<String, String> options = options();
        Path events = tempDir.resolve("failed.csv");
        options.put("event-csv-output", events.toString());
        var kafka = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer());
        RuntimeException sendFailure = new IllegalStateException("mock Kafka send failed");
        RuntimeException closeFailure = new IllegalStateException("mock Kafka close failed");
        kafka.sendException = sendFailure;
        kafka.closeException = closeFailure;
        var publisher = publisher(kafka);
        assertSame(sendFailure, assertThrows(IllegalStateException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher)));
        assertEquals(List.of(closeFailure), List.of(sendFailure.getSuppressed()));
        List<String> rows = Files.readAllLines(events);
        assertEquals(3, rows.size());
        assertTrue(rows.get(1).contains(",ATTEMPTED,"));
        assertTrue(rows.get(2).contains(",FAILED,"));
        assertFalse(Files.exists(Path.of(options.get("csv-output"))));
    }

    @Test
    void reportsExistingEventFileAsIoFailureWithoutOverwritingIt() throws Exception {
        Map<String, String> options = options();
        Path events = tempDir.resolve("existing.csv");
        Files.writeString(events, "preserved observations");
        options.put("event-csv-output", events.toString());
        PrintStream original = System.err;
        ByteArrayOutputStream errors = new ByteArrayOutputStream();
        try (var captured = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            try {
                System.setErr(captured);
                assertThrows(FileAlreadyExistsException.class,
                        () -> SensorProducerApplication.main(arguments(options)));
            } finally {
                System.setErr(original);
            }
        }
        assertTrue(errors.toString(StandardCharsets.UTF_8).contains("Producer execution failed"));
        assertTrue(errors.toString(StandardCharsets.UTF_8).contains("Execution incomplete"));
        assertEquals("preserved observations", Files.readString(events));
        assertFalse(Files.exists(Path.of(options.get("csv-output"))));
    }

    @Test
    void rejectsUnusableOutputPaths() throws Exception {
        Map<String, String> options = options();
        options.put("event-csv-output", tempDir.toString());
        assertThrows(FileAlreadyExistsException.class,
                () -> SensorProducerApplication.openEventWriter(options, tempDir.resolve("totals.csv")));
        options.put("event-csv-output", "\u0000");
        assertThrows(IllegalArgumentException.class,
                () -> SensorProducerApplication.openEventWriter(options, tempDir.resolve("totals.csv")));
    }

    @Test
    void helpDocumentsOptionalExportWithoutCreatingFiles() throws Exception {
        PrintStream original = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (var captured = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(captured);
                SensorProducerApplication.main(new String[]{"--help"});
            } finally {
                System.setOut(original);
            }
        }
        String help = output.toString(StandardCharsets.UTF_8);
        assertTrue(help.contains("--event-csv-output PATH"));
        assertTrue(help.contains("--run-config-output PATH"));
        assertTrue(help.contains("experiments/results/<runId>/producer-events.csv"));
        assertTrue(help.contains("--csv-output PATH"));
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void parsesRunConfigOptionAndRejectsMissingBlankAndDuplicateValues() throws Exception {
        Map<String, String> options = metadataOptions();
        assertEquals(options, parse(arguments(options)));
        assertThrows(IllegalArgumentException.class, () -> parse(new String[]{"--run-config-output"}));
        assertThrows(IllegalArgumentException.class, () -> parse(new String[]{"--run-config-output", " "}));
        assertThrows(IllegalArgumentException.class, () -> parse(new String[]{
                "--run-config-output", "a", "--run-config-output", "b"}));
    }

    @Test
    void metadataExistsBeforePublisherCreationAndRecordsEffectiveConfiguration() throws Exception {
        Map<String, String> options = metadataOptions();
        options.put("acks", "1");
        options.put("enable-idempotence", "false");
        RuntimeException stop = new IllegalStateException("stop at startup");
        assertSame(stop, assertThrows(IllegalStateException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> {
                    try {
                        JsonNode metadata = metadata(options);
                        assertEquals("NOT_FINALIZED", metadata.path("experimentStatus").textValue());
                        assertEquals("BEFORE_WORKLOAD", metadata.path("components").path("producerLifecycle").textValue());
                        assertTrue(metadata.path("clockAnchors").isNull());
                        JsonNode producer = metadata.path("configuration").path("producer");
                        assertEquals("run-1", producer.path("runId").textValue());
                        assertEquals(123, producer.path("randomSeed").longValue());
                        assertEquals(1700000000000L, producer.path("initialEventTimestamp").longValue());
                        assertEquals(1, producer.path("timestampIncrementMs").longValue());
                        assertEquals(1.0, producer.path("targetEventsPerSecond").doubleValue());
                        assertEquals("unused:9092", producer.path("kafkaSettings").path("bootstrap.servers").textValue());
                        assertEquals("1", producer.path("kafkaSettings").path("acks").textValue());
                        assertEquals("false", producer.path("kafkaSettings").path("enable.idempotence").textValue());
                        assertEquals(0, metadata.path("phaseDurationsMs").path("warmUp").longValue());
                        assertEquals(1000, metadata.path("phaseDurationsMs").path("measurement").longValue());
                        assertTrue(metadata.path("phaseDurationsMs").path("maximumDrain").isNull());
                    } catch (IOException exception) {
                        throw new AssertionError(exception);
                    }
                    throw stop;
                })));
        assertEquals("FAILED", metadata(options).path("components").path("producer").textValue());
        assertTrue(metadata(options).path("observed").path("generationStoppedEpochMs").isNull());
    }

    @Test
    void normalCompletionRecordsTimingDrainAndCleanupWithoutFinalizingExperiment() throws Exception {
        Map<String, String> options = metadataOptions();
        options.put("event-csv-output", tempDir.resolve("producer-events.csv").toString());
        var kafka = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer()) {
            @Override
            public void close() {
                while (completeNext()) { }
                super.close();
            }
        };
        var publisher = publisher(kafka);
        SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher);
        JsonNode metadata = metadata(options);
        JsonNode anchor = metadata.path("clockAnchors");
        JsonNode scheduled = metadata.path("scheduled");
        JsonNode observed = metadata.path("observed");
        assertTrue(anchor.path("sampleSpanNs").longValue() >= 0);
        assertTrue(anchor.path("workloadStartNs").isIntegralNumber());
        assertEquals(anchor.path("workloadStartEpochMs"), scheduled.path("measurementStartEpochMs"));
        assertEquals(anchor.path("workloadStartEpochMs").longValue() + 1000,
                scheduled.path("measurementDeadlineEpochMs").longValue());
        assertEquals(0, scheduled.path("measurementStartOffsetNs").longValue());
        assertEquals(1000000000L, scheduled.path("measurementDeadlineOffsetNs").longValue());
        assertEquals("DERIVED_FROM_BRACKETED_CLOCK_SAMPLE", scheduled.path("epochAlignment").textValue());
        assertTrue(observed.path("generationStoppedElapsedNs").longValue() >= 1000000000L);
        for (String field : List.of("generationStoppedEpochMs", "publicationsDrainedEpochMs",
                "eventCsvClosedEpochMs", "producerCleanupCompletedEpochMs")) {
            assertTrue(observed.path(field).isIntegralNumber());
        }
        assertTrue(observed.path("drainEndedEpochMs").isNull());
        assertEquals("COMPLETED", metadata.path("components").path("producer").textValue());
        assertEquals("NOT_FINALIZED", metadata.path("experimentStatus").textValue());
        assertEquals(0, metadata.path("failures").size());
        assertTrue(kafka.closed());
        assertEquals(3, Files.readAllLines(Path.of(options.get("csv-output"))).size());
        assertEquals(1 + 2 * kafka.history().size(),
                Files.readAllLines(Path.of(options.get("event-csv-output"))).size());
        try (var owner = new RunMetadataWriter(Path.of(options.get("run-config-output")), "run-1")) {
            assertEquals("NOT_FINALIZED", metadata(options).path("experimentStatus").textValue());
        }
    }

    @Test
    void metadataPreservesPublicationFailureAndSuppressedCloseFailure() throws Exception {
        Map<String, String> options = metadataOptions();
        var kafka = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer());
        var sendFailure = new IllegalStateException("Kafka send failed");
        var closeFailure = new IllegalStateException("Kafka close failed");
        kafka.sendException = sendFailure;
        kafka.closeException = closeFailure;
        var publisher = publisher(kafka);
        assertSame(sendFailure, assertThrows(IllegalStateException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher)));
        assertEquals(List.of(closeFailure), List.of(sendFailure.getSuppressed()));
        JsonNode metadata = metadata(options);
        assertEquals("FAILED", metadata.path("components").path("producer").textValue());
        assertEquals("NOT_FINALIZED", metadata.path("experimentStatus").textValue());
        assertTrue(metadata.path("observed").path("generationStoppedEpochMs").isIntegralNumber());
        assertTrue(metadata.path("observed").path("publicationsDrainedEpochMs").isNull());
        assertTrue(metadata.path("failures").toString().contains("Kafka send failed"));
        assertTrue(metadata.path("failures").toString().contains("Kafka close failed"));
        assertFalse(Files.exists(Path.of(options.get("csv-output"))));
    }

    @Test
    void interruptionPersistsIncompleteTimingAndRestoresInterruptStatus() throws Exception {
        Map<String, String> options = metadataOptions();
        options.put("warm-up-seconds", "60");
        options.put("measurement-seconds", "300");
        var kafka = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer());
        var publisher = publisher(kafka);
        try {
            assertThrows(InterruptedException.class, () -> SensorProducerApplication.execute(options,
                    (bootstrap, acks, idempotence) -> {
                        Thread.currentThread().interrupt();
                        return publisher;
                    }));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals("INTERRUPTED", metadata(options).path("components").path("producer").textValue());
        long anchor = metadata(options).path("clockAnchors").path("workloadStartEpochMs").longValue();
        assertEquals(anchor + 60_000, metadata(options).path("scheduled").path("measurementStartEpochMs").longValue());
        assertEquals(anchor + 360_000, metadata(options).path("scheduled").path("measurementDeadlineEpochMs").longValue());
        assertTrue(metadata(options).path("observed").path("generationStoppedEpochMs").isIntegralNumber());
        assertTrue(kafka.closed());
        assertFalse(Files.exists(Path.of(options.get("csv-output"))));
    }

    @Test
    void csvCreationFailureIsRecordedWithoutStartingPublisher() throws Exception {
        Map<String, String> options = metadataOptions();
        Path events = tempDir.resolve("existing-events.csv");
        Files.writeString(events, "preserved");
        options.put("event-csv-output", events.toString());
        assertThrows(FileAlreadyExistsException.class, () -> SensorProducerApplication.execute(options,
                (bootstrap, acks, idempotence) -> { throw new AssertionError("must not start"); }));
        assertEquals("preserved", Files.readString(events));
        assertEquals("EVENT_CSV_OPEN", metadata(options).path("failures").get(0).path("category").textValue());
        assertEquals("FAILED", metadata(options).path("components").path("producer").textValue());
    }

    @Test
    void metadataWriteFailureDoesNotReplaceOriginalPublicationFailure() throws Exception {
        Map<String, String> options = metadataOptions();
        Path output = Path.of(options.get("run-config-output"));
        var failure = new IllegalStateException("primary publication failure");
        var kafka = new MockProducer<String, String>() {
            @Override
            public java.util.concurrent.Future<org.apache.kafka.clients.producer.RecordMetadata> send(
                    org.apache.kafka.clients.producer.ProducerRecord<String, String> record,
                    org.apache.kafka.clients.producer.Callback callback) {
                try {
                    Files.move(output, output.resolveSibling("preserved.json"));
                    Files.createDirectory(output);
                } catch (IOException exception) {
                    throw new AssertionError(exception);
                }
                throw failure;
            }
        };
        var publisher = publisher(kafka);
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher)));
        assertTrue(failure.getSuppressed().length > 0);
        assertTrue(List.of(failure.getSuppressed()).stream()
                .anyMatch(error -> error.getMessage().contains("Run metadata persistence failed")));
        assertEquals("NOT_FINALIZED", new ObjectMapper().readTree(
                Files.readAllBytes(output.resolveSibling("preserved.json"))).path("experimentStatus").textValue());
        assertTrue(kafka.closed());
    }

    @Test
    void rejectsMetadataAndLockAliasesWithCsvOutputs() throws Exception {
        for (boolean lockAlias : List.of(false, true)) {
            Map<String, String> options = metadataOptions();
            String output = options.get("run-config-output");
            options.put("csv-output", lockAlias ? output + ".lock" : output);
            assertThrows(IllegalArgumentException.class, () -> SensorProducerApplication.execute(options,
                    (bootstrap, acks, idempotence) -> { throw new AssertionError("must not start"); }));
            assertFalse(Files.exists(Path.of(output)));
        }
    }

    @Test
    void rejectsUnusableOrForeignMetadataBeforePublication() throws Exception {
        Map<String, String> options = metadataOptions();
        for (String invalid : List.of(" ", "\u0000", tempDir.toString())) {
            options.put("run-config-output", invalid);
            assertThrows(Exception.class, () -> SensorProducerApplication.execute(options,
                    (bootstrap, acks, idempotence) -> { throw new AssertionError("must not start"); }));
        }
        Map<String, String> foreign = metadataOptions();
        Path output = Path.of(foreign.get("run-config-output"));
        try (var writer = new RunMetadataWriter(output, "foreign-run")) { }
        byte[] original = Files.readAllBytes(output);
        assertThrows(IOException.class, () -> SensorProducerApplication.execute(foreign,
                (bootstrap, acks, idempotence) -> { throw new AssertionError("must not start"); }));
        org.junit.jupiter.api.Assertions.assertArrayEquals(original, Files.readAllBytes(output));
    }

    @Test
    void asynchronousPublicationFailuresKeepTotalsAndExperimentIncomplete() throws Exception {
        Map<String, String> options = metadataOptions();
        options.put("event-csv-output", tempDir.resolve("events.csv").toString());
        var kafka = new MockProducer<String, String>(false, null, new StringSerializer(), new StringSerializer()) {
            @Override
            public void close() {
                while (errorNext(new IllegalStateException("async broker failure"))) { }
                super.close();
            }
        };
        var publisher = publisher(kafka);
        assertThrows(IllegalStateException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> publisher));
        assertEquals("FAILED", metadata(options).path("components").path("producer").textValue());
        assertEquals("NOT_FINALIZED", metadata(options).path("experimentStatus").textValue());
        assertTrue(metadata(options).path("failures").toString().contains("async broker failure"));
        assertTrue(Files.exists(Path.of(options.get("csv-output"))));
        assertTrue(Files.readString(Path.of(options.get("event-csv-output"))).contains(",FAILED,"));
    }

    @Test
    void metadataFailureAfterGenerationFailsExecutionAndRetainsIncompleteSnapshot() throws Exception {
        Map<String, String> options = metadataOptions();
        Path output = Path.of(options.get("run-config-output"));
        var kafka = new MockProducer<String, String>(true, null, new StringSerializer(), new StringSerializer());
        var publisher = publisher(kafka);
        IOException failure = assertThrows(IOException.class,
                () -> SensorProducerApplication.execute(options, (bootstrap, acks, idempotence) -> {
                    try {
                        Files.move(output, output.resolveSibling("before-failure.json"));
                        Files.createDirectory(output);
                    } catch (IOException exception) {
                        throw new AssertionError(exception);
                    }
                    return publisher;
                }));
        assertTrue(failure.getMessage().contains("Run metadata persistence failed"));
        assertTrue(kafka.closed());
        assertFalse(Files.exists(Path.of(options.get("csv-output"))));
        assertEquals("NOT_FINALIZED", new ObjectMapper().readTree(
                Files.readAllBytes(output.resolveSibling("before-failure.json"))).path("experimentStatus").textValue());
    }

    private Map<String, String> metadataOptions() throws Exception {
        Map<String, String> options = options();
        options.put("run-config-output", tempDir.resolve("run-1/run-config.json").toString());
        return options;
    }

    private static JsonNode metadata(Map<String, String> options) throws IOException {
        return new ObjectMapper().readTree(Files.readAllBytes(Path.of(options.get("run-config-output"))));
    }

    private Map<String, String> options() throws Exception {
        return parse(new String[]{"--bootstrap-servers", "unused:9092", "--run-id", "run-1",
                "--random-seed", "123", "--initial-event-timestamp", "1700000000000",
                "--timestamp-increment", "1", "--target-events-per-second", "1",
                "--warm-up-seconds", "0", "--measurement-seconds", "1",
                "--csv-output", tempDir.resolve("totals.csv").toString()});
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> parse(String[] args) throws Exception {
        var parse = SensorProducerApplication.class.getDeclaredMethod("parse", String[].class);
        parse.setAccessible(true);
        try {
            return (Map<String, String>) parse.invoke(null, (Object) args);
        } catch (InvocationTargetException exception) {
            if (exception.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw new AssertionError(exception.getCause());
        }
    }

    private static String[] arguments(Map<String, String> options) {
        var args = new ArrayList<String>();
        options.forEach((name, value) -> {
            args.add("--" + name);
            args.add(value);
        });
        return args.toArray(String[]::new);
    }

    private static SensorEventPublisher publisher(MockProducer<String, String> kafka) throws Exception {
        var constructor = SensorEventPublisher.class.getDeclaredConstructor(Producer.class);
        constructor.setAccessible(true);
        return constructor.newInstance(kafka);
    }
}
