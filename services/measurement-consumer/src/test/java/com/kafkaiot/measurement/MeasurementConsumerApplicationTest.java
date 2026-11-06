package com.kafkaiot.measurement;

import com.kafkaiot.measurement.consumer.MeasurementKafkaConsumer;
import com.kafkaiot.measurement.io.SensorResultCsvWriter;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MeasurementConsumerApplicationTest {
    private static final String JSON = """
            {"eventId":"event-1","runId":"run-1","deviceId":"device-1",
             "eventTime":1700000000000,"sentAt":1700000000100,"ruleType":"RAPID_CHANGE","triggered":true}
            """;

    @Test
    void readinessOptionIsExplicitAndValidated() throws Exception {
        var parse = MeasurementConsumerApplication.class.getDeclaredMethod("parse", String[].class);
        parse.setAccessible(true);
        for (String value : List.of("true", "false")) {
            @SuppressWarnings("unchecked")
            Map<String, String> options = (Map<String, String>) parse.invoke(null, (Object) new String[]{
                    "--bootstrap-servers", "unused:9092", "--group-id", "g1", "--readiness-stdout", value});
            assertEquals(value, options.get("readiness-stdout"));
        }
        var failure = assertThrows(InvocationTargetException.class,
                () -> parse.invoke(null, (Object) new String[]{"--bootstrap-servers", "unused:9092",
                        "--group-id", "g1", "--readiness-stdout", "yes"}));
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertThrows(IllegalArgumentException.class,
                () -> MeasurementConsumerApplication.main(new String[]{"--readiness-stdout"}));
    }

    @Test
    void readinessOutputIsOneJsonLineWithRequiredFields() throws Exception {
        var original = System.out;
        var bytes = new ByteArrayOutputStream();
        try (var captured = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(captured);
                MeasurementConsumerApplication.printReadiness(new MeasurementKafkaConsumer.ReadinessEvidence(
                        "MEASUREMENT_CONSUMER_READINESS", 1700000000000L, "group-1", "READY", List.of(2),
                        List.of(new MeasurementKafkaConsumer.PartitionPosition("sensor-results", 2, 42))));
            } finally {
                System.setOut(original);
            }
        }
        assertEquals(1, bytes.toString(StandardCharsets.UTF_8).lines().count());
        var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(bytes.toByteArray());
        assertEquals("MEASUREMENT_CONSUMER_READINESS", json.path("evidenceType").textValue());
        assertEquals(1700000000000L, json.path("observedAtEpochMs").longValue());
        assertEquals("group-1", json.path("groupId").textValue());
        assertEquals("READY", json.path("status").textValue());
        assertEquals(42, json.path("assignedResultPartitions").get(0).path("startingPosition").longValue());
    }

    @Test
    void pollingEvidenceUsesSameJsonInterfaceAndOutputFailuresAreExplicit() throws Exception {
        var original = System.out;
        var bytes = new ByteArrayOutputStream();
        var poll = new MeasurementKafkaConsumer.PollEvidence(
                "MEASUREMENT_CONSUMER_POLL_PROGRESS", 1700000000000L, "group-1", "NOT_READY",
                3, 3, 0, true);
        try (var captured = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(captured);
                MeasurementConsumerApplication.printReadiness(poll);
            } finally {
                System.setOut(original);
            }
        }
        var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(bytes.toByteArray());
        assertEquals("MEASUREMENT_CONSUMER_POLL_PROGRESS", json.path("evidenceType").textValue());
        assertEquals(3, json.path("successfulPolls").longValue());
        assertTrue(json.path("lastPollEmpty").booleanValue());
        try (var failed = new PrintStream(new java.io.OutputStream() {
            @Override
            public void write(int value) throws IOException {
                throw new IOException("stdout unavailable");
            }
        })) {
            try {
                System.setOut(failed);
                assertThrows(IllegalStateException.class, () -> MeasurementConsumerApplication.printReadiness(poll));
            } finally {
                System.setOut(original);
            }
        }
    }

    @TempDir
    Path tempDir;

    @Test
    void acceptsResultOutputAlongsideExistingOptions() throws Exception {
        String path = tempDir.resolve("run-1/sensor-results.csv").toString();
        assertEquals(Map.of("bootstrap-servers", "unused:9092", "group-id", "test-group",
                "auto-offset-reset", "latest", "result-csv-output", path), parse(new String[]{
                "--bootstrap-servers", "unused:9092", "--group-id", "test-group",
                "--auto-offset-reset", "latest", "--result-csv-output", path}));
    }

    @Test
    void rejectsMissingAndBlankOutputValues() {
        for (String[] args : List.of(new String[]{"--result-csv-output"},
                new String[]{"--result-csv-output", " "}, new String[]{"--result-csv-output", ""},
                new String[]{"--result-csv-output", "--group-id", "group"})) {
            assertThrows(IllegalArgumentException.class, () -> MeasurementConsumerApplication.main(args));
        }
    }

    @Test
    void absentOutputPreservesDefaultConfigurationCountsAndCreatesNoFiles() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        String console = captureOutput(() -> MeasurementConsumerApplication.execute(options(false),
                (properties, aggregator, writer) -> {
                    assertNull(writer);
                    assertEquals("earliest", properties.getProperty("auto.offset.reset"));
                    assertEquals("test-group", properties.getProperty("group.id"));
                    var consumer = consumer(kafka, aggregator, writer);
                    consumer.shutdown();
                    return consumer;
                }, completed));
        assertTrue(kafka.closed());
        assertEquals(0, completed.getCount());
        assertEquals("Measurement Consumer counts:\nTotal received results: 0\nUnique received results: 0\n"
                + "Duplicate result deliveries: 0\nReceived alerts: 0\n", console.replace("\r\n", "\n"));
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void helpDocumentsOptionalPathWithoutCreatingFiles() throws Exception {
        String console = captureOutput(() -> MeasurementConsumerApplication.main(new String[]{"--help"}));
        assertTrue(console.contains("--result-csv-output PATH"));
        assertTrue(console.contains("experiments/results/<runId>/sensor-results.csv"));
        assertTrue(console.contains("--auto-offset-reset earliest|latest (default: earliest)"));
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
    }

    @Test
    void passesWriterExportsDeliveriesAndClosesItBeforeSignalingCompletion() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        var supplied = new AtomicReference<SensorResultCsvWriter>();
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST) {
            @Override
            public void close() {
                assertEquals(1, completed.getCount());
                try {
                    supplied.get().flush();
                } catch (IOException exception) {
                    throw new AssertionError("Writer closed before Kafka", exception);
                }
                super.close();
            }
        };
        String console = captureOutput(() -> MeasurementConsumerApplication.execute(options(true),
                (properties, aggregator, writer) -> {
                    supplied.set(writer);
                    var consumer = consumer(kafka, aggregator, writer);
                    queue(kafka, consumer, JSON, JSON);
                    return consumer;
                }, completed));
        assertTrue(kafka.closed());
        assertEquals(0, completed.getCount());
        assertThrows(IOException.class, () -> supplied.get().flush());
        List<String> rows = Files.readAllLines(resultPath());
        assertEquals(3, rows.size());
        assertEquals("false", rows.get(1).split(",")[9]);
        assertEquals("true", rows.get(2).split(",")[9]);
        assertTrue(console.contains("Total received results: 2"));
        assertTrue(console.contains("Unique received results: 1"));
        assertTrue(console.contains("Duplicate result deliveries: 1"));
        assertTrue(console.contains("Received alerts: 0"));
    }

    @Test
    void startupFailureClosesWriterPreservesOriginalAndSignalsCompletion() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        var supplied = new AtomicReference<SensorResultCsvWriter>();
        RuntimeException original = new IllegalStateException("mock startup failure");
        assertSame(original, assertThrows(IllegalStateException.class,
                () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                    supplied.set(writer);
                    throw original;
                }, completed)));
        assertThrows(IOException.class, () -> supplied.get().flush());
        assertEquals(1, Files.readAllLines(resultPath()).size());
        assertEquals(0, completed.getCount());
    }

    @Test
    void consumptionFailurePreservesPartialObservationsWithoutSuccessSummary() throws Exception {
        CountDownLatch completed = new CountDownLatch(1);
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        var supplied = new AtomicReference<SensorResultCsvWriter>();
        var console = new AtomicReference<String>();
        String output = captureOutput(() -> {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                        supplied.set(writer);
                        var consumer = consumer(kafka, aggregator, writer);
                        queue(kafka, consumer, JSON, "{invalid");
                        return consumer;
                    }, completed));
            console.set(failure.getMessage());
        });
        assertFalse(output.contains("Measurement Consumer counts:"));
        assertTrue(console.get().contains("Failed to process record"));
        assertTrue(kafka.closed());
        assertThrows(IOException.class, () -> supplied.get().flush());
        assertEquals(2, Files.readAllLines(resultPath()).size());
        assertEquals(0, completed.getCount());
    }

    @Test
    void writerCloseFailureIsVisibleAndCompletionWaitsForCleanup() {
        CountDownLatch completed = new CountDownLatch(1);
        IOException closeFailure = new IOException("mock CSV close failure");
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        UncheckedIOException failure = assertThrows(UncheckedIOException.class,
                () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                    failOnClose(writer, completed, closeFailure);
                    var consumer = consumer(kafka, aggregator, writer);
                    consumer.shutdown();
                    return consumer;
                }, completed));
        assertSame(closeFailure, failure.getCause());
        assertTrue(kafka.closed());
        assertEquals(0, completed.getCount());
    }

    @Test
    void preservesStartupFailureAndSuppressesAdditionalCsvCleanupFailure() {
        CountDownLatch completed = new CountDownLatch(1);
        RuntimeException original = new IllegalStateException("mock initialization failure");
        IOException cleanup = new IOException("mock CSV close failure");
        assertSame(original, assertThrows(IllegalStateException.class,
                () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                    failOnClose(writer, completed, cleanup);
                    throw original;
                }, completed)));
        assertEquals(List.of(cleanup), List.of(original.getSuppressed()));
        assertEquals(0, completed.getCount());
    }

    @Test
    void reportsCsvCreationErrorsWithoutOverwritingExistingFileOrStartingKafka() throws Exception {
        Files.createDirectories(resultPath().getParent());
        Files.writeString(resultPath(), "existing observations");
        PrintStream original = System.err;
        var errors = new ByteArrayOutputStream();
        try (var captured = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            try {
                System.setErr(captured);
                UncheckedIOException failure = assertThrows(UncheckedIOException.class,
                        () -> MeasurementConsumerApplication.main(new String[]{"--bootstrap-servers", "unused:9092",
                                "--group-id", "test-group", "--result-csv-output", resultPath().toString()}));
                assertInstanceOf(FileAlreadyExistsException.class, failure.getCause());
            } finally {
                System.setErr(original);
            }
        }
        assertTrue(errors.toString(StandardCharsets.UTF_8).contains("Result CSV file-writing or cleanup failed"));
        assertTrue(errors.toString(StandardCharsets.UTF_8).contains("execution incomplete"));
        assertEquals("existing observations", Files.readString(resultPath()));
    }

    @Test
    void rejectsInvalidOutputPathBeforeConsumerConstruction() {
        Map<String, String> options = options(true);
        options.put("result-csv-output", "\u0000");
        assertThrows(IllegalArgumentException.class,
                () -> MeasurementConsumerApplication.execute(options, (properties, aggregator, writer) -> {
                    throw new AssertionError("Must not create consumer");
                }, new CountDownLatch(1)));
    }

    @Test
    void preservesRejectionOfUnknownDuplicateAndInvalidResetOptions() {
        for (String[] args : List.of(
                new String[]{"--bootstrap-servers", "unused:9092", "--group-id", "group", "--unknown", "x"},
                new String[]{"--bootstrap-servers", "unused:9092", "--group-id", "group", "--group-id", "other"},
                new String[]{"--bootstrap-servers", "unused:9092", "--group-id", "group", "--auto-offset-reset", "none"})) {
            assertThrows(IllegalArgumentException.class, () -> MeasurementConsumerApplication.main(args));
        }
    }

    @Test
    void unrelatedIoCausesAndSuppressedErrorsAreNotReportedAsCsvFailures() {
        IOException cause = new IOException("unrelated network failure");
        IOException suppressed = new IOException("unrelated resource cleanup");
        for (RuntimeException original : List.of(new IllegalStateException("application failed", cause),
                new UncheckedIOException("unrelated Kafka I/O", cause),
                new IllegalStateException("Failed to write result CSV for topic unrelated-application", cause))) {
            original.addSuppressed(suppressed);
            CountDownLatch completed = new CountDownLatch(1);
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> MeasurementConsumerApplication.execute(options(false), (properties, aggregator, writer) -> {
                        throw original;
                    }, completed));
            assertSame(original, failure);
            assertSame(cause, failure.getCause());
            assertEquals(List.of(suppressed), List.of(failure.getSuppressed()));
            assertEquals(0, completed.getCount());
            String diagnostic = captureErrors(() -> MeasurementConsumerApplication.reportFailure(failure));
            assertTrue(diagnostic.contains("Measurement Consumer startup or execution failed"));
            assertFalse(diagnostic.contains("Result CSV file-writing or cleanup failed"));
        }
    }

    @Test
    void genuineCsvCreationFailureRetainsItsIoCauseAndCsvDiagnostic() throws Exception {
        Files.createDirectories(resultPath().getParent());
        Files.writeString(resultPath(), "preserved observations");
        UncheckedIOException failure = assertThrows(UncheckedIOException.class,
                () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                    throw new AssertionError("Must not construct consumer");
                }, new CountDownLatch(1)));
        assertInstanceOf(FileAlreadyExistsException.class, failure.getCause());
        assertTrue(captureErrors(() -> MeasurementConsumerApplication.reportFailure(failure))
                .contains("Result CSV file-writing or cleanup failed"));
        assertEquals("preserved observations", Files.readString(resultPath()));
    }

    @Test
    void genuineConsumerCsvWriteFailureIsReportedWithOriginalIoCause() {
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                    try {
                        writer.close();
                    } catch (IOException exception) {
                        throw new AssertionError(exception);
                    }
                    var consumer = consumer(kafka, aggregator, writer);
                    queue(kafka, consumer, JSON);
                    return consumer;
                }, new CountDownLatch(1)));
        assertTrue(failure.getMessage().contains("Failed to write result CSV for topic sensor-results"));
        assertInstanceOf(IOException.class, failure.getCause());
        assertTrue(kafka.closed());
        assertTrue(captureErrors(() -> MeasurementConsumerApplication.reportFailure(failure))
                .contains("Result CSV file-writing or cleanup failed"));
    }

    @Test
    void suppressedCsvCleanupRemainsDetectableWithoutReplacingProcessingFailure() {
        CountDownLatch completed = new CountDownLatch(1);
        IOException network = new IOException("unrelated network failure");
        RuntimeException original = new IllegalStateException("processing failed", network);
        IOException cleanup = new IOException("CSV cleanup failure");
        assertSame(original, assertThrows(IllegalStateException.class,
                () -> MeasurementConsumerApplication.execute(options(true), (properties, aggregator, writer) -> {
                    failOnClose(writer, completed, cleanup);
                    throw original;
                }, completed)));
        assertSame(network, original.getCause());
        assertEquals(List.of(cleanup), List.of(original.getSuppressed()));
        assertEquals(0, completed.getCount());
        assertTrue(captureErrors(() -> MeasurementConsumerApplication.reportFailure(original))
                .contains("Result CSV file-writing or cleanup failed"));
    }

    private static String captureErrors(Runnable action) {
        PrintStream original = System.err;
        var errors = new ByteArrayOutputStream();
        try (var captured = new PrintStream(errors, true, StandardCharsets.UTF_8)) {
            try {
                System.setErr(captured);
                action.run();
            } finally {
                System.setErr(original);
            }
        }
        return errors.toString(StandardCharsets.UTF_8);
    }

    private Map<String, String> options(boolean csv) {
        var options = new HashMap<>(Map.of("bootstrap-servers", "unused:9092", "group-id", "test-group"));
        if (csv) {
            options.put("result-csv-output", resultPath().toString());
        }
        return options;
    }

    @Test
    void csvClosureEvidenceFollowsActualCloseAndPrecedesCompletionSignal() throws Exception {
        var options = options(true);
        options.put("readiness-stdout", "true");
        var completed = new CountDownLatch(1);
        var supplied = new AtomicReference<SensorResultCsvWriter>();
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        String output = captureOutput(() -> MeasurementConsumerApplication.execute(options,
                (properties, aggregator, writer) -> {
                    supplied.set(writer);
                    var consumer = consumer(kafka, aggregator, writer);
                    queue(kafka, consumer, JSON);
                    return consumer;
                }, completed));
        assertThrows(IOException.class, () -> supplied.get().flush());
        var event = csvClosure(output);
        assertEquals("CLOSED", event.path("status").textValue());
        assertTrue(event.path("observedAtEpochMs").longValue() > 0);
        assertEquals(resultPath().toString(), event.path("outputPath").textValue());
        assertEquals(0, completed.getCount());
        assertEquals(2, Files.readAllLines(resultPath()).size());
    }

    @Test
    void csvClosureFailureNeverReportsSuccessfulClosure() throws Exception {
        var options = options(true);
        options.put("readiness-stdout", "true");
        var completed = new CountDownLatch(1);
        var failure = new IOException("CSV close failed");
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        String output = captureOutput(() -> {
            var thrown = assertThrows(UncheckedIOException.class,
                    () -> MeasurementConsumerApplication.execute(options, (properties, aggregator, writer) -> {
                        failOnClose(writer, completed, failure);
                        var consumer = consumer(kafka, aggregator, writer);
                        consumer.shutdown();
                        return consumer;
                    }, completed));
            assertSame(failure, thrown.getCause());
        });
        assertEquals("FAILED", csvClosure(output).path("status").textValue());
        assertEquals("CSV close failed", csvClosure(output).path("failure").path("message").textValue());
        assertFalse(output.contains("Measurement Consumer counts:"));
        assertEquals(0, completed.getCount());
    }

    @Test
    void csvWriteFailureCannotBecomeSuccessfulClosureEvidence() throws Exception {
        var options = options(true);
        options.put("readiness-stdout", "true");
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        String output = captureOutput(() -> assertThrows(IllegalStateException.class,
                () -> MeasurementConsumerApplication.execute(options, (properties, aggregator, writer) -> {
                    try {
                        writer.close();
                    } catch (IOException exception) {
                        throw new AssertionError(exception);
                    }
                    var consumer = consumer(kafka, aggregator, writer);
                    queue(kafka, consumer, JSON);
                    return consumer;
                }, new CountDownLatch(1))));
        assertEquals("CLOSED_WITH_WRITE_FAILURE", csvClosure(output).path("status").textValue());
        assertTrue(csvClosure(output).path("failure").path("message").textValue().contains("Failed to write result CSV"));
    }

    @Test
    void csvFailureEvidencePreservesOriginalAndSuppressedCleanupFailures() throws Exception {
        var options = options(true);
        options.put("readiness-stdout", "true");
        var completed = new CountDownLatch(1);
        var original = new IllegalStateException("startup failed");
        var cleanup = new IOException("CSV cleanup failed");
        String output = captureOutput(() -> {
            assertSame(original, assertThrows(IllegalStateException.class,
                    () -> MeasurementConsumerApplication.execute(options, (properties, aggregator, writer) -> {
                        failOnClose(writer, completed, cleanup);
                        throw original;
                    }, completed)));
        });
        assertTrue(List.of(original.getSuppressed()).contains(cleanup));
        assertEquals("FAILED", csvClosure(output).path("status").textValue());
        assertEquals(0, completed.getCount());
    }

    private static com.fasterxml.jackson.databind.JsonNode csvClosure(String output) throws IOException {
        for (String line : output.lines().filter(value -> value.startsWith("{")).toList()) {
            var event = new com.fasterxml.jackson.databind.ObjectMapper().readTree(line);
            if ("MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED".equals(event.path("evidenceType").textValue())) {
                return event;
            }
        }
        throw new AssertionError("CSV closure evidence was not emitted");
    }

    private Path resultPath() {
        return tempDir.resolve("run-1/sensor-results.csv");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> parse(String[] args) throws Exception {
        var method = MeasurementConsumerApplication.class.getDeclaredMethod("parse", String[].class);
        method.setAccessible(true);
        try {
            return (Map<String, String>) method.invoke(null, (Object) args);
        } catch (InvocationTargetException exception) {
            if (exception.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw new AssertionError(exception.getCause());
        }
    }

    private static MeasurementKafkaConsumer consumer(MockConsumer<String, String> kafka,
                                                     MeasurementAggregator aggregator, SensorResultCsvWriter writer) {
        try {
            var constructor = MeasurementKafkaConsumer.class.getDeclaredConstructor(
                    Consumer.class, MeasurementAggregator.class, SensorResultCsvWriter.class);
            constructor.setAccessible(true);
            return constructor.newInstance(kafka, aggregator, writer);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    private static void queue(MockConsumer<String, String> kafka, MeasurementKafkaConsumer consumer, String... json) {
        kafka.schedulePollTask(() -> {
            var partition = new TopicPartition("sensor-results", 0);
            kafka.updateBeginningOffsets(Map.of(partition, 0L));
            kafka.rebalance(List.of(partition));
            for (int i = 0; i < json.length; i++) {
                kafka.addRecord(new ConsumerRecord<>("sensor-results", 0, i, "device-1", json[i]));
            }
        });
        kafka.schedulePollTask(consumer::shutdown);
    }

    private static void failOnClose(SensorResultCsvWriter writer, CountDownLatch completed, IOException failure) {
        try {
            var field = SensorResultCsvWriter.class.getDeclaredField("writer");
            field.setAccessible(true);
            var original = (BufferedWriter) field.get(writer);
            field.set(writer, new BufferedWriter(original) {
                @Override
                public void close() throws IOException {
                    assertEquals(1, completed.getCount());
                    super.close();
                    throw failure;
                }
            });
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError(exception);
        }
    }

    private static String captureOutput(Runnable action) {
        PrintStream original = System.out;
        var output = new ByteArrayOutputStream();
        try (var captured = new PrintStream(output, true, StandardCharsets.UTF_8)) {
            try {
                System.setOut(captured);
                action.run();
            } finally {
                System.setOut(original);
            }
        }
        return output.toString(StandardCharsets.UTF_8);
    }
    @Test
    void configurationEvidenceContainsAppliedDefaultsAndCsvDestination() throws Exception {
        var properties = com.kafkaiot.measurement.config.ConsumerKafkaConfig.createProperties("broker:9092", "group", "earliest");
        var evidence = MeasurementConsumerApplication.configurationEvidence(properties,
                Map.of("result-csv-output", "observations/sensor-results.csv"));
        assertEquals("MEASUREMENT_CONSUMER_CONFIGURATION", evidence.evidenceType());
        assertEquals("group", evidence.groupId());
        assertTrue(evidence.observedAtEpochMs() > 0);
        assertEquals(List.of("sensor-results", "sensor-alerts"), evidence.subscribedTopics());
        var applied = evidence.appliedConfiguration();
        assertEquals(List.of("broker:9092"), applied.get("bootstrap.servers"));
        assertEquals("earliest", applied.get("auto.offset.reset"));
        assertEquals(true, applied.get("enable.auto.commit"));
        assertEquals(5000, applied.get("auto.commit.interval.ms"));
        assertEquals(500, applied.get("max.poll.records"));
        assertEquals(300000, applied.get("max.poll.interval.ms"));
        assertEquals("classic", applied.get("group.protocol"));
        assertEquals("read_uncommitted", applied.get("isolation.level"));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", applied.get("value.deserializer"));
        assertTrue(evidence.resultCsvEnabled());
        assertEquals(Path.of("observations/sensor-results.csv").toAbsolutePath().normalize().toString(), evidence.resultCsvOutput());
        assertTrue(evidence.unavailable().contains("runtimeGeneratedClientId"));
        var json = new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(evidence);
        assertFalse(json.has("status"));
    }

    @Test
    void configurationEvidencePreservesLatestPolicyAndDisabledCsv() {
        var properties = com.kafkaiot.measurement.config.ConsumerKafkaConfig.createProperties("broker:9092", "group", "latest");
        var evidence = MeasurementConsumerApplication.configurationEvidence(properties, Map.of());
        assertEquals("latest", evidence.appliedConfiguration().get("auto.offset.reset"));
        assertFalse(evidence.resultCsvEnabled());
        assertNull(evidence.resultCsvOutput());
    }

    @Test
    void configurationEvidenceDoesNotLeakSecretsOrChangeProperties() throws Exception {
        var properties = com.kafkaiot.measurement.config.ConsumerKafkaConfig.createProperties("broker:9092", "group", "earliest");
        properties.setProperty("ssl.keystore.password", "private-secret");
        properties.setProperty("custom.token", "private-token");
        var original = properties.clone();
        String json = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(
                MeasurementConsumerApplication.configurationEvidence(properties, Map.of()));
        assertFalse(json.contains("private-secret"));
        assertFalse(json.contains("private-token"));
        assertFalse(json.contains("ssl.keystore.password"));
        assertEquals(original, properties);
    }

    @Test
    void configurationEvidenceDisabledDoesNotParseOrPrint() {
        assertEquals("", captureOutput(() -> MeasurementConsumerApplication.emitConfiguration(null, Map.of())));
        assertEquals("", captureOutput(() -> MeasurementConsumerApplication.emitConfiguration(null,
                Map.of("readiness-stdout", "false"))));
    }

    @Test
    void applicationEmitsOneConfigurationBeforeConsumerConstruction() throws Exception {
        var options = options(false);
        options.put("readiness-stdout", "true");
        var completed = new CountDownLatch(1);
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        String output = captureOutput(() -> MeasurementConsumerApplication.execute(options,
                (properties, aggregator, writer) -> {
                    var consumer = consumer(kafka, aggregator, writer);
                    consumer.shutdown();
                    return consumer;
                }, completed));
        var events = output.lines().filter(line -> line.startsWith("{")).toList();
        assertEquals(1, events.size());
        var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(events.getFirst());
        assertEquals("MEASUREMENT_CONSUMER_CONFIGURATION", json.path("evidenceType").textValue());
        assertEquals(0, completed.getCount());
    }

    @Test
    void configurationOutputFailurePreventsConsumerCreationAndReleasesCompletion() {
        var options = options(false);
        options.put("readiness-stdout", "true");
        var completed = new CountDownLatch(1);
        var original = System.out;
        try (var broken = new PrintStream(new java.io.OutputStream() {
            @Override public void write(int value) throws IOException { throw new IOException("stdout failed"); }
        })) {
            try {
                System.setOut(broken);
                assertThrows(IllegalStateException.class, () -> MeasurementConsumerApplication.execute(options,
                        (properties, aggregator, writer) -> { throw new AssertionError("must not construct consumer"); }, completed));
            } finally {
                System.setOut(original);
            }
        }
        assertEquals(0, completed.getCount());
    }

    @Test
    void configurationSerializerFailureUsesExistingExplicitErrorPath() {
        var failure = assertThrows(IllegalStateException.class, () -> MeasurementConsumerApplication.printReadiness(new Object() {
            public String getValue() { throw new IllegalStateException("cannot serialize"); }
        }));
        assertTrue(failure.getMessage().contains("serialized"));
        assertTrue(failure.getCause() instanceof com.fasterxml.jackson.core.JsonProcessingException);
    }
    @Test
    void optionalRunIsolationIsValidatedAndIncludedInConfigurationEvidence() throws Exception {
        var parse = MeasurementConsumerApplication.class.getDeclaredMethod("parse", String[].class);
        parse.setAccessible(true);
        @SuppressWarnings("unchecked")
        var parsed = (Map<String, String>) parse.invoke(null, (Object) new String[]{
                "--bootstrap-servers", "broker:9092", "--group-id", "group", "--run-id", "run-1"});
        var properties = com.kafkaiot.measurement.config.ConsumerKafkaConfig.createProperties("broker:9092", "group", "earliest");
        assertEquals("run-1", MeasurementConsumerApplication.configurationEvidence(properties, parsed).runIdFilter());
        assertNull(MeasurementConsumerApplication.configurationEvidence(properties, Map.of()).runIdFilter());
        var failure = assertThrows(InvocationTargetException.class, () -> parse.invoke(null, (Object) new String[]{
                "--bootstrap-servers", "broker:9092", "--group-id", "group", "--run-id", " "}));
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
    }
    @Test
    void dashboardPortIsOptionalAndStrictlyValidated() throws Exception {
        var parse = MeasurementConsumerApplication.class.getDeclaredMethod("parse", String[].class);
        parse.setAccessible(true);
        @SuppressWarnings("unchecked")
        var defaults = (Map<String, String>) parse.invoke(null, (Object) new String[]{
                "--bootstrap-servers", "broker:9092", "--group-id", "group"});
        assertFalse(defaults.containsKey("dashboard-port"));
        @SuppressWarnings("unchecked")
        var configured = (Map<String, String>) parse.invoke(null, (Object) new String[]{
                "--bootstrap-servers", "broker:9092", "--group-id", "group", "--dashboard-port", "8080"});
        assertEquals("8080", configured.get("dashboard-port"));
        for (String port : List.of("0", "-1", "65536", "2147483648", "text")) {
            var failure = assertThrows(InvocationTargetException.class, () -> parse.invoke(null, (Object) new String[]{
                    "--bootstrap-servers", "broker:9092", "--group-id", "group", "--dashboard-port", port}));
            assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        }
    }

    @Test
    void applicationOwnsHttpClosureBeforeSignallingCompletion() throws Exception {
        int port;
        try (var reservation = new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            port = reservation.getLocalPort();
        }
        var options = options(false);
        options.put("dashboard-port", Integer.toString(port));
        var completed = new CountDownLatch(1);
        var kafka = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
        try (var client = java.net.http.HttpClient.newHttpClient()) {
            var request = java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://127.0.0.1:" + port + "/metrics/dashboard"))
                    .timeout(java.time.Duration.ofSeconds(3)).GET().build();
            captureOutput(() -> MeasurementConsumerApplication.execute(options, (properties, aggregator, writer) -> {
                try {
                    assertEquals(200, client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString()).statusCode());
                } catch (IOException | InterruptedException failure) {
                    throw new AssertionError(failure);
                }
                var consumer = consumer(kafka, aggregator, writer);
                consumer.shutdown();
                return consumer;
            }, completed));
            assertEquals(0, completed.getCount());
            assertThrows(IOException.class, () -> client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString()));
        }
    }

    @Test
    void dashboardBindFailureIsNotClassifiedAsCsvFailure() throws Exception {
        try (var reservation = new java.net.ServerSocket(0, 1, java.net.InetAddress.getByName("127.0.0.1"))) {
            var options = options(false);
            options.put("dashboard-port", Integer.toString(reservation.getLocalPort()));
            var completed = new CountDownLatch(1);
            var failure = assertThrows(UncheckedIOException.class, () -> MeasurementConsumerApplication.execute(options,
                    (properties, aggregator, writer) -> { throw new AssertionError("Consumer must not be created after HTTP bind failure"); }, completed));
            assertTrue(failure.getMessage().contains("Dashboard HTTP startup"));
            assertFalse(captureErrors(() -> MeasurementConsumerApplication.reportFailure(failure)).contains("Result CSV file-writing"));
            assertEquals(0, completed.getCount());
        }
    }

}
