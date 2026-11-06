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
}
