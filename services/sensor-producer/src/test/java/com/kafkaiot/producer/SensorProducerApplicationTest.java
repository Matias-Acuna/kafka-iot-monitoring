package com.kafkaiot.producer;

import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.service.SensorEventPublisher;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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
        assertTrue(help.contains("experiments/results/<runId>/producer-events.csv"));
        assertTrue(help.contains("--csv-output PATH"));
        try (var files = Files.list(tempDir)) {
            assertEquals(0, files.count());
        }
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
