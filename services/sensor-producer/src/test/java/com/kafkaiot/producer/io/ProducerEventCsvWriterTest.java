package com.kafkaiot.producer.io;

import com.kafkaiot.producer.io.ProducerEventCsvWriter.Status;
import com.kafkaiot.producer.model.SensorEvent;
import com.kafkaiot.producer.service.WorkloadRunner.Phase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerEventCsvWriterTest {
    private static final String HEADER =
            "runId,eventId,deviceId,phase,recordedAt,sentAt,status,partition,offset,error\r\n";
    private static final long RECORDED_AT = 1_700_000_000_456L;
    private static final SensorEvent EVENT = new SensorEvent("event-1", "run-1", "device-1",
            25.0, 1_600_000_000_000L, 1_700_000_000_123L);

    @TempDir
    Path tempDir;

    @Test
    void createsParentsAndWritesOnePhysicalHeaderLine() throws IOException {
        Path output = tempDir.resolve("nested/run/producer-events.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.flush();
            assertEquals(HEADER, read(output));
            assertEquals(1, Files.readAllLines(output).size());
        }
        assertTrue(Files.isDirectory(output.getParent()));
    }

    @Test
    void writesAttemptWithOriginalIdentifiersPhaseAndTimestamps() throws IOException {
        Path output = tempDir.resolve("attempt.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(EVENT, Phase.WARM_UP, RECORDED_AT, Status.ATTEMPTED, null, null, null);
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,WARM_UP,1700000000456,1700000000123,ATTEMPTED,,,\r\n", read(output));
    }

    @Test
    void writesConfirmationWithSuppliedPartitionAndOffset() throws IOException {
        Path output = tempDir.resolve("confirmation.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(EVENT, Phase.MEASUREMENT, RECORDED_AT, Status.CONFIRMED, 0, 42L, null);
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,MEASUREMENT,1700000000456,1700000000123,CONFIRMED,0,42,\r\n",
                read(output));
    }

    @Test
    void writesFailureWithErrorAndNoInventedMetadata() throws IOException {
        Path output = tempDir.resolve("failure.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(EVENT, Phase.MEASUREMENT, RECORDED_AT, Status.FAILED,
                    null, null, "publication failed");
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,MEASUREMENT,1700000000456,1700000000123,FAILED,,,publication failed\r\n",
                read(output));
    }

    @Test
    void escapesIdentifiersAndErrorsAndPreservesUtf8() throws IOException {
        Path output = tempDir.resolve("escaping.csv");
        SensorEvent event = new SensorEvent("event\"1", "run,温度", "device\r\n1", 25, 0, 123);
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(event, Phase.MEASUREMENT, 456, Status.FAILED,
                    null, null, "échec,\"quoted\"\r\nnext");
        }
        String expected = HEADER
                + "\"run,温度\",\"event\"\"1\",\"device\r\n1\",MEASUREMENT,456,123,FAILED,,,"
                + "\"échec,\"\"quoted\"\"\r\nnext\"\r\n";
        assertEquals(expected, read(output));
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), Files.readAllBytes(output));
    }

    @Test
    void leavesUnavailableOptionalFieldsEmpty() throws IOException {
        Path output = tempDir.resolve("optional.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(EVENT, Phase.MEASUREMENT, RECORDED_AT, Status.CONFIRMED, null, null, null);
            writer.writeObservation(EVENT, Phase.MEASUREMENT, RECORDED_AT, Status.FAILED, null, null, "");
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,MEASUREMENT,1700000000456,1700000000123,CONFIRMED,,,\r\n"
                + "run-1,event-1,device-1,MEASUREMENT,1700000000456,1700000000123,FAILED,,,\r\n", read(output));
    }

    @Test
    void writesMultipleObservationsIncrementallyWithOnlyOneHeader() throws IOException {
        Path output = tempDir.resolve("multiple.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(EVENT, Phase.MEASUREMENT, 100, Status.ATTEMPTED, null, null, null);
            writer.flush();
            String first = HEADER + "run-1,event-1,device-1,MEASUREMENT,100,1700000000123,ATTEMPTED,,,\r\n";
            assertEquals(first, read(output));
            writer.writeObservation(EVENT, Phase.MEASUREMENT, 200, Status.CONFIRMED, 1, 0L, null);
            writer.flush();
            assertEquals(first
                    + "run-1,event-1,device-1,MEASUREMENT,200,1700000000123,CONFIRMED,1,0,\r\n", read(output));
        }
    }

    @Test
    void closePersistsBufferedRowsAndRejectsFurtherWrites() throws IOException {
        Path output = tempDir.resolve("close.csv");
        var writer = new ProducerEventCsvWriter(output);
        try (writer) {
            writer.writeObservation(EVENT, Phase.MEASUREMENT, RECORDED_AT, Status.ATTEMPTED, null, null, null);
        }
        String persisted = read(output);
        assertEquals(HEADER
                + "run-1,event-1,device-1,MEASUREMENT,1700000000456,1700000000123,ATTEMPTED,,,\r\n", persisted);
        assertThrows(IOException.class,
                () -> writer.writeObservation(EVENT, Phase.MEASUREMENT, RECORDED_AT,
                        Status.CONFIRMED, 0, 1L, null));
        assertThrows(IOException.class, writer::flush);
        writer.close();
        assertEquals(persisted, read(output));
    }

    @Test
    void refusesToOverwriteExistingObservations() throws IOException {
        Path output = tempDir.resolve("existing.csv");
        String original = "previous experimental observations\r\n";
        Files.writeString(output, original, StandardCharsets.UTF_8);
        assertThrows(FileAlreadyExistsException.class, () -> new ProducerEventCsvWriter(output));
        assertEquals(original, read(output));
    }

    @Test
    void refusesToOpenSameOutputWhileWriterIsActive() throws IOException {
        Path output = tempDir.resolve("active.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            writer.writeObservation(EVENT, Phase.WARM_UP, RECORDED_AT, Status.ATTEMPTED, null, null, null);
            writer.flush();
            String original = read(output);
            assertThrows(FileAlreadyExistsException.class, () -> new ProducerEventCsvWriter(output));
            assertEquals(original, read(output));
        }
    }

    @Test
    void rejectsNullRequiredArgumentsWithoutWritingRows() throws IOException {
        assertThrows(NullPointerException.class, () -> new ProducerEventCsvWriter(null));
        Path output = tempDir.resolve("invalid.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            assertThrows(NullPointerException.class,
                    () -> writer.writeObservation(null, Phase.WARM_UP, 0, Status.ATTEMPTED, null, null, null));
            assertThrows(NullPointerException.class,
                    () -> writer.writeObservation(EVENT, null, 0, Status.ATTEMPTED, null, null, null));
            assertThrows(NullPointerException.class,
                    () -> writer.writeObservation(EVENT, Phase.WARM_UP, 0, null, null, null, null));
        }
        assertEquals(HEADER, read(output));
    }

    @Test
    void serializesConcurrentCallbackObservationsWithoutLostOrInterleavedRows() throws Exception {
        Path output = tempDir.resolve("concurrent.csv");
        try (var writer = new ProducerEventCsvWriter(output)) {
            try (var executor = Executors.newFixedThreadPool(4)) {
                var tasks = new ArrayList<Future<?>>();
                for (int thread = 0; thread < 4; thread++) {
                    int worker = thread;
                    tasks.add(executor.submit(() -> {
                        for (int i = 0; i < 50; i++) {
                            SensorEvent event = new SensorEvent("event-" + worker + "-" + i,
                                    "run-1", "device-1", 25, 0, 123);
                            writer.writeObservation(event, Phase.MEASUREMENT, 456, Status.CONFIRMED, worker,
                                    (long) i, null);
                        }
                        return null;
                    }));
                }
                for (Future<?> task : tasks) {
                    task.get();
                }
            }
        }
        var lines = Files.readAllLines(output, StandardCharsets.UTF_8);
        assertEquals(201, lines.size());
        assertEquals(HEADER.stripTrailing(), lines.getFirst());
        var actual = new HashSet<>(lines.subList(1, lines.size()));
        assertEquals(200, actual.size());
        for (int worker = 0; worker < 4; worker++) {
            for (int i = 0; i < 50; i++) {
                assertTrue(actual.contains("run-1,event-" + worker + "-" + i
                        + ",device-1,MEASUREMENT,456,123,CONFIRMED," + worker + "," + i + ","));
            }
        }
    }

    private static String read(Path output) throws IOException {
        return Files.readString(output, StandardCharsets.UTF_8);
    }
}
