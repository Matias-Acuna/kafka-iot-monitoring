package com.kafkaiot.measurement.io;

import com.kafkaiot.measurement.metrics.MeasurementAggregator;
import com.kafkaiot.measurement.metrics.MeasurementAggregator.ReceivedResult;
import com.kafkaiot.measurement.model.SensorResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorResultCsvWriterTest {
    private static final String HEADER = "runId,eventId,deviceId,eventTime,sentAt,ruleType,triggered,"
            + "resultReceivedAt,latencyMs,duplicate,negativeLatency\r\n";
    private static final SensorResult RESULT = new SensorResult("event-1", "run-1", "device-1",
            1_700_000_000_000L, 1_700_000_000_100L, "RAPID_CHANGE", true);

    @TempDir
    Path tempDir;

    @Test
    void createsParentsAndWritesExactSingleLineHeaderWithoutInventingObservations() throws IOException {
        Path output = tempDir.resolve("run-1/nested/sensor-results.csv");
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.flush();
            assertEquals(HEADER, read(output));
            assertEquals(1, Files.readAllLines(output).size());
        }
        assertTrue(Files.isDirectory(output.getParent()));
    }

    @Test
    void serializesAllFieldsOfUniqueResultWithPositiveLatency() throws IOException {
        Path output = tempDir.resolve("unique.csv");
        var aggregator = new MeasurementAggregator();
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(aggregator.registerResult(RESULT, 1_700_000_000_125L));
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                + "1700000000125,25,false,false\r\n", read(output));
    }

    @Test
    void exportsUniqueAndDuplicateDeliveriesWithoutIndependentDeduplication() throws IOException {
        Path output = tempDir.resolve("duplicates.csv");
        var aggregator = new MeasurementAggregator();
        SensorResult result = new SensorResult("event-1", "run-1", "device-1", 100, 200,
                "HIGH_FREQUENCY", false);
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(aggregator.registerResult(result, 210));
            writer.writeObservation(aggregator.registerResult(result, 220));
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,100,200,HIGH_FREQUENCY,false,210,10,false,false\r\n"
                + "run-1,event-1,device-1,100,200,HIGH_FREQUENCY,false,220,20,true,false\r\n", read(output));
        assertEquals(2, aggregator.getReceivedResultCount());
        assertEquals(1, aggregator.getDuplicateResultCount());
    }

    @Test
    void preservesZeroLatencyWithoutNegativeFlag() throws IOException {
        Path output = tempDir.resolve("zero.csv");
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(new ReceivedResult(RESULT, RESULT.sentAt(), 0, false));
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                + "1700000000100,0,false,false\r\n", read(output));
    }

    @Test
    void preservesAndFlagsNegativeLatency() throws IOException {
        Path output = tempDir.resolve("negative.csv");
        var aggregator = new MeasurementAggregator();
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(aggregator.registerResult(RESULT, 1_700_000_000_075L));
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                + "1700000000075,-25,false,true\r\n", read(output));
    }

    @Test
    void usesSuppliedObservationValuesWithoutRecalculatingThem() throws IOException {
        Path output = tempDir.resolve("supplied.csv");
        // Deliberately inconsistent values prove that the writer only persists the supplied observation.
        var observation = new ReceivedResult(RESULT, 123, -777, true);
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(observation);
        }
        assertEquals(HEADER
                + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                + "123,-777,true,true\r\n", read(output));
    }

    @Test
    void escapesAllTextFieldsAndPreservesUtf8AndCrLf() throws IOException {
        Path output = tempDir.resolve("escaped.csv");
        var result = new SensorResult("event\"1", "run,温度", "device\r\n1", 100, 200,
                "rule\n\"é\"", false);
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(new ReceivedResult(result, 210, 10, false));
        }
        String expected = HEADER
                + "\"run,温度\",\"event\"\"1\",\"device\r\n1\",100,200,\"rule\n\"\"é\"\"\",false,"
                + "210,10,false,false\r\n";
        assertEquals(expected, read(output));
        assertArrayEquals(expected.getBytes(StandardCharsets.UTF_8), Files.readAllBytes(output));
    }

    @Test
    void writesIncrementallyAndFlushesBeforeCloseWithOneHeader() throws IOException {
        Path output = tempDir.resolve("incremental.csv");
        try (var writer = new SensorResultCsvWriter(output)) {
            writer.writeObservation(new ReceivedResult(RESULT, 1_700_000_000_125L, 25, false));
            writer.flush();
            String first = HEADER
                    + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                    + "1700000000125,25,false,false\r\n";
            assertEquals(first, read(output));
            writer.writeObservation(new ReceivedResult(RESULT, 1_700_000_000_150L, 50, true));
            writer.flush();
            assertEquals(first
                    + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                    + "1700000000150,50,true,false\r\n", read(output));
        }
    }

    @Test
    void refusesToOverwriteExistingFile() throws IOException {
        Path output = tempDir.resolve("existing.csv");
        Files.writeString(output, "previous observations\r\n", StandardCharsets.UTF_8);
        assertThrows(FileAlreadyExistsException.class, () -> new SensorResultCsvWriter(output));
        assertEquals("previous observations\r\n", read(output));
    }

    @Test
    void closeFlushesBufferedDataAndFurtherWritesAndFlushesFail() throws IOException {
        Path output = tempDir.resolve("closed.csv");
        var observation = new ReceivedResult(RESULT, 1_700_000_000_125L, 25, false);
        var writer = new SensorResultCsvWriter(output);
        try (writer) {
            writer.writeObservation(observation);
        }
        String persisted = read(output);
        assertEquals(HEADER
                + "run-1,event-1,device-1,1700000000000,1700000000100,RAPID_CHANGE,true,"
                + "1700000000125,25,false,false\r\n", persisted);
        assertThrows(IOException.class, () -> writer.writeObservation(observation));
        assertThrows(IOException.class, writer::flush);
        writer.close();
        assertEquals(persisted, read(output));
    }

    @Test
    void rejectsNullObservationsWithoutInventingRows() throws IOException {
        assertThrows(NullPointerException.class, () -> new SensorResultCsvWriter(null));
        Path output = tempDir.resolve("invalid.csv");
        try (var writer = new SensorResultCsvWriter(output)) {
            assertThrows(NullPointerException.class, () -> writer.writeObservation(null));
            assertThrows(NullPointerException.class,
                    () -> writer.writeObservation(new ReceivedResult(null, 0, 0, false)));
        }
        assertEquals(HEADER, read(output));
    }

    @Test
    void propagatesCreationFailureWhenParentIsAFile() throws IOException {
        Path parent = tempDir.resolve("regular-file");
        Files.writeString(parent, "preserved");
        assertThrows(IOException.class, () -> new SensorResultCsvWriter(parent.resolve("results.csv")));
        assertEquals("preserved", Files.readString(parent));
    }

    private static String read(Path output) throws IOException {
        return Files.readString(output, StandardCharsets.UTF_8);
    }
}
