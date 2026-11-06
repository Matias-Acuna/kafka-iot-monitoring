package com.kafkaiot.producer.io;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunMetadataWriterTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String RUN = "run-1";

    @TempDir
    Path tempDir;

    private Path output() {
        return tempDir.resolve("experiments/results/" + RUN + "/run-config.json");
    }

    private ObjectNode read() throws IOException {
        return (ObjectNode) JSON.readTree(Files.readAllBytes(output()));
    }

    @Test
    void createsValidJsonWithExplicitUnknownsAndIncompleteDefaults() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            JsonNode metadata = read();
            assertEquals(1, metadata.path("schemaVersion").intValue());
            assertEquals(RUN, metadata.path("runId").textValue());
            assertEquals("NOT_FINALIZED", metadata.path("experimentStatus").textValue());
            assertEquals("NOT_RECORDED", metadata.path("components").path("producer").textValue());
            assertEquals("NOT_RECORDED", metadata.path("components").path("consumer").textValue());
            for (String field : List.of("configuration", "phaseDurationsMs", "clockAnchors",
                    "scheduled", "observed")) {
                assertTrue(metadata.has(field));
                assertTrue(metadata.get(field).isNull());
            }
            assertTrue(metadata.path("failures").isArray());
            assertEquals(0, metadata.path("failures").size());
        }
    }

    @Test
    void persistsConfigurationTimingFailuresAndEscapedUnicodeWithoutInventingValues() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            ObjectNode metadata = read();
            metadata.putObject("configuration").put("description", "温度 \"quoted\"\nline");
            metadata.putObject("phaseDurationsMs").put("measurement", 300_000);
            metadata.putObject("clockAnchors").put("nanoTime", -123L).put("epochMs", 1700000000000L);
            metadata.putObject("scheduled").put("measurementDeadlineEpochMs", 1700000360000L);
            metadata.putObject("observed").putNull("drainEnded");
            metadata.withArray("failures").addObject().put("message", "publication failed");
            writer.write(metadata);
            assertEquals(JSON.readTree(JSON.writeValueAsBytes(metadata)), read());
            assertEquals(-123L, read().path("clockAnchors").path("nanoTime").longValue());
        }
    }

    @Test
    void reopensSameRunWithoutChangingBytesAndSupportsControlledUpdate() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            ObjectNode metadata = read();
            metadata.putObject("observed").put("generationStoppedEpochMs", 1700000360100L);
            writer.write(metadata);
        }
        byte[] original = Files.readAllBytes(output());
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertArrayEquals(original, Files.readAllBytes(output()));
            ObjectNode metadata = read();
            metadata.withObject("components").put("producer", "COMPLETED");
            writer.write(metadata);
            assertEquals("COMPLETED", read().path("components").path("producer").textValue());
            assertEquals("NOT_FINALIZED", read().path("experimentStatus").textValue());
            assertEquals(1700000360100L, read().path("observed").path("generationStoppedEpochMs").longValue());
        }
    }

    @Test
    void rejectsAnotherOwnerWithoutChangingExistingMetadata() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertTrue(Files.isRegularFile(output()));
        }
        byte[] original = Files.readAllBytes(output());
        assertThrows(IOException.class, () -> new RunMetadataWriter(output(), "other-run"));
        assertArrayEquals(original, Files.readAllBytes(output()));
        // Failed acquisition/validation must release resources.
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertEquals(RUN, read().path("runId").textValue());
        }
    }

    @Test
    void rejectsConcurrentOwnersUntilClose() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            IOException failure = assertThrows(IOException.class,
                    () -> new RunMetadataWriter(output(), RUN));
            assertTrue(failure.getMessage().contains("already owned"));
        }
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertEquals(RUN, read().path("runId").textValue());
        }
    }

    @Test
    void rejectsInvalidSnapshotsAndPreservesFile() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            byte[] original = Files.readAllBytes(output());
            ObjectNode otherRun = read().put("runId", "other-run");
            assertThrows(IOException.class, () -> writer.write(otherRun));
            ObjectNode missingStatus = read();
            missingStatus.remove("experimentStatus");
            assertThrows(IllegalArgumentException.class, () -> writer.write(missingStatus));
            assertThrows(NullPointerException.class, () -> writer.write(null));
            assertArrayEquals(original, Files.readAllBytes(output()));
        }
    }

    @Test
    void preservesInvalidExistingJsonInsteadOfReplacingIt() throws IOException {
        Files.createDirectories(output().getParent());
        for (String invalid : List.of("{broken", "{}", "null", "{\"runId\":\"run-1\"} {}")) {
            Files.writeString(output(), invalid);
            assertThrows(IOException.class, () -> new RunMetadataWriter(output(), RUN));
            assertEquals(invalid, Files.readString(output()));
        }
    }

    @Test
    void unsupportedAtomicReplacementLeavesOldFileAndCleansTemporaryFile() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertTrue(Files.exists(output()));
        }
        byte[] original = Files.readAllBytes(output());
        var unsupported = new AtomicMoveNotSupportedException("source", "target", "test provider");
        try (var writer = new RunMetadataWriter(output(), RUN, (source, target) -> {
            assertEquals(output().getParent(), source.getParent());
            assertEquals(read().put("experimentStatus", "INCOMPLETE"),
                    JSON.readTree(Files.readAllBytes(source)));
            throw unsupported;
        })) {
            assertSame(unsupported, assertThrows(AtomicMoveNotSupportedException.class,
                    () -> writer.write(read().put("experimentStatus", "INCOMPLETE"))));
            assertArrayEquals(original, Files.readAllBytes(output()));
            assertNoTemporaryFiles();
        }
    }

    @Test
    void initialPersistenceFailureLeavesNoMetadataAndReleasesOwnership() throws IOException {
        IOException failure = new IOException("disk write failed");
        assertSame(failure, assertThrows(IOException.class,
                () -> new RunMetadataWriter(output(), RUN, (source, target) -> { throw failure; })));
        assertFalse(Files.exists(output()));
        assertNoTemporaryFiles();
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertEquals("NOT_FINALIZED", read().path("experimentStatus").textValue());
        }
    }

    @Test
    void failedUpdatePreservesPreviousSnapshotAndReleasesOwnership() throws IOException {
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertTrue(Files.exists(output()));
        }
        byte[] original = Files.readAllBytes(output());
        IOException failure = new IOException("replacement failed");
        try (var writer = new RunMetadataWriter(output(), RUN, (source, target) -> { throw failure; })) {
            assertSame(failure, assertThrows(IOException.class, () -> writer.write(read())));
            assertArrayEquals(original, Files.readAllBytes(output()));
            assertNoTemporaryFiles();
        }
        try (var writer = new RunMetadataWriter(output(), RUN)) {
            assertArrayEquals(original, Files.readAllBytes(output()));
        }
    }

    @Test
    void closeIsIdempotentAndRejectsFurtherWrites() throws IOException {
        var writer = new RunMetadataWriter(output(), RUN);
        ObjectNode metadata = read();
        writer.close();
        writer.close();
        assertThrows(IOException.class, () -> writer.write(metadata));
        assertEquals(metadata, read());
    }

    @Test
    void rejectsInvalidArgumentsAndUnusableParent() throws IOException {
        assertThrows(NullPointerException.class, () -> new RunMetadataWriter(null, RUN));
        assertThrows(IllegalArgumentException.class, () -> new RunMetadataWriter(output(), null));
        assertThrows(IllegalArgumentException.class, () -> new RunMetadataWriter(output(), " "));
        Path parent = tempDir.resolve("file");
        Files.writeString(parent, "existing");
        assertThrows(IOException.class, () -> new RunMetadataWriter(parent.resolve("run-config.json"), RUN));
        assertEquals("existing", Files.readString(parent));
    }

    private void assertNoTemporaryFiles() throws IOException {
        try (var files = Files.list(output().getParent())) {
            assertFalse(files.anyMatch(path -> path.getFileName().toString().endsWith(".tmp")));
        }
    }
}
