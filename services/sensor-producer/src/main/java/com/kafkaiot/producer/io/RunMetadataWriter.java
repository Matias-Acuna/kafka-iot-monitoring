package com.kafkaiot.producer.io;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Owns one run-config.json until closed; all participating writers must use this lock. */
public final class RunMetadataWriter implements Closeable {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final Path output;
    private final String runId;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final Replacement replacement;
    private boolean closed;

    /** Creates unknown/default metadata, or acquires an existing same-run file without changing it. */
    public RunMetadataWriter(Path output, String runId) throws IOException {
        this(output, runId, (source, target) -> Files.move(source, target,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING));
    }

    // Allows deterministic replacement-failure tests without filesystem-specific assumptions.
    RunMetadataWriter(Path output, String runId, Replacement replacement) throws IOException {
        Objects.requireNonNull(output, "output");
        if (runId == null || runId.isBlank()) {
            throw new IllegalArgumentException("runId must not be null or blank");
        }
        this.runId = runId;
        this.replacement = Objects.requireNonNull(replacement, "replacement");
        Path absolute = output.toAbsolutePath().normalize();
        Files.createDirectories(absolute.getParent());
        this.output = absolute.getParent().toRealPath().resolve(absolute.getFileName());
        // Keep the lock file: deleting it would allow different processes to lock different inodes.
        lockChannel = FileChannel.open(this.output.resolveSibling(this.output.getFileName() + ".lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
        try {
            lock = lockChannel.tryLock();
            if (lock == null) {
                throw new IOException("Run metadata is already owned: " + this.output);
            }
        } catch (IOException | RuntimeException exception) {
            try {
                lockChannel.close();
            } catch (IOException cleanup) {
                exception.addSuppressed(cleanup);
            }
            if (exception instanceof OverlappingFileLockException) {
                throw new IOException("Run metadata is already owned: " + this.output, exception);
            }
            throw exception;
        }
        try {
            if (Files.exists(this.output, LinkOption.NOFOLLOW_LINKS)) {
                validateExisting();
            } else {
                ObjectNode initial = JSON.createObjectNode();
                initial.put("schemaVersion", 1);
                initial.put("runId", runId);
                initial.putNull("configuration");
                initial.putNull("phaseDurationsMs");
                initial.putNull("clockAnchors");
                initial.putNull("scheduled");
                initial.putNull("observed");
                initial.putObject("components").put("producer", "NOT_RECORDED")
                        .put("consumer", "NOT_RECORDED");
                initial.put("experimentStatus", "NOT_FINALIZED");
                initial.putArray("failures");
                write(initial);
            }
        } catch (IOException | RuntimeException exception) {
            try {
                close();
            } catch (IOException cleanup) {
                exception.addSuppressed(cleanup);
            }
            throw exception;
        }
    }

    /** Replaces a complete snapshot. Finalization must be explicitly supplied by its authorized owner. */
    public synchronized void write(ObjectNode metadata) throws IOException {
        if (closed) {
            throw new IOException("Run metadata writer is closed");
        }
        ObjectNode snapshot = Objects.requireNonNull(metadata, "metadata").deepCopy();
        validateRunId(snapshot);
        if (!snapshot.path("experimentStatus").isTextual()) {
            throw new IllegalArgumentException("experimentStatus must be explicitly supplied");
        }
        if (Files.exists(output, LinkOption.NOFOLLOW_LINKS)) {
            validateExisting();
        }
        byte[] bytes = JSON.writerWithDefaultPrettyPrinter().writeValueAsBytes(snapshot);
        Path temporary = Files.createTempFile(output.getParent(), ".run-config-", ".tmp");
        Throwable failure = null;
        try {
            Files.write(temporary, bytes);
            // Unsupported atomic moves propagate; never fall back to truncating the destination.
            replacement.move(temporary, output);
        } catch (IOException | RuntimeException | Error exception) {
            failure = exception;
            throw exception;
        } finally {
            try {
                Files.deleteIfExists(temporary);
            } catch (IOException cleanup) {
                if (failure == null) {
                    throw cleanup;
                }
                failure.addSuppressed(cleanup);
            }
        }
    }

    private void validateExisting() throws IOException {
        if (!Files.isRegularFile(output, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Metadata destination must be a regular file: " + output);
        }
        validateRunId(JSON.readTree(Files.readAllBytes(output)));
    }

    private void validateRunId(JsonNode metadata) throws IOException {
        if (metadata == null || !metadata.isObject() || !metadata.path("runId").isTextual()
                || !runId.equals(metadata.path("runId").textValue())) {
            throw new IOException("Metadata runId does not match owner " + runId);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;
            lockChannel.close(); // Closing the channel releases its lifetime lock.
        }
    }

    @FunctionalInterface
    interface Replacement {
        void move(Path source, Path target) throws IOException;
    }
}
