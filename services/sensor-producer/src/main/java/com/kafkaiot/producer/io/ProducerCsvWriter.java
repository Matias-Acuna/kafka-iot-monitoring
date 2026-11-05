package com.kafkaiot.producer.io;

import com.kafkaiot.producer.metrics.ProducerMetrics.PhaseSnapshot;
import com.kafkaiot.producer.metrics.ProducerMetrics.Snapshot;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Writes a supplied producer metrics snapshot without waiting for Kafka callbacks.
 */
public final class ProducerCsvWriter {
    private ProducerCsvWriter() {
    }

    /**
     * Creates or overwrites the output with a header and the two phase rows.
     * The caller is responsible for capturing the snapshot after completion.
     */
    public static void write(Path outputPath, Snapshot snapshot) throws IOException {
        Objects.requireNonNull(outputPath, "outputPath");
        Objects.requireNonNull(snapshot, "snapshot");

        Path parent = outputPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath,
                StandardCharsets.UTF_8, StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            writer.write("runId,phase,attempted,confirmed,failed,latestError\r\n");
            writeRow(writer, snapshot.runId(), snapshot.warmUp());
            writeRow(writer, snapshot.runId(), snapshot.measurement());
        }
    }

    private static void writeRow(BufferedWriter writer, String runId,
                                 PhaseSnapshot phase) throws IOException {
        writer.write(escape(runId));
        writer.write(',');
        writer.write(escape(phase.phase().name()));
        writer.write(',');
        writer.write(Long.toString(phase.attempted()));
        writer.write(',');
        writer.write(Long.toString(phase.confirmed()));
        writer.write(',');
        writer.write(Long.toString(phase.failed()));
        writer.write(',');
        writer.write(escape(phase.latestError()));
        writer.write("\r\n");
    }

    private static String escape(String value) {
        if (value == null) {
            return "";
        }
        if (value.indexOf(',') >= 0 || value.indexOf('"') >= 0
                || value.indexOf('\r') >= 0 || value.indexOf('\n') >= 0) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
