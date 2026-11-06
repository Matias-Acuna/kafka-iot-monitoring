package com.kafkaiot.measurement.io;

import com.kafkaiot.measurement.metrics.MeasurementAggregator.ReceivedResult;
import com.kafkaiot.measurement.model.SensorResult;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Incrementally persists existing result observations without changing measurement semantics. */
public final class SensorResultCsvWriter implements Closeable {
    private final BufferedWriter writer;

    /** Creates a new UTF-8 CSV file and refuses to overwrite an existing destination. */
    public SensorResultCsvWriter(Path outputPath) throws IOException {
        Objects.requireNonNull(outputPath, "outputPath");
        Path parent = outputPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try {
            writer.write("runId,eventId,deviceId,eventTime,sentAt,ruleType,triggered,"
                    + "resultReceivedAt,latencyMs,duplicate,negativeLatency\r\n");
        } catch (IOException exception) {
            try {
                writer.close();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    /** Writes one delivery, preserving its recorded timestamp, latency, and duplicate flag. */
    public synchronized void writeObservation(ReceivedResult observation) throws IOException {
        Objects.requireNonNull(observation, "observation");
        SensorResult result = Objects.requireNonNull(observation.result(), "result");
        writer.write(String.join(",", escape(result.runId()), escape(result.eventId()),
                escape(result.deviceId()), Long.toString(result.eventTime()), Long.toString(result.sentAt()),
                escape(result.ruleType()), Boolean.toString(result.triggered()),
                Long.toString(observation.resultReceivedAt()), Long.toString(observation.latencyMs()),
                Boolean.toString(observation.duplicate()), Boolean.toString(observation.latencyMs() < 0))
                + "\r\n");
    }

    public synchronized void flush() throws IOException {
        writer.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
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
