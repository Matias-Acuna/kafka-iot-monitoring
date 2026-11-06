package com.kafkaiot.producer.io;

import com.kafkaiot.producer.model.SensorEvent;
import com.kafkaiot.producer.service.WorkloadRunner.Phase;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/** Incrementally writes publication observations; calls are serialized for callback safety. */
public final class ProducerEventCsvWriter implements Closeable {
    public enum Status {
        ATTEMPTED,
        CONFIRMED,
        FAILED
    }

    private final BufferedWriter writer;

    /** Creates a new UTF-8 CSV file, refusing to overwrite an existing path. */
    public ProducerEventCsvWriter(Path outputPath) throws IOException {
        Objects.requireNonNull(outputPath, "outputPath");
        Path parent = outputPath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try {
            writer.write("runId,eventId,deviceId,phase,recordedAt,sentAt,status,partition,offset,error\r\n");
        } catch (IOException exception) {
            try {
                writer.close();
            } catch (IOException closeException) {
                exception.addSuppressed(closeException);
            }
            throw exception;
        }
    }

    /**
     * recordedAt is the caller-captured wall-clock observation time in Unix milliseconds.
     * sentAt is copied unchanged from event. Unavailable metadata and errors are null.
     * I/O failures propagate; callers must finish callbacks before closing this writer.
     */
    public synchronized void writeObservation(SensorEvent event, Phase phase, long recordedAt,
                                               Status status, Integer partition, Long offset,
                                               String error) throws IOException {
        Objects.requireNonNull(event, "event");
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(status, "status");
        writer.write(String.join(",", escape(event.runId()), escape(event.eventId()),
                escape(event.deviceId()), phase.name(), Long.toString(recordedAt),
                Long.toString(event.sentAt()), status.name(),
                partition == null ? "" : partition.toString(),
                offset == null ? "" : offset.toString(), escape(error)) + "\r\n");
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
