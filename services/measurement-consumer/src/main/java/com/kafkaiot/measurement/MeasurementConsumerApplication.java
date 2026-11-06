package com.kafkaiot.measurement;

import com.kafkaiot.measurement.config.ConsumerKafkaConfig;
import com.kafkaiot.measurement.consumer.MeasurementKafkaConsumer;
import com.kafkaiot.measurement.io.SensorResultCsvWriter;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Standalone Measurement Consumer entry point with explicit CLI configuration. */
public final class MeasurementConsumerApplication {
    private static final Set<String> OPTIONS = Set.of(
            "bootstrap-servers", "group-id", "auto-offset-reset", "result-csv-output");

    private MeasurementConsumerApplication() {
    }

    public static void main(String[] args) {
        if (args.length == 1 && "--help".equals(args[0])) {
            System.out.println("""
                    Usage: MeasurementConsumerApplication --option value ...
                    Required:
                      --bootstrap-servers HOST:PORT[,HOST:PORT...]
                      --group-id ID
                    Optional:
                      --auto-offset-reset earliest|latest (default: earliest)
                      --result-csv-output PATH (new file; e.g. experiments/results/<runId>/sensor-results.csv)
                    Use --help alone to display this help without starting the consumer.
                    """);
            return;
        }
        try {
            execute(parse(args));
        } catch (RuntimeException exception) {
            reportFailure(exception);
            throw exception;
        }
    }

    static void reportFailure(RuntimeException exception) {
        System.err.println("Measurement Consumer startup or execution failed: " + exception);
        if (hasCsvFailure(exception)) {
            System.err.println("Result CSV file-writing or cleanup failed; execution incomplete. "
                    + "Available observations are partial.");
        }
    }

    private static void execute(Map<String, String> options) {
        execute(options, MeasurementKafkaConsumer::new, new CountDownLatch(1));
    }

    @FunctionalInterface
    interface ConsumerFactory {
        MeasurementKafkaConsumer create(Properties properties, MeasurementAggregator aggregator,
                                        SensorResultCsvWriter writer);
    }

    // Allows lifecycle verification with an in-memory consumer and an observable completion latch.
    static void execute(Map<String, String> options, ConsumerFactory factory, CountDownLatch completed) {
        Properties properties = ConsumerKafkaConfig.createProperties(
                options.get("bootstrap-servers"), options.get("group-id"),
                options.getOrDefault("auto-offset-reset", "earliest"));
        MeasurementAggregator aggregator = new MeasurementAggregator();
        try {
            try {
                SensorResultCsvWriter writer = openResultWriter(options);
                try (Closeable cleanup = () -> closeResultWriter(writer)) {
                    MeasurementKafkaConsumer consumer = factory.create(properties, aggregator, writer);
                    runConsumer(consumer, completed);
                }
            } catch (IOException exception) {
                if (!hasCsvFailure(exception)) {
                    markCsvFailure(exception);
                }
                throw new UncheckedIOException("Result CSV creation or closure failed; execution incomplete", exception);
            }
            System.out.println("Measurement Consumer counts:");
            System.out.println("Total received results: " + aggregator.getReceivedResultCount());
            System.out.println("Unique received results: " + aggregator.getUniqueResultCount());
            System.out.println("Duplicate result deliveries: " + aggregator.getDuplicateResultCount());
            System.out.println("Received alerts: " + aggregator.getReceivedAlertCount());
        } finally {
            // Includes writer closure, even when construction, polling, or cleanup fails.
            completed.countDown();
        }
    }

    private static SensorResultCsvWriter openResultWriter(Map<String, String> options) throws IOException {
        if (!options.containsKey("result-csv-output")) {
            return null;
        }
        String value = options.get("result-csv-output");
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--result-csv-output requires a usable file path");
        }
        return new SensorResultCsvWriter(Path.of(value));
    }

    private static void closeResultWriter(SensorResultCsvWriter writer) throws IOException {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException exception) {
                markCsvFailure(exception);
                throw exception;
            }
        }
    }

    private static void markCsvFailure(IOException exception) {
        exception.addSuppressed(new CsvFailureMarker());
    }

    /** Marks I/O failures caught specifically at the owned CSV resource boundary. */
    private static final class CsvFailureMarker extends Exception {
    }

    private static boolean hasCsvFailure(Throwable exception) {
        if (exception instanceof CsvFailureMarker) {
            return true;
        }
        // D.6.14 has no dedicated exception type; verify its wrapper and origin.
        StackTraceElement[] origin = exception.getStackTrace();
        if (exception instanceof IllegalStateException && exception.getCause() instanceof IOException
                && exception.getMessage() != null
                && exception.getMessage().startsWith("Failed to write result CSV for topic ")
                && origin.length > 0
                && origin[0].getClassName().equals(MeasurementKafkaConsumer.class.getName())
                && origin[0].getMethodName().equals("handleRecord")) {
            return true;
        }
        if (exception.getCause() != null && hasCsvFailure(exception.getCause())) {
            return true;
        }
        for (Throwable suppressed : exception.getSuppressed()) {
            if (hasCsvFailure(suppressed)) {
                return true;
            }
        }
        return false;
    }

    private static void runConsumer(MeasurementKafkaConsumer consumer, CountDownLatch completed) {
        Thread shutdownHook = new Thread(() -> {
            try {
                consumer.shutdown();
            } catch (RuntimeException exception) {
                System.err.println("Measurement Consumer shutdown request failed: " + exception);
            }
            try {
                if (!completed.await(30, TimeUnit.SECONDS)) {
                    System.err.println("Measurement Consumer shutdown did not complete within 30 seconds.");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                System.err.println("Interrupted while waiting for Measurement Consumer shutdown.");
            }
        }, "measurement-consumer-shutdown");
        try {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        } catch (RuntimeException exception) {
            // run() owns closure even if hook registration fails before polling starts.
            try {
                consumer.shutdown();
                consumer.run();
            } catch (RuntimeException cleanupException) {
                exception.addSuppressed(cleanupException);
            }
            throw exception;
        }
        consumer.run();
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String option = args[i];
            if (!option.startsWith("--") || !OPTIONS.contains(option.substring(2))) {
                throw new IllegalArgumentException("Unknown option: " + option + "; use --help");
            }
            if (i + 1 >= args.length || args[i + 1].startsWith("--") || args[i + 1].isBlank()) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            if (options.putIfAbsent(option.substring(2), args[i + 1]) != null) {
                throw new IllegalArgumentException("Duplicate option: " + option);
            }
        }
        for (String required : new String[]{"bootstrap-servers", "group-id"}) {
            if (!options.containsKey(required)) {
                throw new IllegalArgumentException("Missing required option --" + required + "; use --help");
            }
        }
        String autoOffsetReset = options.getOrDefault("auto-offset-reset", "earliest");
        if (!"earliest".equals(autoOffsetReset) && !"latest".equals(autoOffsetReset)) {
            throw new IllegalArgumentException("--auto-offset-reset must be earliest or latest");
        }
        return options;
    }
}
