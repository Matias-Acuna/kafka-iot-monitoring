package com.kafkaiot.measurement;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.measurement.config.ConsumerKafkaConfig;
import com.kafkaiot.measurement.consumer.MeasurementKafkaConsumer;
import com.kafkaiot.measurement.dashboard.DashboardController;
import com.kafkaiot.measurement.io.SensorResultCsvWriter;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Standalone Measurement Consumer entry point with explicit CLI configuration. */
public final class MeasurementConsumerApplication {
    private static final Set<String> OPTIONS = Set.of(
            "bootstrap-servers", "group-id", "auto-offset-reset", "result-csv-output", "readiness-stdout", "run-id", "dashboard-port", "dashboard-host");

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
                      --run-id ID (optional; record observations only for this run)
                      --dashboard-port PORT (1..65535; enables the optional HTTP dashboard)
                      --dashboard-host HOST (default: 127.0.0.1; 0.0.0.0 binds all IPv4 interfaces)
                      --readiness-stdout true|false (default: false; Consumer lifecycle JSON evidence)
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
        ConsumerFactory factory = (properties, aggregator, writer) -> new MeasurementKafkaConsumer(
                properties, aggregator, writer, null, null, options.get("run-id"));
        if ("true".equals(options.get("readiness-stdout"))) {
            factory = (properties, aggregator, writer) -> new MeasurementKafkaConsumer(
                    properties, aggregator, writer, MeasurementConsumerApplication::printReadiness,
                    MeasurementConsumerApplication::printReadiness, options.get("run-id"));
        }
        execute(options, factory, new CountDownLatch(1));
    }

    static void printReadiness(Object evidence) {
        try {
            System.out.println(new ObjectMapper().writeValueAsString(evidence));
            if (System.out.checkError()) {
                throw new IllegalStateException("Consumer evidence stdout could not be written");
            }
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Consumer evidence could not be serialized", exception);
        }
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
        try (DashboardController dashboard = openDashboard(options, aggregator)) {
            emitConfiguration(properties, options);
            try {
                SensorResultCsvWriter writer = openResultWriter(options);
                Throwable[] processingFailure = {null};
                try (Closeable cleanup = () -> closeResultWriterWithEvidence(writer, options, processingFailure[0])) {
                    try {
                        MeasurementKafkaConsumer consumer = factory.create(properties, aggregator, writer);
                        runConsumer(consumer, completed);
                        if (options.containsKey("run-id")) {
                            System.out.println("Ignored observations from other runs: results="
                                    + consumer.getIgnoredForeignResults() + ", alerts=" + consumer.getIgnoredForeignAlerts());
                        }
                    } catch (RuntimeException | Error exception) {
                        processingFailure[0] = exception;
                        throw exception;
                    }
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

    private static DashboardController openDashboard(Map<String, String> options, MeasurementAggregator aggregator) {
        if (!options.containsKey("dashboard-port")) return null;
        try {
            DashboardController dashboard = new DashboardController(options.getOrDefault("dashboard-host", "127.0.0.1"),
                    dashboardPort(options.get("dashboard-port")), aggregator);
            try {
                dashboard.start();
                return dashboard;
            } catch (RuntimeException | Error failure) {
                dashboard.close();
                throw failure;
            }
        } catch (IOException failure) {
            // HTTP startup I/O is deliberately outside the CSV failure boundary.
            throw new UncheckedIOException("Dashboard HTTP startup failed", failure);
        }
    }

    private static int dashboardPort(String value) {
        try {
            int port = Integer.parseInt(value);
            if (port > 0 && port <= 65535) return port;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("--dashboard-port must be an integer from 1 to 65535", failure);
        }
        throw new IllegalArgumentException("--dashboard-port must be an integer from 1 to 65535");
    }

    record ConfigurationEvidence(String evidenceType, long observedAtEpochMs, String groupId,
                                 List<String> subscribedTopics, Map<String, Object> appliedConfiguration,
                                 boolean resultCsvEnabled, String resultCsvOutput, String runIdFilter,
                                 List<String> unavailable) { }

    static void emitConfiguration(Properties properties, Map<String, String> options) {
        if ("true".equals(options.get("readiness-stdout"))) {
            printReadiness(configurationEvidence(properties, options));
        }
    }

    /** Snapshot of the properties supplied to the Consumer, including Kafka-resolved defaults. */
    static ConfigurationEvidence configurationEvidence(Properties properties, Map<String, String> options) {
        var config = new ConfigurationView(properties);
        Map<String, Object> applied = new LinkedHashMap<>();
        // Credentials and arbitrary properties must never enter stdout evidence.
        for (String key : List.of("group.id", "bootstrap.servers", "auto.offset.reset",
                "key.deserializer", "value.deserializer", "enable.auto.commit", "auto.commit.interval.ms",
                "max.poll.records", "max.poll.interval.ms", "session.timeout.ms", "heartbeat.interval.ms",
                "group.protocol", "partition.assignment.strategy", "isolation.level", "fetch.min.bytes",
                "fetch.max.bytes", "fetch.max.wait.ms", "max.partition.fetch.bytes", "request.timeout.ms",
                "default.api.timeout.ms", "allow.auto.create.topics", "security.protocol")) {
            Object value = config.values().get(key);
            applied.put(key, value instanceof Class<?> type ? type.getName() : value);
        }
        String output = options.get("result-csv-output");
        if (output != null) {
            if (output.isBlank()) throw new IllegalArgumentException("--result-csv-output requires a usable file path");
            output = Path.of(output).toAbsolutePath().normalize().toString();
        }
        return new ConfigurationEvidence("MEASUREMENT_CONSUMER_CONFIGURATION", System.currentTimeMillis(),
                config.getString("group.id"), List.of("sensor-results", "sensor-alerts"), applied,
                output != null, output, options.get("run-id"), List.of("runtimeGeneratedClientId", "brokerEffectiveTopicConfiguration"));
    }

    private static final class ConfigurationView extends org.apache.kafka.clients.consumer.ConsumerConfig {
        private ConfigurationView(Properties properties) {
            super(properties, false);
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

    public record CsvClosureEvidence(String evidenceType, long observedAtEpochMs, String groupId,
                                     String outputPath, String status,
                                     MeasurementKafkaConsumer.FailureDetail failure) { }

    private static void closeResultWriterWithEvidence(SensorResultCsvWriter writer,
                                                      Map<String, String> options,
                                                      Throwable processingFailure) throws IOException {
        if (writer == null) return;
        try {
            closeResultWriter(writer);
        } catch (IOException | RuntimeException | Error closeFailure) {
            try {
                emitCsvClosure(options, "FAILED", closeFailure);
            } catch (RuntimeException | Error evidenceFailure) {
                if (closeFailure != evidenceFailure) closeFailure.addSuppressed(evidenceFailure);
            }
            throw closeFailure;
        }
        // A successful close cannot erase an earlier CSV write failure.
        boolean csvWriteFailed = processingFailure != null && hasCsvFailure(processingFailure);
        emitCsvClosure(options, csvWriteFailed ? "CLOSED_WITH_WRITE_FAILURE" : "CLOSED",
                csvWriteFailed ? processingFailure : null);
    }

    private static void emitCsvClosure(Map<String, String> options, String status, Throwable failure) {
        if ("true".equals(options.get("readiness-stdout"))) {
            printReadiness(new CsvClosureEvidence("MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED",
                    System.currentTimeMillis(), options.get("group-id"), options.get("result-csv-output"),
                    status, MeasurementKafkaConsumer.FailureDetail.from(failure)));
        }
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
        // CSV write failures can be wrapped in IllegalStateException; verify their cause and origin.
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
        String readiness = options.getOrDefault("readiness-stdout", "false");
        if (!"true".equals(readiness) && !"false".equals(readiness)) {
            throw new IllegalArgumentException("--readiness-stdout must be true or false");
        }
        String autoOffsetReset = options.getOrDefault("auto-offset-reset", "earliest");
        if (!"earliest".equals(autoOffsetReset) && !"latest".equals(autoOffsetReset)) {
            throw new IllegalArgumentException("--auto-offset-reset must be earliest or latest");
        }
        if (options.containsKey("dashboard-port")) dashboardPort(options.get("dashboard-port"));
        if (options.containsKey("dashboard-host")) DashboardController.validateHost(options.get("dashboard-host"));
        return options;
    }
}
