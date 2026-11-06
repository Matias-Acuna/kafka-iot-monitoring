package com.kafkaiot.producer;

import com.kafkaiot.producer.config.ProducerKafkaConfig;
import com.kafkaiot.producer.generation.SensorEventGenerator;
import com.kafkaiot.producer.io.ProducerCsvWriter;
import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.metrics.ProducerMetrics;
import com.kafkaiot.producer.metrics.ProducerMetrics.PhaseSnapshot;
import com.kafkaiot.producer.service.SensorEventPublisher;
import com.kafkaiot.producer.service.WorkloadRunner;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Plain Java entry point for one configured producer experiment. */
public final class SensorProducerApplication {
    private static final Set<String> OPTIONS = Set.of(
            "bootstrap-servers", "run-id", "random-seed", "initial-event-timestamp",
            "timestamp-increment", "target-events-per-second", "warm-up-seconds",
            "measurement-seconds", "csv-output", "acks", "enable-idempotence", "event-csv-output");

    private SensorProducerApplication() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && "--help".equals(args[0])) {
            System.out.println("""
                    Usage: SensorProducerApplication --option value ...
                    Required:
                      --bootstrap-servers HOST:PORT[,HOST:PORT...]
                      --run-id ID
                      --random-seed LONG
                      --initial-event-timestamp LONG   (Unix epoch milliseconds)
                      --timestamp-increment LONG       (positive milliseconds per event)
                      --target-events-per-second DOUBLE (finite and positive)
                      --csv-output PATH
                    Optional:
                      --event-csv-output PATH     (new file; e.g. experiments/results/<runId>/producer-events.csv)
                      --warm-up-seconds LONG      (default from WorkloadRunner: 60)
                      --measurement-seconds LONG  (default from WorkloadRunner: 300)
                      --acks all|-1|0|1           (default from ProducerKafkaConfig)
                      --enable-idempotence true|false (default from ProducerKafkaConfig)
                    Existing CSV output is overwritten only after a complete, accounted run.
                    """);
            return;
        }
        try {
            execute(parse(args));
        } catch (Exception exception) {
            System.err.println("Producer execution failed: " + exception);
            throw exception;
        }
    }

    private static void execute(Map<String, String> options) throws Exception {
        execute(options, SensorEventPublisher::new);
    }

    @FunctionalInterface
    interface PublisherFactory {
        SensorEventPublisher create(String bootstrapServers, String acks, boolean enableIdempotence);
    }

    // Allows application lifecycle verification without creating a Kafka client.
    static void execute(Map<String, String> options, PublisherFactory publisherFactory) throws Exception {
        String bootstrap = required(options, "bootstrap-servers");
        String runId = required(options, "run-id");
        long seed = longValue(options, "random-seed");
        long initialTimestamp = longValue(options, "initial-event-timestamp");
        long increment = longValue(options, "timestamp-increment");
        double rate;
        try {
            rate = Double.parseDouble(required(options, "target-events-per-second"));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("--target-events-per-second must be a number", exception);
        }
        if (!Double.isFinite(rate) || rate <= 0) {
            throw new IllegalArgumentException("--target-events-per-second must be finite and positive");
        }
        if (increment <= 0) {
            throw new IllegalArgumentException("--timestamp-increment must be positive");
        }
        long warmUp = optionalLong(options, "warm-up-seconds", WorkloadRunner.DEFAULT_WARM_UP_SECONDS);
        long measurement = optionalLong(options, "measurement-seconds",
                WorkloadRunner.DEFAULT_MEASUREMENT_SECONDS);
        if (warmUp < 0 || measurement <= 0) {
            throw new IllegalArgumentException(
                    "--warm-up-seconds must be non-negative and --measurement-seconds positive");
        }
        try {
            Math.multiplyExact(Math.addExact(warmUp, measurement), 1_000_000_000L);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("Combined phase durations are too large", exception);
        }
        Path csvOutput = Path.of(required(options, "csv-output"));
        Properties baseline = ProducerKafkaConfig.createProperties(bootstrap);
        String acks = options.getOrDefault("acks", baseline.getProperty(ProducerConfig.ACKS_CONFIG));
        if (!Set.of("all", "-1", "0", "1").contains(acks)) {
            throw new IllegalArgumentException("--acks must be all, -1, 0 or 1");
        }
        String idempotence = options.getOrDefault("enable-idempotence",
                baseline.getProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        if (!"true".equals(idempotence) && !"false".equals(idempotence)) {
            throw new IllegalArgumentException("--enable-idempotence must be true or false");
        }
        boolean enableIdempotence = Boolean.parseBoolean(idempotence);
        if (enableIdempotence && !"all".equals(acks) && !"-1".equals(acks)) {
            throw new IllegalArgumentException("Idempotence requires --acks all or -1");
        }

        ProducerMetrics metrics = new ProducerMetrics(runId);
        SensorEventGenerator generator = new SensorEventGenerator(runId, seed, initialTimestamp, increment);
        Exception executionFailure = null;
        boolean interrupted = false;
        try (ProducerEventCsvWriter eventWriter = openEventWriter(options, csvOutput);
             SensorEventPublisher publisher = publisherFactory.create(bootstrap, acks, enableIdempotence)) {
            WorkloadRunner runner = new WorkloadRunner(generator, publisher, rate, warmUp, measurement);
            try {
                runner.run((phase, event) -> metrics.recordAttempt(phase),
                        (phase, event, metadata, exception) -> {
                            // WorkloadRunner guards each event's outcome against duplicates.
                            if (exception == null) {
                                metrics.recordConfirmation(phase);
                            } else {
                                metrics.recordFailure(phase, exception.toString());
                            }
                        }, eventWriter);
            } catch (InterruptedException exception) {
                interrupted = true;
                // Allow resource closure to await callbacks; restore status below.
                Thread.interrupted();
                throw exception;
            }
        } catch (Exception exception) {
            executionFailure = exception;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        // Successful producer close waits for pending sends and their callbacks.
        // A failed close prevents export even if counters happen to match.
        ProducerMetrics.Snapshot snapshot = metrics.snapshot();
        System.out.printf(Locale.ROOT, "runId=%s targetEventsPerSecond=%s%n", runId, rate);
        boolean consistent = report(snapshot.warmUp()) & report(snapshot.measurement());
        if (executionFailure != null) {
            System.err.println("No final CSV exported: workload, producer closure, or event CSV output failed.");
            if (options.containsKey("event-csv-output")) {
                System.err.println("Execution incomplete; any observations written for this execution are partial.");
            }
            throw executionFailure;
        }
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("No final CSV exported: execution was interrupted");
        }
        if (!consistent) {
            throw new IllegalStateException("No final CSV exported: unmatched publication accounting");
        }
        ProducerCsvWriter.write(csvOutput, snapshot);
        System.out.println("Final CSV exported: " + csvOutput);
        if (snapshot.warmUp().failed() != 0 || snapshot.measurement().failed() != 0) {
            throw new IllegalStateException(
                    "Workload completed with publication failures; final CSV contains those failures");
        }
    }

    private static boolean report(PhaseSnapshot phase) {
        long unmatched = phase.attempted() - phase.confirmed() - phase.failed();
        System.out.printf(Locale.ROOT, "%s attempted=%d confirmed=%d failed=%d unmatched=%d%n",
                phase.phase(), phase.attempted(), phase.confirmed(), phase.failed(), unmatched);
        return unmatched == 0;
    }

    static ProducerEventCsvWriter openEventWriter(Map<String, String> options, Path csvOutput) throws IOException {
        if (!options.containsKey("event-csv-output")) {
            return null;
        }
        String value = options.get("event-csv-output");
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("--event-csv-output requires a usable file path");
        }
        Path eventOutput = Path.of(value);
        if (resolvedOutputPath(eventOutput).equals(resolvedOutputPath(csvOutput))
                || (Files.exists(eventOutput) && Files.exists(csvOutput)
                && Files.isSameFile(eventOutput, csvOutput))) {
            throw new IllegalArgumentException("--event-csv-output and --csv-output must refer to different files");
        }
        return new ProducerEventCsvWriter(eventOutput);
    }

    // Resolve existing ancestors so aliases through symbolic-link directories are detected too.
    private static Path resolvedOutputPath(Path path) throws IOException {
        Path absolute = path.toAbsolutePath().normalize();
        Path ancestor = absolute;
        while (!Files.exists(ancestor)) {
            ancestor = ancestor.getParent();
        }
        return ancestor.toRealPath().resolve(ancestor.relativize(absolute)).normalize();
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            String argument = args[i];
            if (!argument.startsWith("--") || !OPTIONS.contains(argument.substring(2))) {
                throw new IllegalArgumentException("Unknown option: " + argument + "; use --help");
            }
            if (i + 1 >= args.length || args[i + 1].startsWith("--") || args[i + 1].isBlank()) {
                throw new IllegalArgumentException("Missing value for " + argument);
            }
            if (options.putIfAbsent(argument.substring(2), args[i + 1]) != null) {
                throw new IllegalArgumentException("Duplicate option: " + argument);
            }
        }
        return options;
    }

    private static String required(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null) {
            throw new IllegalArgumentException("Missing required option --" + name + "; use --help");
        }
        return value;
    }

    private static long longValue(Map<String, String> options, String name) {
        try {
            return Long.parseLong(required(options, name));
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("--" + name + " must be a 64-bit integer", exception);
        }
    }

    private static long optionalLong(Map<String, String> options, String name, long defaultValue) {
        return options.containsKey(name) ? longValue(options, name) : defaultValue;
    }
}
