package com.kafkaiot.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kafkaiot.producer.config.ProducerKafkaConfig;
import com.kafkaiot.producer.generation.SensorEventGenerator;
import com.kafkaiot.producer.io.ProducerCsvWriter;
import com.kafkaiot.producer.io.ProducerEventCsvWriter;
import com.kafkaiot.producer.io.RunMetadataWriter;
import com.kafkaiot.producer.metrics.ProducerMetrics;
import com.kafkaiot.producer.metrics.ProducerMetrics.PhaseSnapshot;
import com.kafkaiot.producer.service.SensorEventPublisher;
import com.kafkaiot.producer.service.WorkloadRunner;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
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
            "measurement-seconds", "csv-output", "acks", "enable-idempotence", "event-csv-output", "run-config-output", "timing-stdout");

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
                      --timing-stdout true|false  (default: false; one workload timing JSON event)
                      --run-config-output PATH    (e.g. experiments/results/<runId>/run-config.json)
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
        String timingStdout = options.getOrDefault("timing-stdout", "false");
        if (!"true".equals(timingStdout) && !"false".equals(timingStdout)) {
            throw new IllegalArgumentException("--timing-stdout must be true or false");
        }
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
        try (Metadata metadata = openMetadata(options, csvOutput, runId, seed, initialTimestamp,
                increment, rate, warmUp, measurement,
                ProducerKafkaConfig.createProperties(bootstrap, acks, enableIdempotence))) {
            try {
                Exception executionFailure = null;
                boolean interrupted = false;
                try {
                    stage(metadata, "EVENT_CSV_OPEN");
                    ProducerEventCsvWriter eventWriter = openEventWriter(options, csvOutput);
                    try (Closeable eventCleanup = () -> {
                        if (eventWriter != null) {
                            try {
                                eventWriter.close();
                            } catch (IOException exception) {
                                if (metadata != null) {
                                    metadata.addFailure("EVENT_CSV_CLOSE", exception);
                                }
                                throw exception;
                            }
                            observe(metadata, "eventCsvClosedEpochMs", "EVENT_CSV_CLOSED");
                        }
                    }) {
                        stage(metadata, "PUBLISHER_STARTUP");
                        SensorEventPublisher publisher = publisherFactory.create(bootstrap, acks, enableIdempotence);
                        try (Closeable publisherCleanup = () -> {
                            try {
                                publisher.close();
                            } catch (RuntimeException exception) {
                                if (metadata != null) {
                                    metadata.addFailure("PUBLISHER_CLOSE", exception);
                                }
                                throw exception;
                            }
                            observe(metadata, "publicationsDrainedEpochMs", "PUBLICATIONS_DRAINED");
                        }) {
                            WorkloadRunner runner = new WorkloadRunner(generator, publisher, rate, warmUp, measurement);
                            Throwable workloadFailure = null;
                            stage(metadata, "WORKLOAD");
                            try {
                                runner.run((phase, event) -> metrics.recordAttempt(phase),
                                        (phase, event, recordMetadata, exception) -> {
                                            // WorkloadRunner guards each event's outcome against duplicates.
                                            if (exception == null) {
                                                metrics.recordConfirmation(phase);
                                            } else {
                                                metrics.recordFailure(phase, exception.toString());
                                            }
                                        }, eventWriter, "true".equals(timingStdout)
                                                ? timing -> printTiming(runId, timing) : null);
                            } catch (InterruptedException exception) {
                                interrupted = true;
                                // Allow resource closure to await callbacks; restore status below.
                                Thread.interrupted();
                                workloadFailure = exception;
                                throw exception;
                            } catch (RuntimeException | Error exception) {
                                workloadFailure = exception;
                                throw exception;
                            } finally {
                                if (metadata != null) {
                                    try {
                                        metadata.timing(runner.getTiming());
                                    } catch (IOException timingFailure) {
                                        if (workloadFailure == null) {
                                            throw timingFailure;
                                        }
                                        workloadFailure.addSuppressed(timingFailure);
                                    }
                                }
                            }
                        }
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
                if (metadata != null) {
                    for (PhaseSnapshot phase : new PhaseSnapshot[]{snapshot.warmUp(), snapshot.measurement()}) {
                        if (phase.failed() != 0) {
                            metadata.data.withArray("failures").addObject().put("category", "PUBLICATION")
                                    .put("phase", phase.phase().name()).put("count", phase.failed())
                                    .put("latestError", phase.latestError());
                        }
                    }
                }
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
                stage(metadata, "TOTALS_CSV");
                ProducerCsvWriter.write(csvOutput, snapshot);
                System.out.println("Final CSV exported: " + csvOutput);
                if (snapshot.warmUp().failed() != 0 || snapshot.measurement().failed() != 0) {
                    stage(metadata, "PUBLICATION_FAILURE");
                    throw new IllegalStateException(
                            "Workload completed with publication failures; final CSV contains those failures");
                }
                if (metadata != null) {
                    metadata.data.withObject("components").put("producer", "COMPLETED");
                    metadata.observe("producerCleanupCompletedEpochMs", "COMPLETED");
                }
            } catch (Exception | Error exception) {
                if (metadata != null) {
                    boolean restoreInterrupt = Thread.interrupted();
                    try {
                        metadata.failed(exception);
                    } catch (IOException recordingFailure) {
                        exception.addSuppressed(recordingFailure);
                    } finally {
                        if (restoreInterrupt) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
                throw exception;
            }
        }
    }

    public record TimingEvidence(String evidenceType, String runId, long workloadStartNs,
                                 long workloadStartEpochMs, long sampleSpanNs, long warmUpDurationNs,
                                 long measurementDurationNs, long measurementStartEpochMs,
                                 long measurementDeadlineEpochMs, String epochAlignment) { }

    static void printTiming(String runId, WorkloadRunner.Timing timing) {
        TimingEvidence evidence = new TimingEvidence("PRODUCER_WORKLOAD_TIMING", runId,
                timing.workloadStartNs(), timing.workloadStartEpochMs(), timing.sampleSpanNs(),
                timing.warmUpDurationNs(), timing.measurementDurationNs(), timing.measurementStartEpochMs(),
                timing.measurementDeadlineEpochMs(), "DERIVED_FROM_BRACKETED_CLOCK_SAMPLE");
        try {
            System.out.println(new ObjectMapper().writeValueAsString(evidence));
            if (System.out.checkError()) {
                throw new IllegalStateException("Producer timing evidence stdout could not be written");
            }
        } catch (com.fasterxml.jackson.core.JsonProcessingException exception) {
            throw new IllegalStateException("Producer timing evidence could not be serialized", exception);
        }
    }

    private static void stage(Metadata metadata, String stage) {
        if (metadata != null) {
            metadata.stage = stage;
        }
    }

    private static void observe(Metadata metadata, String name, String state) throws IOException {
        if (metadata != null) {
            metadata.observe(name, state);
        }
    }

    private static Metadata openMetadata(Map<String, String> options, Path csvOutput, String runId,
                                         long seed, long initialTimestamp, long increment, double rate,
                                         long warmUp, long measurement, Properties settings) throws IOException {
        if (!options.containsKey("run-config-output")) {
            return null;
        }
        String value = required(options, "run-config-output");
        if (value.isBlank()) {
            throw new IllegalArgumentException("--run-config-output requires a usable file path");
        }
        Path output = Path.of(value);
        Path lock = output.resolveSibling(output.getFileName() + ".lock");
        for (Path reserved : new Path[]{output, lock}) {
            rejectAlias(reserved, csvOutput);
            if (options.containsKey("event-csv-output")) {
                rejectAlias(reserved, Path.of(required(options, "event-csv-output")));
            }
        }
        try {
            return new Metadata(output, runId, options, seed, initialTimestamp, increment,
                    rate, warmUp, measurement, settings);
        } catch (IOException exception) {
            throw new MetadataFailure(exception);
        }
    }

    private static void rejectAlias(Path metadata, Path csv) throws IOException {
        if (resolvedOutputPath(metadata).equals(resolvedOutputPath(csv))
                || (Files.exists(metadata) && Files.exists(csv) && Files.isSameFile(metadata, csv))) {
            throw new IllegalArgumentException("Run metadata, its lock, and CSV outputs must be different files");
        }
    }

    /** Application-owned snapshots; never invoked from publication callbacks. */
    private static final class Metadata implements Closeable {
        private static final ObjectMapper JSON = new ObjectMapper();
        private final RunMetadataWriter writer;
        private final ObjectNode data;
        private String stage = "BEFORE_WORKLOAD";

        private Metadata(Path output, String runId, Map<String, String> options, long seed,
                         long initialTimestamp, long increment, double rate, long warmUp,
                         long measurement, Properties settings) throws IOException {
            writer = new RunMetadataWriter(output, runId);
            try {
                data = (ObjectNode) JSON.readTree(Files.readAllBytes(output));
                ObjectNode configuration = data.putObject("configuration");
                ObjectNode producer = configuration.putObject("producer");
                producer.put("runId", runId).put("randomSeed", seed)
                        .put("initialEventTimestamp", initialTimestamp).put("timestampIncrementMs", increment)
                        .put("targetEventsPerSecond", rate).put("csvOutput", options.get("csv-output"))
                        .put("eventCsvOutput", options.get("event-csv-output"))
                        .put("runConfigOutput", output.toString());
                ObjectNode kafka = producer.putObject("kafkaSettings");
                settings.stringPropertyNames().forEach(key -> kafka.put(key, settings.getProperty(key)));
                configuration.putNull("streams");
                configuration.putNull("consumer");
                data.putObject("phaseDurationsMs").put("warmUp", Math.multiplyExact(warmUp, 1000L))
                        .put("measurement", Math.multiplyExact(measurement, 1000L)).putNull("maximumDrain");
                data.putNull("clockAnchors");
                data.putNull("scheduled");
                data.putObject("observed").putNull("generationStoppedElapsedNs")
                        .putNull("generationStoppedEpochMs").putNull("publicationsDrainedEpochMs")
                        .putNull("eventCsvClosedEpochMs").putNull("producerCleanupCompletedEpochMs")
                        .putNull("drainEndedEpochMs");
                data.withObject("components").put("producer", "RUNNING");
                data.put("experimentStatus", "NOT_FINALIZED");
                observe("beforeWorkloadEpochMs", "BEFORE_WORKLOAD");
            } catch (IOException | RuntimeException exception) {
                try {
                    writer.close();
                } catch (IOException cleanup) {
                    exception.addSuppressed(cleanup);
                }
                throw exception;
            }
        }

        private void timing(WorkloadRunner.Timing timing) throws IOException {
            if (timing != null) {
                data.putObject("clockAnchors").put("workloadStartNs", timing.workloadStartNs())
                        .put("workloadStartEpochMs", timing.workloadStartEpochMs())
                        .put("sampleSpanNs", timing.sampleSpanNs());
                data.withObject("phaseDurationsMs").put("warmUpDurationNs", timing.warmUpDurationNs())
                        .put("measurementDurationNs", timing.measurementDurationNs());
                data.putObject("scheduled").put("measurementStartOffsetNs", timing.measurementStartOffsetNs())
                        .put("measurementDeadlineOffsetNs", timing.measurementDeadlineOffsetNs())
                        .put("measurementStartEpochMs", timing.measurementStartEpochMs())
                        .put("measurementDeadlineEpochMs", timing.measurementDeadlineEpochMs())
                        .put("epochAlignment", "DERIVED_FROM_BRACKETED_CLOCK_SAMPLE");
                data.withObject("observed").put("generationStoppedElapsedNs", timing.generationStoppedElapsedNs())
                        .put("generationStoppedEpochMs", timing.generationStoppedEpochMs());
            }
            observe("generationSnapshotRecordedEpochMs", "GENERATION_TERMINATED");
        }

        private void observe(String name, String state) throws IOException {
            data.withObject("observed").put(name, System.currentTimeMillis());
            data.withObject("components").put("producerLifecycle", state);
            persist();
        }

        private void failed(Throwable failure) throws IOException {
            data.withObject("components").put("producer",
                    failure instanceof InterruptedException ? "INTERRUPTED" : "FAILED");
            addFailure(stage, failure);
            observe("failureObservedEpochMs", "INCOMPLETE");
        }

        private void addFailure(String stage, Throwable failure) {
            ObjectNode entry = data.withArray("failures").addObject();
            entry.put("stage", stage).put("type", failure.getClass().getName()).put("message", failure.getMessage());
            entry.put("category", failure instanceof MetadataFailure ? "METADATA"
                    : failure instanceof InterruptedException ? "INTERRUPTION"
                    : failure instanceof UncheckedIOException
                    && "PUBLISHER_CLOSE".equals(stage)
                    && failure.getMessage() != null
                    && failure.getMessage().startsWith("Producer observation CSV write failed")
                    ? "EVENT_CSV" : stage);
            for (Throwable suppressed : failure.getSuppressed()) {
                entry.withArray("suppressed").add(suppressed.toString());
            }
        }

        private void persist() throws IOException {
            try {
                writer.write(data);
            } catch (IOException exception) {
                throw new MetadataFailure(exception);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }

    private static final class MetadataFailure extends IOException {
        private MetadataFailure(IOException cause) {
            super("Run metadata persistence failed; execution incomplete", cause);
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
