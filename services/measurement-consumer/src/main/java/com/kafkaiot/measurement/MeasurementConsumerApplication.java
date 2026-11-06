package com.kafkaiot.measurement;

import com.kafkaiot.measurement.config.ConsumerKafkaConfig;
import com.kafkaiot.measurement.consumer.MeasurementKafkaConsumer;
import com.kafkaiot.measurement.metrics.MeasurementAggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/** Standalone Measurement Consumer entry point with explicit CLI configuration. */
public final class MeasurementConsumerApplication {
    private static final Set<String> OPTIONS = Set.of(
            "bootstrap-servers", "group-id", "auto-offset-reset");

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
                    Use --help alone to display this help without starting the consumer.
                    """);
            return;
        }
        try {
            execute(parse(args));
        } catch (RuntimeException exception) {
            System.err.println("Measurement Consumer startup or execution failed: " + exception);
            throw exception;
        }
    }

    private static void execute(Map<String, String> options) {
        Properties properties = ConsumerKafkaConfig.createProperties(
                options.get("bootstrap-servers"), options.get("group-id"),
                options.getOrDefault("auto-offset-reset", "earliest"));
        MeasurementAggregator aggregator = new MeasurementAggregator();
        MeasurementKafkaConsumer consumer = new MeasurementKafkaConsumer(properties, aggregator);
        CountDownLatch completed = new CountDownLatch(1);
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
        try {
            consumer.run();
            System.out.println("Measurement Consumer counts:");
            System.out.println("Total received results: " + aggregator.getReceivedResultCount());
            System.out.println("Unique received results: " + aggregator.getUniqueResultCount());
            System.out.println("Duplicate result deliveries: " + aggregator.getDuplicateResultCount());
            System.out.println("Received alerts: " + aggregator.getReceivedAlertCount());
        } finally {
            completed.countDown();
        }
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
