package com.kafkaiot.streams;

import com.kafkaiot.streams.config.StreamsConfig;
import com.kafkaiot.streams.topology.SensorTopology;
import org.apache.kafka.streams.KafkaStreams;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Standalone Kafka Streams entry point with explicit CLI configuration. */
public final class SensorStreamsApplication {
    private static final Set<String> OPTIONS = Set.of(
            "bootstrap-servers", "application-id", "rule", "temperature-threshold");
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

    private SensorStreamsApplication() {
    }

    public static void main(String[] args) {
        if (args.length == 1 && "--help".equals(args[0])) {
            System.out.println("""
                    Usage: SensorStreamsApplication --option value ...
                    Required:
                      --bootstrap-servers HOST:PORT[,HOST:PORT...]
                      --application-id ID
                      --rule HIGH_TEMPERATURE
                      --temperature-threshold DOUBLE (finite Celsius value)
                    Use --help alone to display this help without starting Kafka Streams.
                    """);
            return;
        }

        try {
            Map<String, String> options = parse(args);
            String rule = options.get("rule");
            if (!"HIGH_TEMPERATURE".equals(rule)) {
                throw new IllegalArgumentException(
                        "Unsupported --rule: " + rule + "; only HIGH_TEMPERATURE is supported");
            }
            double threshold;
            try {
                threshold = Double.parseDouble(options.get("temperature-threshold"));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException("--temperature-threshold must be a number", exception);
            }
            if (!Double.isFinite(threshold)) {
                throw new IllegalArgumentException("--temperature-threshold must be finite");
            }

            Properties properties = StreamsConfig.createProperties(
                    options.get("bootstrap-servers"), options.get("application-id"));
            KafkaStreams streams = new KafkaStreams(SensorTopology.build(threshold), properties);
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (!streams.close(CLOSE_TIMEOUT)) {
                        System.err.println("Kafka Streams shutdown did not complete within 30 seconds.");
                    }
                }, "sensor-streams-shutdown"));
                streams.start();
                System.out.println("Kafka Streams start requested for applicationId="
                        + options.get("application-id") + ", rule=" + rule
                        + ", temperatureThreshold=" + threshold);
            } catch (RuntimeException exception) {
                try {
                    if (!streams.close(CLOSE_TIMEOUT)) {
                        System.err.println("Kafka Streams cleanup did not complete within 30 seconds.");
                    }
                } catch (RuntimeException closeException) {
                    if (closeException != exception) {
                        exception.addSuppressed(closeException);
                    }
                }
                throw exception;
            }
        } catch (RuntimeException exception) {
            System.err.println("Kafka Streams startup failed: " + exception);
            throw exception;
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
        for (String required : OPTIONS) {
            if (!options.containsKey(required)) {
                throw new IllegalArgumentException("Missing required option --" + required + "; use --help");
            }
        }
        return options;
    }
}
