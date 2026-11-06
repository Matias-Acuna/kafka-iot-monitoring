package com.kafkaiot.streams;

import com.kafkaiot.streams.config.StreamsConfig;
import com.kafkaiot.streams.topology.SensorTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Standalone Kafka Streams entry point with explicit CLI configuration. */
public final class SensorStreamsApplication {
    private static final Set<String> OPTIONS = Set.of(
            "bootstrap-servers", "application-id", "rule", "temperature-threshold",
            "window-duration-ms", "count-threshold", "max-time-difference-ms", "temperature-change-threshold");
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
                      --rule HIGH_TEMPERATURE|HIGH_FREQUENCY|RAPID_CHANGE
                    Required for HIGH_TEMPERATURE:
                      --temperature-threshold DOUBLE (finite Celsius value)
                    Required for HIGH_FREQUENCY:
                      --window-duration-ms LONG (positive milliseconds)
                      --count-threshold INTEGER (positive event count)
                    Required for RAPID_CHANGE:
                      --max-time-difference-ms LONG (positive milliseconds)
                      --temperature-change-threshold DOUBLE (finite and positive)
                    Options belonging to the other rule are rejected.
                    Use --help alone to display this help without starting Kafka Streams.
                    """);
            return;
        }

        try {
            Map<String, String> options = parse(args);
            String rule = options.get("rule");
            Topology topology;
            String ruleDetails;
            if ("HIGH_TEMPERATURE".equals(rule)) {
                double threshold;
                try {
                    threshold = Double.parseDouble(options.get("temperature-threshold"));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException("--temperature-threshold must be a number", exception);
                }
                if (!Double.isFinite(threshold)) {
                    throw new IllegalArgumentException("--temperature-threshold must be finite");
                }
                topology = SensorTopology.build(threshold);
                ruleDetails = ", temperatureThreshold=" + threshold;
            } else if ("HIGH_FREQUENCY".equals(rule)) {
                long windowDurationMs = positiveLong(options.get("window-duration-ms"), "window-duration-ms");
                int countThreshold = positiveInt(options.get("count-threshold"));
                topology = SensorTopology.buildHighFrequency(windowDurationMs, countThreshold);
                ruleDetails = ", windowDurationMs=" + windowDurationMs + ", countThreshold=" + countThreshold;
            } else if ("RAPID_CHANGE".equals(rule)) {
                long maxTimeDifferenceMs = positiveLong(options.get("max-time-difference-ms"),
                        "max-time-difference-ms");
                double temperatureChangeThreshold = positiveFiniteDouble(options.get("temperature-change-threshold"));
                topology = SensorTopology.buildRapidChange(maxTimeDifferenceMs, temperatureChangeThreshold);
                ruleDetails = ", maxTimeDifferenceMs=" + maxTimeDifferenceMs
                        + ", temperatureChangeThreshold=" + temperatureChangeThreshold;
            } else {
                throw new IllegalArgumentException("Unsupported --rule: " + rule);
            }

            Properties properties = StreamsConfig.createProperties(
                    options.get("bootstrap-servers"), options.get("application-id"));
            KafkaStreams streams = new KafkaStreams(topology, properties);
            try {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    if (!streams.close(CLOSE_TIMEOUT)) {
                        System.err.println("Kafka Streams shutdown did not complete within 30 seconds.");
                    }
                }, "sensor-streams-shutdown"));
                streams.start();
                System.out.println("Kafka Streams start requested for applicationId="
                        + options.get("application-id") + ", rule=" + rule
                        + ruleDetails);
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
        for (String required : new String[]{"bootstrap-servers", "application-id", "rule"}) {
            require(options, required);
        }
        switch (options.get("rule")) {
            case "HIGH_TEMPERATURE" -> {
                reject(options, "window-duration-ms");
                reject(options, "count-threshold");
                reject(options, "max-time-difference-ms");
                reject(options, "temperature-change-threshold");
                require(options, "temperature-threshold");
            }
            case "HIGH_FREQUENCY" -> {
                reject(options, "temperature-threshold");
                reject(options, "max-time-difference-ms");
                reject(options, "temperature-change-threshold");
                require(options, "window-duration-ms");
                require(options, "count-threshold");
            }
            case "RAPID_CHANGE" -> {
                reject(options, "temperature-threshold");
                reject(options, "window-duration-ms");
                reject(options, "count-threshold");
                require(options, "max-time-difference-ms");
                require(options, "temperature-change-threshold");
            }
            default -> throw new IllegalArgumentException("Unsupported --rule: " + options.get("rule")
                    + "; supported rules: HIGH_TEMPERATURE, HIGH_FREQUENCY, RAPID_CHANGE");
        }
        return options;
    }

    private static void require(Map<String, String> options, String name) {
        if (!options.containsKey(name)) {
            throw new IllegalArgumentException("Missing required option --" + name + "; use --help");
        }
    }

    private static void reject(Map<String, String> options, String name) {
        if (options.containsKey(name)) {
            throw new IllegalArgumentException("--" + name + " is not valid for --rule " + options.get("rule"));
        }
    }

    private static long positiveLong(String value, String option) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("--" + option + " must be a positive long", exception);
        }
        throw new IllegalArgumentException("--" + option + " must be a positive long");
    }

    private static double positiveFiniteDouble(String value) {
        try {
            double parsed = Double.parseDouble(value);
            if (Double.isFinite(parsed) && parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(
                    "--temperature-change-threshold must be finite and positive", exception);
        }
        throw new IllegalArgumentException("--temperature-change-threshold must be finite and positive");
    }

    private static int positiveInt(String value) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed > 0) {
                return parsed;
            }
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException("--count-threshold must be a positive int", exception);
        }
        throw new IllegalArgumentException("--count-threshold must be a positive int");
    }
}
