package com.kafkaiot.streams.config;

import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/** Provides the baseline Kafka Streams configuration. */
public final class StreamsConfig {
    private StreamsConfig() {
    }

    public static Properties createProperties(String bootstrapServers, String applicationId) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be null or blank");
        }
        if (applicationId == null || applicationId.isBlank()) {
            throw new IllegalArgumentException("applicationId must not be null or blank");
        }

        Properties properties = new Properties();
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG,
                applicationId);
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers);
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE);
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                "1");
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.StringSerde.class.getName());
        properties.setProperty(org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.StringSerde.class.getName());
        return properties;
    }
}
