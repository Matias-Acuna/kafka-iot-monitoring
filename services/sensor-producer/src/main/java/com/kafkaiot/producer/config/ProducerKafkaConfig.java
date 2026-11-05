package com.kafkaiot.producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Provides the baseline producer settings and explicit B2 experiment overrides.
 */
public final class ProducerKafkaConfig {
    private ProducerKafkaConfig() {
    }

    public static Properties createProperties(String bootstrapServers) {
        return createProperties(bootstrapServers, "all", true);
    }

    /**
     * Overrides only acks and idempotence, preserving the other baseline settings.
     * The caller must choose compatible values for the experimental scenario.
     */
    public static Properties createProperties(String bootstrapServers,
                                              String acks,
                                              boolean enableIdempotence) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be null or blank");
        }

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, acks);
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                Boolean.toString(enableIdempotence));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "5");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        return properties;
    }
}
