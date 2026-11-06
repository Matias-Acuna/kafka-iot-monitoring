package com.kafkaiot.measurement.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/** Provides the baseline Measurement Consumer configuration. */
public final class ConsumerKafkaConfig {
    private ConsumerKafkaConfig() {
    }

    public static Properties createProperties(String bootstrapServers,
                                              String groupId,
                                              String autoOffsetReset) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be null or blank");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be null or blank");
        }
        if (!"earliest".equals(autoOffsetReset) && !"latest".equals(autoOffsetReset)) {
            throw new IllegalArgumentException("autoOffsetReset must be earliest or latest");
        }

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        return properties;
    }
}
