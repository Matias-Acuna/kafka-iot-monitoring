package com.kafkaiot.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamsConfigTest {
    @Test
    void preservesDifferentInputsAndAppliesTheSameBaseline() {
        String[][] inputs = {
                {"broker-a:9092", "streams-test-a"},
                {"broker-b:19092,broker-c:29092", "streams-test-b"}
        };

        for (String[] input : inputs) {
            Properties properties = com.kafkaiot.streams.config.StreamsConfig
                    .createProperties(input[0], input[1]);

            assertAll(
                    () -> assertEquals(input[0],
                            properties.getProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG)),
                    () -> assertEquals(input[1],
                            properties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG)),
                    () -> assertEquals(StreamsConfig.AT_LEAST_ONCE,
                            properties.getProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG)),
                    () -> assertEquals("1",
                            properties.getProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG)),
                    () -> assertEquals(Serdes.StringSerde.class.getName(),
                            properties.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG)),
                    () -> assertEquals(Serdes.StringSerde.class.getName(),
                            properties.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG)));
        }
    }

    @Test
    void rejectsNullEmptyAndBlankBootstrapServers() {
        for (String bootstrapServers : new String[]{null, "", " ", "\t\r\n"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> com.kafkaiot.streams.config.StreamsConfig
                            .createProperties(bootstrapServers, "streams-test"));
        }
    }

    @Test
    void rejectsNullEmptyAndBlankApplicationIds() {
        for (String applicationId : new String[]{null, "", " ", "\t\r\n"}) {
            assertThrows(IllegalArgumentException.class,
                    () -> com.kafkaiot.streams.config.StreamsConfig
                            .createProperties("broker-a:9092", applicationId));
        }
    }
}
