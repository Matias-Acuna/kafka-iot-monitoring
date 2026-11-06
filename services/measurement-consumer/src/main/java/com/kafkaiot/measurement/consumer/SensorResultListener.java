package com.kafkaiot.measurement.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.measurement.model.SensorResult;

/** Deserializes Kafka Streams result messages for measurement. */
public final class SensorResultListener {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Returns the result or throws if the JSON cannot be deserialized. */
    public SensorResult onMessage(String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, SensorResult.class);
    }
}
