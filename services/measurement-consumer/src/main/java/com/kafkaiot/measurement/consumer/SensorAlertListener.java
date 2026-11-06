package com.kafkaiot.measurement.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.measurement.model.SensorAlert;

/** Deserializes Kafka Streams alert messages for measurement. */
public final class SensorAlertListener {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** Returns the alert or throws if the JSON cannot be deserialized. */
    public SensorAlert onMessage(String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, SensorAlert.class);
    }
}
