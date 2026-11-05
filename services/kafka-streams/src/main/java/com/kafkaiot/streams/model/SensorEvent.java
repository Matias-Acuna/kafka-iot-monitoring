package com.kafkaiot.streams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Objects;

/**
 * A sensor event with eventTime and sentAt expressed as Unix epoch milliseconds.
 */
public record SensorEvent(
        String eventId,
        String runId,
        String deviceId,
        double temperature,
        long eventTime,
        long sentAt
) {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * @throws NullPointerException if json is null
     * @throws IllegalArgumentException if json represents a JSON null value
     * @throws JsonProcessingException if JSON parsing or record mapping fails
     */
    public static SensorEvent fromJson(String json) throws JsonProcessingException {
        Objects.requireNonNull(json, "json must not be null");
        SensorEvent event = OBJECT_MAPPER.readValue(json, SensorEvent.class);
        if (event == null) {
            throw new IllegalArgumentException("JSON null is not a sensor event");
        }
        return event;
    }
}
