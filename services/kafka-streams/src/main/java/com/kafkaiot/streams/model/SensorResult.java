package com.kafkaiot.streams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Result of evaluating the active rule for an input event.
 * eventTime and sentAt retain the input event's Unix epoch millisecond values.
 * ruleType identifies HIGH_TEMPERATURE, HIGH_FREQUENCY, or RAPID_CHANGE;
 * triggered indicates whether that rule was triggered for the input event.
 */
public record SensorResult(
        String eventId,
        String runId,
        String deviceId,
        long eventTime,
        long sentAt,
        String ruleType,
        boolean triggered
) {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public String toJson() throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }
}
