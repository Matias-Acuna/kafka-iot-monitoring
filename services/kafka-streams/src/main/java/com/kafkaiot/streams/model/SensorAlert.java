package com.kafkaiot.streams.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Alert for a rule-triggering input event, as approved in D.3.13.
 * eventId, runId, deviceId, eventTime, and sentAt retain the input event's values;
 * eventTime and sentAt are Unix epoch milliseconds.
 * ruleType identifies HIGH_TEMPERATURE, HIGH_FREQUENCY, or RAPID_CHANGE.
 */
public record SensorAlert(
        String eventId,
        String runId,
        String deviceId,
        long eventTime,
        long sentAt,
        String ruleType
) {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public String toJson() throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }
}
