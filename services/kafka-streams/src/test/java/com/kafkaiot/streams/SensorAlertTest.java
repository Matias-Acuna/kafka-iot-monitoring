package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.streams.model.SensorAlert;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class SensorAlertTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void serializesExactlyTheSixApprovedFieldsWithExpectedValuesAndTypes()
            throws JsonProcessingException {
        SensorAlert alert = new SensorAlert("run-test-1", "run-test", "device-001",
                1_700_000_000_123L, 1_700_000_000_456L, "HIGH_TEMPERATURE");
        JsonNode expected = OBJECT_MAPPER.readTree("""
                {"eventId":"run-test-1","runId":"run-test","deviceId":"device-001",
                 "eventTime":1700000000123,"sentAt":1700000000456,
                 "ruleType":"HIGH_TEMPERATURE"}
                """);

        JsonNode actual = OBJECT_MAPPER.readTree(alert.toJson());

        // Tree equality checks object structure, names, types, numeric values,
        // and absence of extra properties, independently of property order.
        assertEquals(expected, actual);
    }

    @Test
    void preservesEscapedCharactersInAllStringFields() throws JsonProcessingException {
        SensorAlert alert = new SensorAlert("event-\"quoted\"", "run-\\path\nline",
                "device-\t\r\b\fé", 1_700_000_000_123L, 1_700_000_000_456L,
                "HIGH_TEMPERATURE-\"\\\n\t");

        JsonNode json = OBJECT_MAPPER.readTree(alert.toJson());

        assertAll(
                () -> assertEquals(alert.eventId(), json.get("eventId").textValue()),
                () -> assertEquals(alert.runId(), json.get("runId").textValue()),
                () -> assertEquals(alert.deviceId(), json.get("deviceId").textValue()),
                () -> assertEquals(alert.ruleType(), json.get("ruleType").textValue()));
    }
}
