package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.streams.model.SensorResult;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorResultTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void producesAValidJsonObject() throws JsonProcessingException {
        JsonNode json = OBJECT_MAPPER.readTree(result(true).toJson());

        assertTrue(json.isObject());
    }

    @Test
    void serializesAllSevenFieldsWithExpectedNamesValuesAndTypes() throws JsonProcessingException {
        JsonNode expected = OBJECT_MAPPER.readTree("""
                {"eventId":"run-test-1","runId":"run-test","deviceId":"device-001",
                 "eventTime":1700000000123,"sentAt":1700000000456,
                 "ruleType":"HIGH_TEMPERATURE","triggered":true}
                """);

        JsonNode actual = OBJECT_MAPPER.readTree(result(true).toJson());

        assertEquals(expected, actual);
    }

    @Test
    void serializesTriggeredAsBooleanTrue() throws JsonProcessingException {
        JsonNode triggered = OBJECT_MAPPER.readTree(result(true).toJson()).get("triggered");

        assertTrue(triggered.isBoolean());
        assertTrue(triggered.booleanValue());
    }

    @Test
    void serializesTriggeredAsBooleanFalse() throws JsonProcessingException {
        JsonNode triggered = OBJECT_MAPPER.readTree(result(false).toJson()).get("triggered");

        assertTrue(triggered.isBoolean());
        assertFalse(triggered.booleanValue());
    }

    @Test
    void preservesEscapedCharactersInAllStringFields() throws JsonProcessingException {
        SensorResult result = new SensorResult("event-\"quoted\"", "run-\\path\nline",
                "device-\t\r\b\fé", 1_700_000_000_123L, 1_700_000_000_456L,
                "rule-\"\\\n\t", true);

        JsonNode json = OBJECT_MAPPER.readTree(result.toJson());

        assertEquals(result.eventId(), json.get("eventId").textValue());
        assertEquals(result.runId(), json.get("runId").textValue());
        assertEquals(result.deviceId(), json.get("deviceId").textValue());
        assertEquals(result.ruleType(), json.get("ruleType").textValue());
    }

    @Test
    void introducesNoAdditionalJsonProperties() throws JsonProcessingException {
        JsonNode json = OBJECT_MAPPER.readTree(result(false).toJson());
        Set<String> names = new HashSet<>();
        json.fieldNames().forEachRemaining(names::add);

        assertEquals(Set.of("eventId", "runId", "deviceId", "eventTime", "sentAt",
                "ruleType", "triggered"), names);
        assertEquals(7, json.size());
    }

    private static SensorResult result(boolean triggered) {
        return new SensorResult("run-test-1", "run-test", "device-001",
                1_700_000_000_123L, 1_700_000_000_456L, "HIGH_TEMPERATURE", triggered);
    }
}
