package com.kafkaiot.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaiot.streams.model.SensorEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SensorEventTest {
    @Test
    void deserializesProducerJsonAndPreservesAllSixFields() throws JsonProcessingException {
        String json = """
                {"eventId":"run-test-1","runId":"run-test","deviceId":"device-001",
                 "temperature":29.9,"eventTime":1700000000123,"sentAt":1700000000456}
                """;

        SensorEvent event = SensorEvent.fromJson(json);

        assertAll(
                () -> assertEquals("run-test-1", event.eventId()),
                () -> assertEquals("run-test", event.runId()),
                () -> assertEquals("device-001", event.deviceId()),
                () -> assertEquals(29.9, event.temperature()),
                () -> assertEquals(1_700_000_000_123L, event.eventTime()),
                () -> assertEquals(1_700_000_000_456L, event.sentAt()));
    }

    @Test
    void interpretsEscapedCharactersInJsonStringFields() throws JsonProcessingException {
        String json = """
                {"eventId":"event-\\\"quoted\\\"","runId":"run-\\\\path\\nline",
                 "deviceId":"device-\\t\\r\\b\\f\\u00e9","temperature":25.0,
                 "eventTime":1700000000123,"sentAt":1700000000456}
                """;

        SensorEvent event = SensorEvent.fromJson(json);

        assertAll(
                () -> assertEquals("event-\"quoted\"", event.eventId()),
                () -> assertEquals("run-\\path\nline", event.runId()),
                () -> assertEquals("device-\t\r\b\fé", event.deviceId()));
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(JsonProcessingException.class,
                () -> SensorEvent.fromJson("{\"eventId\":\"unfinished\""));
    }

    @Test
    void rejectsNullJavaString() {
        assertThrows(NullPointerException.class, () -> SensorEvent.fromJson(null));
    }

    @Test
    void rejectsJsonNullLiteral() {
        assertThrows(IllegalArgumentException.class, () -> SensorEvent.fromJson("null"));
    }
}
