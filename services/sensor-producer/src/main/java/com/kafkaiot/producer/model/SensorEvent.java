package com.kafkaiot.producer.model;

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
}
