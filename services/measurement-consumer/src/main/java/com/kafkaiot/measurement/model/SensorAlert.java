package com.kafkaiot.measurement.model;

/**
 * Kafka Streams alert identifying a rule-triggering input event.
 * eventTime and sentAt preserve the input event's Unix epoch milliseconds.
 */
public record SensorAlert(
        String eventId,
        String runId,
        String deviceId,
        long eventTime,
        long sentAt,
        String ruleType
) {
}
