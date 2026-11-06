package com.kafkaiot.measurement.model;

/**
 * Kafka Streams rule evaluation result consumed for measurement.
 * eventTime and sentAt preserve the input event's Unix epoch milliseconds.
 * The Measurement Consumer captures resultReceivedAt separately.
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
}
