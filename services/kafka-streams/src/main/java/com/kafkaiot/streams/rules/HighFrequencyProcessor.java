package com.kafkaiot.streams.rules;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.model.SensorResult;
import com.kafkaiot.streams.state.StateStoreDefinitions;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;

/** Counts events in epoch-aligned windows, assuming increasing eventTime per device. */
public final class HighFrequencyProcessor implements Processor<String, String, String, String> {
    private final long windowDurationMs;
    private final int countThreshold;
    private ProcessorContext<String, String> context;
    private KeyValueStore<String, String> store;

    public HighFrequencyProcessor(long windowDurationMs, int countThreshold) {
        if (windowDurationMs <= 0) {
            throw new IllegalArgumentException("windowDurationMs must be positive");
        }
        if (countThreshold <= 0) {
            throw new IllegalArgumentException("countThreshold must be positive");
        }
        this.windowDurationMs = windowDurationMs;
        this.countThreshold = countThreshold;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        this.context = Objects.requireNonNull(context, "context must not be null");
        store = Objects.requireNonNull(context.getStateStore(StateStoreDefinitions.HIGH_FREQUENCY_STORE),
                "HIGH_FREQUENCY state store must be connected");
    }

    @Override
    public void process(Record<String, String> record) {
        Objects.requireNonNull(record, "record must not be null");
        SensorEvent event;
        try {
            event = SensorEvent.fromJson(record.value());
        } catch (JsonProcessingException exception) {
            throw new SerializationException("Failed to deserialize SensorEvent", exception);
        }
        String runId = Objects.requireNonNull(event.runId(), "runId must not be null");
        String deviceId = Objects.requireNonNull(event.deviceId(), "deviceId must not be null");
        // The runId length makes the boundary unambiguous, even with delimiters in identifiers.
        String stateKey = runId.length() + ":" + runId + deviceId;
        long windowId = Math.floorDiv(event.eventTime(), windowDurationMs);
        long count = 1;
        String storedValue = store.get(stateKey);
        if (storedValue != null) {
            WindowCount previous = decodeState(storedValue);
            if (windowId < previous.windowId()) {
                throw new IllegalStateException("Event precedes the current HIGH_FREQUENCY window; "
                        + "increasing eventTime is required");
            }
            if (windowId == previous.windowId()) {
                count = Math.addExact(previous.count(), 1L);
            }
        }

        SensorResult result = new SensorResult(event.eventId(), event.runId(), event.deviceId(),
                event.eventTime(), event.sentAt(), "HIGH_FREQUENCY", count > countThreshold);
        String resultJson;
        try {
            resultJson = result.toJson();
        } catch (JsonProcessingException exception) {
            throw new SerializationException("Failed to serialize SensorResult", exception);
        }
        store.put(stateKey, windowId + ":" + count);
        context.forward(record.withValue(resultJson));
    }

    private static WindowCount decodeState(String value) {
        String[] fields = value.split(":", -1);
        if (fields.length != 2) {
            throw new IllegalStateException("Invalid HIGH_FREQUENCY state: expected windowId:count");
        }
        try {
            long windowId = Long.parseLong(fields[0]);
            long count = Long.parseLong(fields[1]);
            if (count <= 0) {
                throw new IllegalStateException("Invalid HIGH_FREQUENCY state: count must be positive");
            }
            return new WindowCount(windowId, count);
        } catch (NumberFormatException exception) {
            throw new IllegalStateException("Invalid HIGH_FREQUENCY state: expected long integers", exception);
        }
    }

    private record WindowCount(long windowId, long count) {
    }
}
