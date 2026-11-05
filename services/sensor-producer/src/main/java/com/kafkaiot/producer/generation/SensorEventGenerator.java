package com.kafkaiot.producer.generation;

import com.kafkaiot.producer.model.SensorEvent;

import java.util.Locale;
import java.util.Objects;
import java.util.Random;

/**
 * Generates events for 100 devices in round-robin order.
 * Temperatures are uniformly distributed in [15.0, 35.0) degrees Celsius.
 * Event timestamps start at initialEventTime and advance by timestampIncrementMs
 * for every event, so each device advances by 100 increments between its events.
 * This generator is intended for sequential use by a single thread.
 */
public final class SensorEventGenerator {
    private static final int DEVICE_COUNT = 100;

    private final String runId;
    private final Random random;
    private final long initialEventTime;
    private final long timestampIncrementMs;
    private long sequence;

    /**
     * @param runId identifier of the run
     * @param randomSeed seed for reproducible temperature values
     * @param initialEventTime initial Unix epoch timestamp in milliseconds
     * @param timestampIncrementMs positive increment in milliseconds per event
     */
    public SensorEventGenerator(String runId, long randomSeed,
                                long initialEventTime, long timestampIncrementMs) {
        if (timestampIncrementMs <= 0) {
            throw new IllegalArgumentException("timestampIncrementMs must be positive");
        }
        this.runId = Objects.requireNonNull(runId, "runId");
        this.random = new Random(randomSeed);
        this.initialEventTime = initialEventTime;
        this.timestampIncrementMs = timestampIncrementMs;
    }

    public SensorEvent nextEvent() {
        long nextSequence = Math.incrementExact(sequence);
        long eventTime = Math.addExact(initialEventTime,
                Math.multiplyExact(sequence, timestampIncrementMs));
        String deviceId = String.format(Locale.ROOT, "device-%03d",
                (sequence % DEVICE_COUNT) + 1);
        double temperature = 15.0 + random.nextDouble() * 20.0;

        SensorEvent event = new SensorEvent(
                runId + "-" + nextSequence,
                runId,
                deviceId,
                temperature,
                eventTime,
                0L
        );
        sequence = nextSequence;
        return event;
    }
}
