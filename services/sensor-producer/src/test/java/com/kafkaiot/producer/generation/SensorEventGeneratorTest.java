package com.kafkaiot.producer.generation;

import com.kafkaiot.producer.model.SensorEvent;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SensorEventGeneratorTest {
    private static final String RUN_ID = "generator-test";
    private static final long SEED = 42L;
    private static final long INITIAL_EVENT_TIME = 1_700_000_000_000L;
    private static final long INCREMENT_MS = 25L;

    @Test
    void identicalInputsProduceIdenticalEventSequences() {
        SensorEventGenerator first = newGenerator();
        SensorEventGenerator second = newGenerator();

        for (int i = 0; i < 1_000; i++) {
            assertEquals(first.nextEvent(), second.nextEvent(), "Event index " + i);
        }
    }

    @Test
    void devicesRepeatInRoundRobinOrderEveryHundredEvents() {
        SensorEventGenerator generator = newGenerator();

        for (int i = 0; i < 301; i++) {
            String expectedDevice = String.format(Locale.ROOT, "device-%03d", i % 100 + 1);
            assertEquals(expectedDevice, generator.nextEvent().deviceId(), "Event index " + i);
        }
    }

    @Test
    void temperaturesStayWithinTheSpecifiedRange() {
        SensorEventGenerator generator = newGenerator();

        for (int i = 0; i < 10_000; i++) {
            double temperature = generator.nextEvent().temperature();
            assertTrue(temperature >= 15.0 && temperature < 35.0,
                    "Out-of-range temperature at event index " + i + ": " + temperature);
        }
    }

    @Test
    void timestampsStartAtInitialTimeAndAdvanceByTheConfiguredIncrement() {
        SensorEventGenerator generator = newGenerator();
        long previous = generator.nextEvent().eventTime();
        assertEquals(INITIAL_EVENT_TIME, previous);

        for (int i = 1; i < 250; i++) {
            long current = generator.nextEvent().eventTime();
            assertEquals(INCREMENT_MS, current - previous, "Event index " + i);
            previous = current;
        }
    }

    @Test
    void eventsHaveSequentialIdsTheConfiguredRunIdAndZeroSentAt() {
        SensorEventGenerator generator = newGenerator();

        for (int sequence = 1; sequence <= 250; sequence++) {
            SensorEvent event = generator.nextEvent();
            String expectedId = RUN_ID + "-" + sequence;
            assertAll(
                    () -> assertEquals(expectedId, event.eventId()),
                    () -> assertEquals(RUN_ID, event.runId()),
                    () -> assertEquals(0L, event.sentAt()));
        }
    }

    @Test
    void zeroAndNegativeTimestampIncrementsAreRejected() {
        for (long increment : new long[]{0L, -1L, Long.MIN_VALUE}) {
            assertThrows(IllegalArgumentException.class,
                    () -> new SensorEventGenerator(RUN_ID, SEED, INITIAL_EVENT_TIME, increment),
                    "Timestamp increment " + increment);
        }
    }

    private static SensorEventGenerator newGenerator() {
        return new SensorEventGenerator(RUN_ID, SEED, INITIAL_EVENT_TIME, INCREMENT_MS);
    }
}
