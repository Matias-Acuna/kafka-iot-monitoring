package com.kafkaiot.streams;

import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.rules.HighTemperatureRule;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HighTemperatureRuleTest {
    @Test
    void temperaturesBelowThresholdDoNotTrigger() {
        assertFalse(HighTemperatureRule.isTriggered(event(25.0), 30.0));
        assertFalse(HighTemperatureRule.isTriggered(event(29.9), 30.0));
    }

    @Test
    void temperatureEqualToThresholdDoesNotTrigger() {
        assertFalse(HighTemperatureRule.isTriggered(event(30.0), 30.0));
    }

    @Test
    void temperaturesAboveThresholdTrigger() {
        assertTrue(HighTemperatureRule.isTriggered(event(30.1), 30.0));
        assertTrue(HighTemperatureRule.isTriggered(event(34.9), 30.0));
    }

    @Test
    void usesTheConfiguredThreshold() {
        assertFalse(HighTemperatureRule.isTriggered(event(19.9), 20.0));
        assertFalse(HighTemperatureRule.isTriggered(event(20.0), 20.0));
        assertTrue(HighTemperatureRule.isTriggered(event(20.1), 20.0));
        assertFalse(HighTemperatureRule.isTriggered(event(34.9), 35.0));
    }

    @Test
    void rejectsNullEvent() {
        assertThrows(NullPointerException.class,
                () -> HighTemperatureRule.isTriggered(null, 30.0));
    }

    @Test
    void rejectsNonFiniteThresholds() {
        SensorEvent event = event(25.0);
        for (double threshold : new double[]{Double.NaN, Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY}) {
            assertThrows(IllegalArgumentException.class,
                    () -> HighTemperatureRule.isTriggered(event, threshold),
                    "Threshold " + threshold);
        }
    }

    private static SensorEvent event(double temperature) {
        return new SensorEvent("event-1", "rule-test", "device-001",
                temperature, 1_700_000_000_000L, 0L);
    }
}
