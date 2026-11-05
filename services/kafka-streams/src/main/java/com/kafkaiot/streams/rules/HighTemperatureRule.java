package com.kafkaiot.streams.rules;

import com.kafkaiot.streams.model.SensorEvent;

import java.util.Objects;

/** Stateless evaluation of the HIGH_TEMPERATURE rule. */
public final class HighTemperatureRule {
    private HighTemperatureRule() {
    }

    public static boolean isTriggered(SensorEvent event, double thresholdCelsius) {
        Objects.requireNonNull(event, "event must not be null");
        if (!Double.isFinite(thresholdCelsius)) {
            throw new IllegalArgumentException("thresholdCelsius must be finite");
        }
        return event.temperature() > thresholdCelsius;
    }
}
