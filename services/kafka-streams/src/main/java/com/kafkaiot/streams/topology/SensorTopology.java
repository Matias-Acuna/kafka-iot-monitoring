package com.kafkaiot.streams.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafkaiot.streams.model.SensorAlert;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.model.SensorResult;
import com.kafkaiot.streams.rules.HighTemperatureRule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/** Stateless HIGH_TEMPERATURE processing with separate result and alert outputs. */
public final class SensorTopology {
    private SensorTopology() {
    }

    public static Topology build(double thresholdCelsius) {
        if (!Double.isFinite(thresholdCelsius)) {
            throw new IllegalArgumentException("thresholdCelsius must be finite");
        }

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream("sensor-events",
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, SensorResult> results = input.mapValues(json -> {
            SensorEvent event;
            try {
                event = SensorEvent.fromJson(json);
            } catch (JsonProcessingException exception) {
                throw new SerializationException("Failed to deserialize SensorEvent", exception);
            }
            boolean triggered = HighTemperatureRule.isTriggered(event, thresholdCelsius);
            return new SensorResult(event.eventId(), event.runId(), event.deviceId(),
                    event.eventTime(), event.sentAt(), "HIGH_TEMPERATURE", triggered);
        });

        results.mapValues(result -> {
            try {
                return result.toJson();
            } catch (JsonProcessingException exception) {
                throw new SerializationException("Failed to serialize SensorResult", exception);
            }
        }).to("sensor-results", Produced.with(Serdes.String(), Serdes.String()));

        results.filter((key, result) -> result.triggered())
                .mapValues(result -> {
                    SensorAlert alert = new SensorAlert(result.eventId(), result.runId(),
                            result.deviceId(), result.eventTime(), result.sentAt(), result.ruleType());
                    try {
                        return alert.toJson();
                    } catch (JsonProcessingException exception) {
                        throw new SerializationException("Failed to serialize SensorAlert", exception);
                    }
                })
                .to("sensor-alerts", Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
