package com.kafkaiot.streams.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaiot.streams.model.SensorAlert;
import com.kafkaiot.streams.model.SensorEvent;
import com.kafkaiot.streams.model.SensorResult;
import com.kafkaiot.streams.rules.HighTemperatureRule;
import com.kafkaiot.streams.rules.HighFrequencyProcessor;
import com.kafkaiot.streams.rules.RapidChangeProcessor;
import com.kafkaiot.streams.state.StateStoreDefinitions;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/** Builds rule-specific topologies with separate result and alert outputs. */
public final class SensorTopology {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

        publishAlerts(results);
        return builder.build();
    }

    /** Builds stateful HIGH_FREQUENCY processing for events keyed by deviceId. */
    public static Topology buildHighFrequency(long windowDurationMs, int countThreshold) {
        if (windowDurationMs <= 0) {
            throw new IllegalArgumentException("windowDurationMs must be positive");
        }
        if (countThreshold <= 0) {
            throw new IllegalArgumentException("countThreshold must be positive");
        }

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(StateStoreDefinitions.highFrequencyStoreBuilder());
        KStream<String, String> input = builder.stream("sensor-events",
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> resultJson = input.process(
                () -> new HighFrequencyProcessor(windowDurationMs, countThreshold),
                StateStoreDefinitions.HIGH_FREQUENCY_STORE);
        resultJson.to("sensor-results", Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, SensorResult> results = resultJson.mapValues(json -> {
            try {
                return OBJECT_MAPPER.readValue(json, SensorResult.class);
            } catch (JsonProcessingException exception) {
                throw new SerializationException("Failed to deserialize SensorResult", exception);
            }
        });
        publishAlerts(results);
        return builder.build();
    }

    /** Builds stateful RAPID_CHANGE processing for events keyed by deviceId. */
    public static Topology buildRapidChange(long maxTimeDifferenceMs, double temperatureChangeThreshold) {
        if (maxTimeDifferenceMs <= 0) {
            throw new IllegalArgumentException("maxTimeDifferenceMs must be positive");
        }
        if (!Double.isFinite(temperatureChangeThreshold) || temperatureChangeThreshold <= 0) {
            throw new IllegalArgumentException("temperatureChangeThreshold must be finite and positive");
        }

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(StateStoreDefinitions.rapidChangeStoreBuilder());
        KStream<String, String> input = builder.stream("sensor-events",
                Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> resultJson = input.process(
                () -> new RapidChangeProcessor(maxTimeDifferenceMs, temperatureChangeThreshold),
                StateStoreDefinitions.RAPID_CHANGE_STORE);
        resultJson.to("sensor-results", Produced.with(Serdes.String(), Serdes.String()));

        KStream<String, SensorResult> results = resultJson.mapValues(json -> {
            try {
                return OBJECT_MAPPER.readValue(json, SensorResult.class);
            } catch (JsonProcessingException exception) {
                throw new SerializationException("Failed to deserialize SensorResult", exception);
            }
        });
        publishAlerts(results);
        return builder.build();
    }

    private static void publishAlerts(KStream<String, SensorResult> results) {
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

    }
}
