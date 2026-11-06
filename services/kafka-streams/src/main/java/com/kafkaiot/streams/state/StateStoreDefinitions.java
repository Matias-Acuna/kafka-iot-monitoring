package com.kafkaiot.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/** Defines persistent state stores for stateful processing rules. */
public final class StateStoreDefinitions {
    public static final String HIGH_FREQUENCY_STORE = "high-frequency-store";

    private StateStoreDefinitions() {
    }

    /** Stores fixed-window identifiers and counts per (runId, deviceId), encoded later. */
    public static StoreBuilder<KeyValueStore<String, String>> highFrequencyStoreBuilder() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(HIGH_FREQUENCY_STORE),
                Serdes.String(), Serdes.String());
    }
}
