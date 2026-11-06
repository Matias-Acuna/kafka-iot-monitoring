package com.kafkaiot.streams.state;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

/** Defines persistent state stores for stateful processing rules. */
public final class StateStoreDefinitions {
    public static final String HIGH_FREQUENCY_STORE = "high-frequency-store";
    public static final String RAPID_CHANGE_STORE = "rapid-change-store";

    private StateStoreDefinitions() {
    }

    /** Stores fixed-window identifiers and counts per (runId, deviceId), encoded later. */
    public static StoreBuilder<KeyValueStore<String, String>> highFrequencyStoreBuilder() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(HIGH_FREQUENCY_STORE),
                Serdes.String(), Serdes.String());
    }

    /** Stores the previous eventTime and temperature per (runId, deviceId). */
    public static StoreBuilder<KeyValueStore<String, String>> rapidChangeStoreBuilder() {
        return Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(RAPID_CHANGE_STORE),
                Serdes.String(), Serdes.String());
    }
}
