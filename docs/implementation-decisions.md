# Implementation Decisions

This document records the technical choices behind the IoT monitoring implementation. It focuses on configuration, event contracts, measurement rules, and experimental execution. The experimental methodology and interpretation of results are described in the Bachelor thesis.

## 1. Runtime configuration

The Sensor Producer, Kafka Streams application, and Measurement Consumer are standalone Java 21 services. Their runtime settings are passed as command-line arguments rather than through `application.yml` files. This keeps the configuration explicit for each experiment and avoids adding a separate YAML configuration mechanism.

- **Sensor Producer** (`services/sensor-producer`): workload rate, random seed, timestamps, broker addresses, and producer settings such as acknowledgments and idempotence are supplied through the CLI.
- **Kafka Streams** (`services/kafka-streams`): broker addresses, application ID, active processing rule, and rule-specific thresholds are supplied through the CLI. The application supports `HIGH_TEMPERATURE`, `HIGH_FREQUENCY`, and `RAPID_CHANGE`.
- **Measurement Consumer** (`services/measurement-consumer`): broker addresses, consumer group, offset behavior, and measurement options are configured through the CLI.

Keeping these settings outside the application code allows each experimental run to use its own configuration without changing the Java sources. The configuration used during a run is recorded with its experimental evidence.

## 2. Kafka Streams output models

The Kafka Streams service publishes two kinds of JSON records: `SensorResult` and `SensorAlert`.

### SensorResult

| Field | Java type |
| --- | --- |
| `eventId` | `String` |
| `runId` | `String` |
| `deviceId` | `String` |
| `eventTime` | `long` |
| `sentAt` | `long` |
| `ruleType` | `String` |
| `triggered` | `boolean` |

### SensorAlert

| Field | Java type |
| --- | --- |
| `eventId` | `String` |
| `runId` | `String` |
| `deviceId` | `String` |
| `eventTime` | `long` |
| `sentAt` | `long` |
| `ruleType` | `String` |

A `SensorResult` is written for each successfully processed input event, even when the active rule does not trigger. Its `triggered` field records the evaluation outcome. A `SensorAlert` is written only for a triggered rule. The supported `ruleType` values are `HIGH_TEMPERATURE`, `HIGH_FREQUENCY`, and `RAPID_CHANGE`.

Both models retain the identifiers and timestamps of the input event. `eventTime` and `sentAt` represent Unix epoch milliseconds. For stateful rules, an alert refers to the input event that caused the condition to trigger, not to an independent window summary.

The output models deliberately omit the temperature, thresholds, counts, and other rule-specific diagnostic data. `SensorAlert` has no `triggered` field because every alert already represents a triggered condition. The receipt timestamp `resultReceivedAt` is recorded by the Measurement Consumer rather than Kafka Streams.

These fields allow producer events and consumer observations to be matched by `(runId, eventId)` and provide the timestamps required for end-to-end latency measurement.

## 3. Experimental metrics and observations

The measurement pipeline distinguishes between attempted publications, confirmed publications, and unique results received by the Measurement Consumer. It also records alert counts, individual end-to-end latencies, duplicate deliveries, unmatched confirmed events, and relevant errors. Broker leadership and in-sync replica (ISR) observations are used for the failure/recovery scenario.

### Latency

End-to-end latency is calculated in milliseconds:

```text
latencyMs = resultReceivedAt - sentAt
```

P50, P95, and P99 are calculated using Type 7 linear interpolation; P50 is the median. The latency population uses unique results for measurement-phase events, including eligible results received during the drain period. Negative latency values are retained and flagged rather than changed to zero.

### Phases

The standard B1–B3 execution has three phases:

| Phase | Duration |
| --- | --- |
| `WARM_UP` | 60 seconds |
| `MEASUREMENT` | 300 seconds |
| `DRAIN` | Up to 120 seconds |

Warm-up gives the services time to settle before throughput measurement. Drain allows late results from measurement-phase events to be collected for latency and completeness checks. It does not extend the 300-second throughput interval. Observed throughput is calculated from actual timestamps, not the synthetic `eventTime` values.

### Reliability

Events are identified using `(runId, eventId)`. Confirmed publications are compared with unique received results after drain, while duplicate deliveries are counted separately. An unmatched confirmed event is reported as a missing result; this alone does not establish Kafka data loss.

### Evidence files

Experimental evidence is stored under `experiments/results/<runId>/`. The main files are:

| File | Contents |
| --- | --- |
| `run-config.json` | Actual run configuration, timing, and finalization metadata |
| `producer-events.csv` | Per-event publication attempts, confirmations, and related timestamps |
| `sensor-results.csv` | Received results, receipt timestamps, latency, and duplicate information |
| `throughput.csv` | Per-second attempted, confirmed, and unique-result observations |
| `summary.json` | Aggregated throughput, latency, reliability, and completion information |
| `coordination.jsonl` | Coordinator events and operational evidence, including B4 actions |
| `errors.log` | Recorded execution errors |

Incomplete runs retain their available evidence but are not treated as completed experimental observations. The files used for scientific evaluation are separate from the live Dashboard.

## 4. Throughput measurement policy

For B1–B3, a publication or result contributes to primary throughput only when both conditions hold:

1. The originating event belongs to `MEASUREMENT`.
2. The corresponding publication or receipt timestamp falls within the fixed 300-second measurement interval.

Attempted inputs, confirmed inputs, and unique results are reported separately. Results are deduplicated by `(runId, eventId)`, and one-second time bins include intervals with no observations.

Warm-up events confirmed during measurement do not count toward measurement throughput. Likewise, measurement events confirmed or received during drain remain available for latency and reliability analysis, but are not moved into the throughput interval. B4 uses additional time-based observations for interruption and recovery rather than treating its failure period as an ordinary B1–B3 run.

## 5. Run metadata and timing

Each experiment has a separate `run-config.json` file. The Sensor Producer writes its metadata when the optional `--run-config-output` argument is enabled. The coordinator takes over the file only after the Producer has finished, so the two processes do not write to it simultaneously. Metadata from a different `runId` must not be overwritten.

A run is not considered complete just because the Producer has exited. Runs that have not passed finalization remain marked `NOT_FINALIZED`; unavailable measurements are recorded as unknown rather than filled with estimated values.

Two Java clocks are used for different purposes:

- `System.nanoTime()` measures local elapsed time and schedules phases.
- `System.currentTimeMillis()` records epoch timestamps for observations and exported evidence.

At workload start, closely spaced `nanoTime()` and epoch readings are captured to qualify their alignment:

```java
long n0 = System.nanoTime();
long epochMs = System.currentTimeMillis();
long n1 = System.nanoTime();
long sampleSpanNs = n1 - n0;
```

This sampling provides an alignment estimate, not exact synchronization. `nanoTime()` readings are not compared across separate JVMs, and clock inconsistencies are reported rather than silently corrected.

The corresponding implementation is located in `RunMetadataWriter`, `WorkloadRunner`, and the Producer's optional metadata-output handling.

## 6. Experimental workloads

The configured Producer rates are targets, not guaranteed achieved throughput:

| Level | Requested rate (events/s) |
| --- | ---: |
| LOW | 10 |
| MEDIUM | 100 |
| HIGH | 1000 |

The scenarios use these workloads:

| Scenario | Workload levels |
| --- | --- |
| B1 — Processing Patterns | LOW, MEDIUM, HIGH |
| B2 — Kafka Configuration | LOW, HIGH |
| B3 — Local Parallelism | HIGH |
| B4 — Broker Failure and Recovery | MEDIUM |

Each run records the requested input rate alongside the actual attempted and confirmed rates. The experimental plan uses three repetitions per condition, paired random seeds for comparable variants, a documented execution order, and cleanup between independent runs.

### Preliminary workload checks

Three `HIGH_TEMPERATURE` pilot runs helped select the target rates. Their mean confirmed input rates over the 300-second measurement interval were:

| Requested rate (events/s) | Confirmed rate (events/s) | Evidence |
| --- | ---: | --- |
| 10 | 10.00 | [Reference pilot](../experiments/results/fd8-b1-high-temperature-reference-20261109/summary.json) |
| 100 | 100.00 | [100 events/s pilot](../experiments/results/fd8-b1-high-temperature-pilot-100eps-20261109/summary.json) |
| 1000 | approximately 950.51 | [1000 events/s pilot](../experiments/results/fd8-b1-high-temperature-pilot-1000eps-20261109/summary.json) |

These are individual pilot results, not substitutes for the three-repetition evaluation. The lower achieved rate at the highest target does not, by itself, show that Kafka was saturated or identify a single bottleneck.

## 7. B3: local Kafka Streams parallelism

B3 uses `RAPID_CHANGE` as the fixed processing rule. This rule retains the previous temperature and event timestamp for each `(runId, deviceId)` and checks the change against the current event. Using the same stateful rule in both conditions keeps the processing logic constant while the number of Kafka Streams instances changes.

Both conditions use:

- `maxTimeDifferenceMs = 5000`
- `temperatureChangeThreshold = 5.0`
- Three Kafka brokers and six partitions per application topic
- A requested Producer rate of 1000 events/s

| Condition | Streams instances | Threads per instance | CPU limit per instance | Memory limit per instance |
| --- | ---: | ---: | --- | --- |
| A | 1 | 1 | 2 CPU | 4 GiB |
| B | 2 | 1 | 1 CPU | 2 GiB |

The total Streams resource budget stays at 2 CPU and 4 GiB in both conditions. `scripts/run-kafka-streams-b3.sh` starts each instance in a `systemd-run --user --scope` unit with explicit `CPUQuota`, `MemoryMax`, and `MemorySwapMax=0` settings. The launcher rejects unsupported settings instead of starting an unrestricted JVM. The limits are resource ceilings, not dedicated cores.

B3 measures local parallelism on a single physical machine. It does not represent scaling across separate hosts, and the configuration alone does not imply that two instances perform better than one.

## 8. B4: broker failure and recovery observations

B4 records the effects of stopping and restarting a broker in the three-broker topology. Besides the standard throughput, latency, and reliability metrics, the analysis considers service continuity, broker leadership, and ISR observations.

The recovery assessment uses two criteria:

- **Service recovery:** service rates reach at least 95% of the pre-failure reference for a continuous 30-second window, without sustained growth of reconstructed pending-event populations. The reference uses the 60 seconds immediately before the interruption.
- **Replication recovery:** full ISR membership is observed and maintained for 30 seconds after a degradation.

These criteria depend on the available publication, receipt, and metadata observations. A result that cannot be established from the recorded evidence must not be presented as confirmed recovery. The detailed B4 procedure and interpretation remain in the thesis.
