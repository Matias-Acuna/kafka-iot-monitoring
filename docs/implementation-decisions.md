# Implementation Decisions

## D.2.34 — Sensor Producer CLI Configuration

- **Approval date:** 2026-11-05
- **Status:** Approved by the project owner
- **Component:** `services/sensor-producer`

### Original approved design

The official Phase D protocol included `src/main/resources/application.yml` in the Sensor Producer structure.

### Approved implementation change

Do not create `application.yml` for the Sensor Producer. Use the existing CLI configuration implemented in `SensorProducerApplication.java`.

### Technical rationale

- The producer uses plain Java 21 and Kafka Clients 4.1.2, without Spring Boot or a YAML configuration reader.
- CLI arguments already supply and validate the required execution parameters.
- The CLI mechanism has been functionally verified in Ubuntu and Docker.
- Adding YAML support would introduce redundant configuration and unnecessary complexity.

### Experimental implications

- Workload settings and Kafka producer options remain configurable through CLI arguments.
- Experimental reproducibility requires the parameters actually used to be recorded for each run.
- The Chapter 4 methodology and B1–B4 experiment definitions remain unchanged.

### Scope

This decision applies only to the Sensor Producer. It does not automatically remove `application.yml` from the planned Kafka Streams or Measurement Consumer modules.

### Traceability

This is an explicitly authorized minor implementation adaptation to the official Phase D protocol, not a silent revision of that protocol.

## D.3.13 — Kafka Streams Output Contracts

- **Approval date:** 2026-11-05
- **Status:** Approved by the project owner
- **Component:** `services/kafka-streams`

### Approved output contracts

`SensorResult` and `SensorAlert` are the formally approved Kafka Streams output contracts. Their fields and Java types are defined in the following order.

**SensorResult**

| Field | Java type |
| --- | --- |
| `eventId` | `String` |
| `runId` | `String` |
| `deviceId` | `String` |
| `eventTime` | `long` |
| `sentAt` | `long` |
| `ruleType` | `String` |
| `triggered` | `boolean` |

**SensorAlert**

| Field | Java type |
| --- | --- |
| `eventId` | `String` |
| `runId` | `String` |
| `deviceId` | `String` |
| `eventTime` | `long` |
| `sentAt` | `long` |
| `ruleType` | `String` |

### Output semantics

- One `SensorResult` is produced per successfully processed input event, regardless of whether the active rule triggers. Its `triggered` field records that outcome.
- A `SensorAlert` is produced separately only when the active rule triggers.
- `ruleType` uses `String` with the controlled values `HIGH_TEMPERATURE`, `HIGH_FREQUENCY`, and `RAPID_CHANGE`.
- `eventId`, `runId`, `deviceId`, `eventTime`, and `sentAt` preserve the corresponding input event values. Both timestamps retain their Unix epoch millisecond meaning.
- Stateful alerts are associated with the input event that triggers their condition, rather than independent window summaries.

### Minimal contract scope

These contracts exclude temperature, thresholds, frequency counts, temperature differences, and additional diagnostic fields. `SensorAlert` has no `triggered` field because its presence represents a triggered condition.

`resultReceivedAt` belongs to the Measurement Consumer and is not part of either Kafka Streams output model. The Measurement Consumer calculates E2E latency as `resultReceivedAt - sentAt`.

### Experimental implications and traceability

The contracts preserve event and run traceability, maintain the timestamps needed by the experimental design, and support the subsequent FD4 and FD5 implementation stages across all three rules.

This is an approved implementation-level contract decision. It does not modify the official Phase D protocol or Chapter 4.

## D.3.31 — Kafka Streams CLI Configuration

- **Approval date:** 2026-11-05
- **Status:** Approved by the project owner
- **Component:** `services/kafka-streams`

### Original approved design

The official Phase D protocol originally specifies an `application.yml` file for the Kafka Streams service.

### Approved implementation change

The project owner explicitly approved replacing `application.yml` with command-line (CLI) configuration for this service. The Kafka Streams application will receive its runtime configuration through these planned parameters:

- `--bootstrap-servers`
- `--application-id`
- `--rule`
- `--temperature-threshold`

Initially, only `HIGH_TEMPERATURE` will be supported as the active rule. `HIGH_FREQUENCY` and `RAPID_CHANGE` belong to the later D.5 implementation stage. The temperature threshold must remain configurable, not hardcoded.

The existing `StreamsConfig.createProperties()` method will be reused for Kafka Streams baseline properties. The CLI implementation is still pending and will be developed in subsequent steps.

### Technical rationale

This decision avoids an additional YAML parser and unnecessary duplicate configuration mechanisms. The approach remains consistent with the previously approved CLI configuration of the Sensor Producer.

### Scope and traceability

This decision applies only to the Kafka Streams service. It does not change the existing Sensor Producer configuration decision or automatically change the future Measurement Consumer configuration.

This is an explicitly approved implementation adaptation. It does not modify the official Phase D protocol or Chapter 4.

## D.4.9 — Measurement Consumer CLI Configuration

- **Approval date:** 2026-11-06
- **Status:** Approved by the project owner
- **Component:** `services/measurement-consumer`

### Original approved design

The official Phase D protocol originally specifies `services/measurement-consumer/src/main/resources/application.yml` for the Measurement Consumer configuration.

### Approved implementation change

Use command-line (CLI) arguments for the Measurement Consumer configuration instead of `application.yml`.

### Technical rationale

- The Measurement Consumer is a standalone Java 21 application using Apache Kafka Clients 4.1.2.
- CLI configuration is consistent with the previously approved Sensor Producer and Kafka Streams implementations.
- No additional YAML parser or application framework is necessary.
- Runtime parameters can be supplied explicitly for experimental executions.
- Avoiding redundant configuration reduces unnecessary complexity.

### Experimental implications

Runtime configuration can be supplied explicitly through CLI arguments for each experimental execution. This decision does not modify the experimental methodology in Chapter 4.

### Scope and traceability

This decision applies only to the Measurement Consumer. All previous implementation decisions remain unchanged.

This is an explicitly approved implementation adaptation. The original Phase D protocol remains unchanged.

## D.6.2 — FD6 Experimental Metrics and Export Design

- **Approval date:** 2026-11-06
- **Status:** Approved by the project owner
- **Component:** FD6 experimental metrics and exports across the Sensor Producer, Kafka Streams, and Measurement Consumer

### Approved implementation design

FD6 will implement the following experimental measurements:

- Attempted input throughput and confirmed input throughput.
- Unique results throughput, with per-second observations.
- Per-second observations of attempted and confirmed inputs, and alerts per second as a secondary metric.
- Individual end-to-end latency and P50, P95, and P99 latency percentiles.
- Missing results and duplicate deliveries, counted separately.
- Production and processing errors.
- Interruption and recovery observations.
- Broker leadership and in-sync replica (ISR) observations.

These are approved implementation requirements, not a claim that all measurements or exports already exist.

### Latency and percentile calculation

Calculate individual end-to-end latency in milliseconds as:

```text
latencyMs = resultReceivedAt - sentAt
```

Calculate P50, P95, and P99 using Type 7 linear interpolation, with P50 representing the median. Use unique measurement-event results, including eligible results received during drain. Retain and flag negative latency observations instead of silently converting them to zero.

### Experimental phases and throughput

The approved ordinary experimental phases are:

| Phase | Duration |
| --- | --- |
| `WARM_UP` | 60 seconds |
| `MEASUREMENT` | 300 seconds |
| `DRAIN` | Up to 120 seconds |

Results from measurement-phase events received during drain count for latency and completeness checks, but not for measured throughput during the 300-second measurement interval. Throughput observations must use actual elapsed time, not simulated `eventTime`.

### Reliability accounting

- Identify events by `(runId, eventId)` and retain confirmed producer event identities.
- Count unique received results separately from duplicate deliveries.
- Reconcile confirmed inputs against unique received results after the drain boundary.
- Do not automatically equate missing results with Kafka data loss.

### Experimental export structure

Store each run's experimental observations under:

```text
experiments/results/<runId>/
    run-config.json
    producer-events.csv
    sensor-results.csv
    throughput.csv
    summary.json
    errors.log
```

| File | Intended purpose |
| --- | --- |
| `run-config.json` | Record the actual configuration, workload parameters, random seeds, and phase boundaries. |
| `producer-events.csv` | Preserve individual producer event identities and publication outcomes, including confirmation evidence for reconciliation. |
| `sensor-results.csv` | Preserve individual received-result observations, including receipt timestamps, calculated latency, duplicate status, and negative-latency flags. |
| `throughput.csv` | Record per-second attempted input, confirmed input, unique result, and secondary alert observations using actual elapsed time. |
| `summary.json` | Record measured run summaries, latency percentiles, reliability accounting, and execution completion status. |
| `errors.log` | Preserve production and processing errors and interruption, recovery, broker leadership, and ISR observations. |

Preserve available partial observations from incomplete executions and explicitly record their completion status. Incomplete executions must not be presented as valid completed experimental runs. Store experimental observations separately from dashboard visualization.

### B4 experimental implications

B4 retains its special experimental procedure and provisional recovery criteria defined in Chapter 4:

- Service rates must reach at least 95% of the pre-failure reference for 30 seconds, without sustained growth of pending events.
- The pre-failure reference is based on the 60 seconds immediately before broker interruption.
- Full ISR recovery must be maintained for 30 seconds.
- These provisional recovery criteria require validation through pilot tests.

### Technical rationale and implementation restrictions

- Reuse existing components wherever possible and avoid unnecessary dependencies and services.
- Prefer progressive writing of individual measurements to limit memory usage.
- Preserve the existing Kafka message contracts and established processing rules.
- Do not change Chapter 4 or its experimental methodology.
- Do not execute final B1–B4 experiments in FD6.
- Preserve the separation between FD6 metrics, FD7 dashboard visualization, and FD8 experiments.

### Scope and traceability

This decision records implementation details formally approved by the project owner for FD6. It does not replace or modify the official Phase D protocol or the methodology in Chapter 4. All previous implementation decisions remain unchanged.
