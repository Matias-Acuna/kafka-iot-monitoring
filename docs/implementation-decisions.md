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
