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
