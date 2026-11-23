#!/usr/bin/env python3
"""Control Kafka brokers and collect metadata for B4 experiments.

The coordinator schedules broker operations, verifies the deployment,
records evidence, and attempts to restore the selected broker during
cleanup.

Importing this module does not execute Docker commands, and the module
does not write files. Restoration cannot be guaranteed if the process
is forcibly terminated, the host fails, or Docker becomes unavailable.

Metadata timestamps indicate when states were observed, not the exact
time of Kafka leader elections.
"""
import copy
import json
import math
from pathlib import Path
import re
import subprocess
import time

BROKERS = {4: "broker-1", 5: "broker-2", 6: "broker-3"}
TOPICS = {"sensor-events", "sensor-results", "sensor-alerts"}
COMPOSE_FILE = Path(__file__).resolve().parents[1] / "infra/compose.3-brokers.yml"


class ControlError(ValueError):
    """Rejected input, missing identity or unsuccessful broker operation."""


def broker_service(broker_id):
    if type(broker_id) is not int or broker_id not in BROKERS:
        raise ControlError("broker ID must be 4, 5 or 6")
    return BROKERS[broker_id]


def _integer(value, name, minimum=0):
    if type(value) is not int or value < minimum:
        raise ControlError(f"{name}: integer >= {minimum} required")
    return value


def parse_topic_metadata(output, topic, observed_at_epoch_ms):
    """Parse one complete six-partition CLI description; reject unknown leaders."""
    if topic not in TOPICS:
        raise ControlError("unsupported experimental topic")
    _integer(observed_at_epoch_ms, "observedAtEpochMs")
    partitions, summaries = {}, 0
    for line in output.splitlines():
        if not line.strip():
            continue
        pairs = re.findall(r"([A-Za-z]+):\s*(\S+)", line)
        fields = dict(pairs)
        if len(fields) != len(pairs):
            raise ControlError("duplicate metadata fields")
        if fields.get("Topic") != topic:
            raise ControlError("unexpected or malformed topic metadata line")
        if "PartitionCount" in fields:
            summaries += 1
            if fields.get("PartitionCount") != "6" or fields.get("ReplicationFactor") != "3":
                raise ControlError("expected six partitions and replication factor three")
            continue
        try:
            def number(name):
                value = fields[name]
                if not re.fullmatch(r"[0-9]+", value):
                    raise ValueError(name)
                return int(value)
            partition, leader = number("Partition"), number("Leader")
            def ids(name):
                values = fields[name].split(",")
                if any(not re.fullmatch(r"[0-9]+", v) for v in values):
                    raise ValueError(name)
                result = [int(v) for v in values]
                if len(set(result)) != len(result):
                    raise ValueError(name)
                return result
            replicas, isr = ids("Replicas"), ids("Isr")
        except (KeyError, ValueError) as error:
            raise ControlError("incomplete or malformed partition metadata") from error
        if (partition not in range(6) or partition in partitions
                or set(replicas) != set(BROKERS) or not set(isr).issubset(replicas)
                or leader not in BROKERS or leader not in isr):
            raise ControlError("invalid partition, leader, replica assignment or ISR")
        partitions[partition] = dict(topic=topic, partition=partition,
                                     leaderBrokerId=leader, replicaBrokerIds=replicas,
                                     isrBrokerIds=isr, observedAtEpochMs=observed_at_epoch_ms)
    if summaries != 1 or set(partitions) != set(range(6)):
        raise ControlError("complete topic summary and six unique partitions required")
    return [partitions[p] for p in range(6)]


class BrokerControl:
    """Explicit synchronous calls; command timeout is not an experiment timeline.

    Evidence is retained in memory and optionally forwarded to the coordinator's
    journal callback. Supply the actual existing Compose project explicitly.
    """
    def __init__(self, run_id, condition, project, command_timeout_seconds, emit=None):
        if not isinstance(run_id, str) or not run_id.strip():
            raise ControlError("nonblank runId required")
        if condition not in {"NO_FAILURE", "BROKER_FAILURE"}:
            raise ControlError("unsupported B4 condition")
        if not isinstance(project, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", project):
            raise ControlError("explicit Compose project identity required")
        if (type(command_timeout_seconds) not in (int, float)
                or not math.isfinite(command_timeout_seconds) or command_timeout_seconds <= 0):
            raise ControlError("finite positive command timeout required")
        # Verify the existing mapping without adding a YAML dependency.
        source = COMPOSE_FILE.read_text(encoding="utf-8")
        for broker, service in BROKERS.items():
            match = re.search(r"^  " + re.escape(service) + r":\n(.*?)(?=^  \S|^volumes:)", source, re.M | re.S)
            if not match or not re.search(r"KAFKA_NODE_ID:\s*" + str(broker) + r"\s*$", match[1], re.M):
                raise ControlError("Compose broker mapping differs from the approved deployment")
        self.run_id, self.condition, self.project = run_id, condition, project
        self.timeout, self.emit = command_timeout_seconds, emit
        self.compose = ["docker", "compose", "--project-name", project, "-f", str(COMPOSE_FILE)]
        self.evidence, self.containers, self.volumes = [], {}, {}
        self.selected, self.monitor = None, "broker-1"
        self.pending_restore = False
        self.previous, self.last_success = {}, {}

    def _record(self, kind, **fields):
        record = dict(runId=self.run_id, condition=self.condition, evidenceType=kind,
                      observedAtEpochMs=time.time_ns() // 1_000_000,
                      composeProject=self.project, composeFile=str(COMPOSE_FILE))
        if self.selected:
            record.update(self.selected)
        record.update(fields)
        self.evidence.append(copy.deepcopy(record))
        if self.emit:
            self.emit(copy.deepcopy(record))
        return record

    def _run(self, argv):
        requested = time.time_ns() // 1_000_000
        try:
            result = subprocess.run(argv, shell=False, capture_output=True, text=True,
                                    timeout=self.timeout, check=False)
            code, stdout, stderr, error = result.returncode, result.stdout, result.stderr, None
        except (subprocess.TimeoutExpired, OSError) as failure:
            code, error = None, f"{type(failure).__name__}: {failure}"
            def decoded(value):
                return value.decode(errors="replace") if isinstance(value, bytes) else value or ""
            stdout = decoded(getattr(failure, "stdout", ""))
            stderr = decoded(getattr(failure, "stderr", ""))
        record = self._record("B4_COMMAND_RESULT", command=list(argv),
                              requestedAtEpochMs=requested, completedAtEpochMs=time.time_ns() // 1_000_000,
                              returnCode=code, stdout=stdout, stderr=stderr, error=error,
                              status="SUCCEEDED" if code == 0 else "FAILED")
        if code != 0:
            raise ControlError(f"command failed; inspect B4_COMMAND_RESULT: {error or stderr}")
        return stdout, record

    def _inspect(self, service, pinned=True):
        if service not in BROKERS.values():
            raise ControlError("only approved broker services may be inspected or controlled")
        output, _ = self._run(self.compose + ["ps", "--all", "--quiet", service])
        ids = output.split()
        if len(ids) != 1 or not re.fullmatch(r"[0-9a-f]{12,64}", ids[0]):
            raise ControlError("exactly one existing broker container required")
        output, _ = self._run(["docker", "inspect", ids[0]])
        try:
            item, = json.loads(output)
            labels = item["Config"]["Labels"]
            config_files = labels["com.docker.compose.project.config_files"].split(",")
            broker = next(b for b, s in BROKERS.items() if s == service)
            mounts = [m for m in item["Mounts"] if m["Destination"] == "/var/lib/kafka/data"]
            mount, = mounts
            identity = item["Id"]
            if (identity != ids[0] or labels["com.docker.compose.project"] != self.project
                    or labels["com.docker.compose.service"] != service
                    or [Path(p).resolve() for p in config_files] != [COMPOSE_FILE]
                    or f"KAFKA_NODE_ID={broker}" not in item["Config"]["Env"]
                    or "KAFKA_PROCESS_ROLES=broker" not in item["Config"]["Env"]
                    or mount["Type"] != "volume" or not mount["Name"]):
                raise ControlError("container does not match the pinned three-broker deployment")
            state = item["State"]["Status"]
            if pinned and (self.containers.get(service) != identity or self.volumes.get(service) != mount["Name"]):
                raise ControlError("broker container or data volume identity changed")
        except (KeyError, TypeError, ValueError) as error:
            raise ControlError(f"invalid broker identity evidence: {error}") from error
        return identity, mount["Name"], state

    def pin_deployment(self):
        """Explicit preflight: pin all existing containers and their data volumes."""
        if self.containers:
            raise ControlError("deployment is already pinned")
        identities, volumes = {}, {}
        for service in BROKERS.values():
            identity, volume, state = self._inspect(service, pinned=False)
            if state != "running":
                raise ControlError("all three brokers must be running before pinning")
            identities[service], volumes[service] = identity, volume
        self.containers, self.volumes = identities, volumes
        return self._record("B4_DEPLOYMENT_PINNED", containers=identities, dataVolumes=volumes)

    def sample_metadata(self, topic, max_gap_ms):
        """One sample only. Caller supplies schedule/gap tolerance; failures retain gaps."""
        _integer(max_gap_ms, "max_gap_ms", 1)
        if topic not in TOPICS or not self.containers:
            raise ControlError("approved topic and pinned deployment required")
        try:
            if self._inspect(self.monitor)[2] != "running":
                raise ControlError("metadata monitoring broker is not running")
            output, _ = self._run(self.compose + ["exec", "-T", "--interactive=false", self.monitor,
                "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server",
                "broker-1:19092,broker-2:19092,broker-3:19092", "--describe", "--topic", topic])
            now = time.time_ns() // 1_000_000
            partitions = parse_topic_metadata(output, topic, now)
        except ControlError as error:
            return self._record("B4_METADATA_QUERY_FAILED", topic=topic, status="FAILED", error=str(error),
                                samplingGap=True,
                                previousSuccessfulObservedAtEpochMs=self.last_success.get(topic))
        previous = self.previous.get(topic, {})
        changes = [dict(partition=p["partition"], previousLeaderBrokerId=previous[p["partition"]]["leaderBrokerId"],
                        previousIsrBrokerIds=previous[p["partition"]]["isrBrokerIds"],
                        leaderBrokerId=p["leaderBrokerId"], isrBrokerIds=p["isrBrokerIds"])
                   for p in partitions if p["partition"] in previous and
                   (p["leaderBrokerId"] != previous[p["partition"]]["leaderBrokerId"]
                    or set(p["isrBrokerIds"]) != set(previous[p["partition"]]["isrBrokerIds"]))]
        gap = now - self.last_success[topic] if topic in self.last_success else None
        record = self._record("B4_METADATA_SAMPLE", topic=topic, status="OBSERVED", partitions=partitions,
                              changes=changes, sampleGapMs=gap, maxGapMs=max_gap_ms,
                              samplingGap=gap is not None and (gap < 0 or gap > max_gap_ms))
        self.previous[topic] = {p["partition"]: p for p in partitions}
        self.last_success[topic] = now
        self.sample_monotonic = time.monotonic()
        return record

    def select_leader(self, partition, max_metadata_age_ms, max_gap_ms):
        """Fresh sensor-events query; no fixed partition, leader or fault schedule."""
        _integer(partition, "partition")
        _integer(max_metadata_age_ms, "max_metadata_age_ms", 1)
        if partition not in range(6) or self.pending_restore:
            raise ControlError("valid partition and no outstanding restoration required")
        self.selected = None  # A failed refresh must not leave an earlier target usable.
        sample = self.sample_metadata("sensor-events", max_gap_ms)
        if sample["status"] != "OBSERVED":
            raise ControlError("fresh valid leadership metadata unavailable")
        selected = sample["partitions"][partition]
        self.selected = dict(topic=selected["topic"], partition=partition,
                             selectedBrokerId=selected["leaderBrokerId"],
                             selectedService=broker_service(selected["leaderBrokerId"]))
        self.selection_monotonic = self.sample_monotonic
        self.max_metadata_age_ms = max_metadata_age_ms
        self.monitor = next(s for s in BROKERS.values() if s != self.selected["selectedService"])
        return self._record("B4_BROKER_SELECTED", selectionMetadata=selected,
                            metadataObservedAtEpochMs=selected["observedAtEpochMs"],
                            maxMetadataAgeMs=max_metadata_age_ms)

    def _action(self, operation, stop_grace_seconds=None):
        if self.condition != "BROKER_FAILURE" or not self.selected:
            raise ControlError("broker actions require BROKER_FAILURE and explicit leader selection")
        service = self.selected["selectedService"]
        if broker_service(self.selected["selectedBrokerId"]) != service:
            raise ControlError("selected broker/service mismatch")
        _, _, state = self._inspect(service)
        if operation == "stop":
            _integer(stop_grace_seconds, "stop_grace_seconds", 1)
            if self.pending_restore or state != "running":
                raise ControlError("stop requires the original running broker and no pending restoration")
            if (time.monotonic() - self.selection_monotonic) * 1000 > self.max_metadata_age_ms:
                raise ControlError("leader selection is stale; acquire fresh metadata")
            self.pending_restore = True  # Even a timed-out stop may have changed Docker state.
            args = ["stop", "--timeout", str(stop_grace_seconds), service]
            expected = "exited"
        else:
            if not self.pending_restore:
                raise ControlError("restart requires a previously attempted stop")
            args, expected = ["start", service], "running"
        self._record("B4_BROKER_ACTION_REQUESTED", action=operation, containerId=self.containers[service],
                     faultMechanism="COMPOSE_SERVICE_STOP", stopGraceSeconds=stop_grace_seconds)
        try:
            _, command = self._run(self.compose + args)
            identity, volume, state = self._inspect(service)
            if state != expected:
                raise ControlError(f"broker state is {state}; expected {expected}")
        except ControlError as error:
            self._record("B4_BROKER_ACTION_FAILED", action=operation, error=str(error), status="FAILED")
            raise
        if operation == "start":
            self.pending_restore = False
        return self._record("B4_BROKER_ACTION_CONFIRMED", action=operation, status="CONFIRMED",
                            containerId=identity, dataVolume=volume, containerState=state,
                            requestedAtEpochMs=command["requestedAtEpochMs"],
                            completedAtEpochMs=command["completedAtEpochMs"], returnCode=command["returnCode"],
                            kafkaRecoveryVerified=False)

    def stop_selected(self, stop_grace_seconds):
        """Controlled Compose stop, not an abrupt crash or a promised SIGKILL fault."""
        return self._action("stop", stop_grace_seconds)

    def start_selected(self):
        """Start the same existing container; this does not certify Kafka recovery."""
        return self._action("start")

    def restore_selected(self, max_attempts):
        """Bounded cleanup retries; caller must journal failures and handle SIGKILL externally."""
        _integer(max_attempts, "max_attempts", 1)
        if max_attempts > 10:
            raise ControlError("at most ten restoration attempts permitted")
        if not self.pending_restore:
            return self._record("B4_RESTORATION_NOT_REQUIRED")
        errors = []
        for attempt in range(1, max_attempts + 1):
            try:
                return self.start_selected()
            except ControlError as error:
                errors.append(str(error))
                self._record("B4_RESTORATION_ATTEMPT_FAILED", attempt=attempt, error=str(error))
        return self._record("B4_RESTORATION_FAILED", status="FAILED", errors=errors,
                            manualInspectionRequired=True)
