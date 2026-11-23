#!/usr/bin/env python3
"""Supervise B1-B4 execution and validate finalization evidence; never calculate metrics.

CLI: python3 scripts/run-experiment.py --config PATH
Or explicitly: python3 scripts/run-experiment.py --revalidate-run PATH/run-config.json
JSON keys: runId, scenario, outputDirectory, commands (consumer/streams/producer
argument arrays; optional streams2 for B3), optional startupTimeoutSeconds (120),
shutdownTimeoutSeconds (45). Commands execute from the repository root. Existing Java option/value
interfaces are preserved; evidence flags, ordinary durations and fresh output
paths are added or checked. No shell strings or shell -c commands are accepted.
Outputs: coordination.jsonl, errors.log and per-component stdout/stderr logs,
plus files created by the Java applications. Metadata handoff follows Producer exit;
finalization requires verified operational evidence. Revalidation is never automatic.
"""

import argparse
import importlib.util
import queue
import threading
from contextlib import contextmanager
import csv
import re
import fcntl
import stat
import tempfile
import json
import math
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import time

ROLES = ("consumer", "streams", "producer")


class CoordinationError(ValueError):
    pass


def text(value, name):
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise CoordinationError(f"{name}: nonblank text required")
    return value


def integer(value, name):
    if type(value) is not int or value < 0:
        raise CoordinationError(f"{name}: nonnegative integer required")
    return value


def command_roles(config):
    """Preserve legacy roles; only B3 may add the second Streams process."""
    commands = config.get("commands")
    roles = ROLES + (("streams2",) if isinstance(commands, dict) and "streams2" in commands else ())
    if not isinstance(commands, dict) or set(commands) != set(roles):
        raise CoordinationError("commands must identify consumer, streams and producer; only B3 may add streams2")
    if "streams2" in commands and config.get("scenario") != "B3":
        raise CoordinationError("streams2 is supported only for B3")
    return roles


def streams_roles(config):
    return tuple(role for role in command_roles(config) if role in {"streams", "streams2"})


def state_directory(value):
    path = Path(text(value, "state-dir"))
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / path
    return path.resolve()


def verify_streams_pair(config):
    """Check shared application settings without creating directories or processes."""
    if "streams2" not in config["commands"]:
        return
    options = {}
    for role in ("streams", "streams2"):
        command = config["commands"][role]
        options[role] = {command[i][2:]: command[i + 1] for i in range(len(command) - 1)
                         if command[i].startswith("--")}
        text(options[role].get("state-dir"), f"{role}: --state-dir")
    first, second = options["streams"], options["streams2"]
    if state_directory(first["state-dir"]) == state_directory(second["state-dir"]):
        raise CoordinationError("streams and streams2 require distinct normalized state directories")
    shared = {"bootstrap-servers", "application-id", "rule", "temperature-threshold",
              "window-duration-ms", "count-threshold", "max-time-difference-ms",
              "temperature-change-threshold"}
    for name in shared:
        if first.get(name) != second.get(name):
            raise CoordinationError(f"streams2: --{name} must match streams")


def validate_b4_settings(config):
    """B4 minute eight is 480 seconds after the end of warm-up.

    No timing, polling tolerance or Compose project is silently defaulted.
    """
    required = {"condition", "composeProject", "targetPartition", "warmUpSeconds",
                "preFailureSeconds", "interruptionSeconds", "generationSecondsAfterWarmUp",
                "observationSecondsAfterWarmUp", "drainSeconds", "metadataIntervalSeconds",
                "metadataMaxGapMs", "metadataMaxAgeMs", "commandTimeoutSeconds",
                "stopGraceSeconds", "restorationAttempts", "maxActionDelayMs"}
    settings = config.get("b4")
    if not isinstance(settings, dict) or set(settings) != required:
        raise CoordinationError("b4 must contain exactly the documented B4 control/timing keys")
    if not isinstance(settings["condition"], str) or settings["condition"] not in {"NO_FAILURE", "BROKER_FAILURE"}:
        raise CoordinationError("b4.condition must be NO_FAILURE or BROKER_FAILURE")
    if not isinstance(settings["composeProject"], str) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", settings["composeProject"]):
        raise CoordinationError("b4.composeProject must explicitly identify the existing deployment")
    for key in required - {"condition", "composeProject"}:
        integer(settings[key], "b4." + key)
    if settings["targetPartition"] not in range(6):
        raise CoordinationError("b4.targetPartition must be 0..5")
    for key, expected in {"warmUpSeconds": 60, "preFailureSeconds": 120, "interruptionSeconds": 60,
                          "generationSecondsAfterWarmUp": 480, "observationSecondsAfterWarmUp": 480}.items():
        if settings[key] != expected:
            raise CoordinationError(f"b4.{key} must be explicitly {expected} for this planning interpretation")
    if settings["drainSeconds"] > 120:
        raise CoordinationError("B4 drain must be bounded by 120 seconds")
    for key in ("metadataIntervalSeconds", "metadataMaxGapMs", "metadataMaxAgeMs",
                "commandTimeoutSeconds", "stopGraceSeconds", "restorationAttempts", "maxActionDelayMs"):
        if settings[key] <= 0:
            raise CoordinationError(f"b4.{key} must be positive")
    if (settings["metadataIntervalSeconds"] > 60 or settings["restorationAttempts"] > 10
            or settings["metadataMaxGapMs"] < settings["metadataIntervalSeconds"] * 1000
            or settings["stopGraceSeconds"] >= settings["commandTimeoutSeconds"]
            or settings["maxActionDelayMs"] >= settings["interruptionSeconds"] * 1000):
        raise CoordinationError("contradictory B4 cadence, timeout, restoration or action-delay limits")
    # One worker serializes queries/actions; cleanup covers a running job plus restoration.
    bound = (9 + 5 * settings["restorationAttempts"]) * settings["commandTimeoutSeconds"] + 2
    shutdown_timeout = config.get("shutdownTimeoutSeconds", 45)
    if type(shutdown_timeout) not in {int, float} or not math.isfinite(shutdown_timeout) or shutdown_timeout < bound:
        raise CoordinationError(f"B4 shutdownTimeoutSeconds must cover the bounded worker cleanup ({bound} s)")
    return settings


def experiment_durations(config):
    if config.get("scenario") == "B4":
        b4 = validate_b4_settings(config)
        return b4["generationSecondsAfterWarmUp"], b4["drainSeconds"]
    return 300, 120


def validate_b4_commands(config):
    """No alternate executables, topology flags or extra JVMs for B4."""
    root = Path(__file__).resolve().parents[1]
    options = {}
    for role, script in {"consumer": "run-measurement-consumer.sh", "streams": "run-kafka-streams.sh",
                         "producer": "run-sensor-producer.sh"}.items():
        command = config["commands"][role]
        first = next((i for i, arg in enumerate(command) if arg.startswith("--")), len(command))
        if first != 1 or Path(command[0]).resolve() != root / "scripts" / script:
            raise CoordinationError(f"B4 {role} requires the existing {script} executable")
        options[role] = dict(zip(command[first::2], command[first + 1::2]))
        if options[role].get("--bootstrap-servers") != "localhost:29092,localhost:39092,localhost:49092":
            raise CoordinationError(f"B4 {role} requires the THREE_BROKERS bootstrap list")
    producer, streams = options["producer"], options["streams"]
    if producer.get("--acks", "all") != "all" or producer.get("--enable-idempotence", "true") != "true":
        raise CoordinationError("B4 requires Producer acks=all and idempotence=true")
    try:
        correct = (float(producer["--target-events-per-second"]) == 100
                   and streams["--rule"] == "RAPID_CHANGE"
                   and int(streams["--max-time-difference-ms"]) == 5000
                   and float(streams["--temperature-change-threshold"]) == 5.0)
    except (KeyError, ValueError):
        correct = False
    if not correct:
        raise CoordinationError("B4 requires MEDIUM=100 and RAPID_CHANGE parameters 5000/5.0")


def b4_module():
    """Import primitives only; never instantiate control during static revalidation."""
    spec = importlib.util.spec_from_file_location("b4_control", Path(__file__).with_name("b4-control.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate(config):
    """Validate all launch inputs without creating files or processes."""
    if not isinstance(config, dict):
        raise CoordinationError("configuration must be an object")
    run_id = text(config.get("runId"), "runId")
    if config.get("scenario") not in ("B1", "B2", "B3", "B4"):
        raise CoordinationError("scenario must be B1, B2, B3 or B4")
    output = Path(text(config.get("outputDirectory"), "outputDirectory")).resolve()
    if output.exists():
        raise CoordinationError(f"output directory already exists: {output}")
    commands = config.get("commands")
    roles = command_roles(config)
    if config["scenario"] == "B4":
        validate_b4_settings(config)
    result = dict(config, outputDirectory=output, commands={})
    for key, default in (("startupTimeoutSeconds", 120), ("shutdownTimeoutSeconds", 45)):
        value = config.get(key, default)
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            raise CoordinationError(f"{key} must be finite and positive")
        result[key] = value
    allowed = {
        "consumer": {"bootstrap-servers", "group-id", "auto-offset-reset", "result-csv-output", "readiness-stdout", "run-id"},
        "streams": {"bootstrap-servers", "application-id", "rule", "temperature-threshold",
                    "window-duration-ms", "count-threshold", "max-time-difference-ms",
                    "temperature-change-threshold", "lifecycle-stdout", "state-dir"},
        "producer": {"bootstrap-servers", "run-id", "random-seed", "initial-event-timestamp",
                     "timestamp-increment", "target-events-per-second", "warm-up-seconds",
                     "measurement-seconds", "acks", "enable-idempotence", "csv-output",
                     "event-csv-output", "run-config-output", "timing-stdout"},
    }
    managed = {
        "consumer": {"readiness-stdout": "true", "result-csv-output": str(output / "sensor-results.csv"), "run-id": run_id},
        "streams": {"lifecycle-stdout": "true"},
        "producer": {"timing-stdout": "true", "run-id": run_id, "warm-up-seconds": "60",
                     "measurement-seconds": "300", "csv-output": str(output / "producer-totals.csv"),
                     "event-csv-output": str(output / "producer-events.csv"),
                     "run-config-output": str(output / "run-config.json")},
    }
    if config["scenario"] == "B4":
        managed["producer"]["measurement-seconds"] = str(config["b4"]["generationSecondsAfterWarmUp"])
    for role in roles:
        base_role = "streams" if role == "streams2" else role
        b3_streams = config["scenario"] == "B3" and base_role == "streams"
        command = commands[role]
        if not isinstance(command, list) or not command:
            raise CoordinationError(f"{role}: command must be a nonempty argument array")
        command = [text(arg, f"{role} argument") for arg in command]
        if Path(command[0]).name in {"bash", "sh", "zsh", "cmd", "cmd.exe", "powershell", "pwsh"}:
            if any(arg.lower() in {"-c", "/c", "-command", "-encodedcommand"} for arg in command):
                raise CoordinationError("shell command strings are unsupported")
        first = next((i for i, arg in enumerate(command) if arg.startswith("--")), len(command))
        if first == 0 or not shutil.which(command[0]):
            raise CoordinationError(f"{role}: launch executable is missing or unavailable")
        options = {}
        arguments = command[first:]
        if len(arguments) % 2:
            raise CoordinationError(f"{role}: options require values")
        for option, value in zip(arguments[::2], arguments[1::2]):
            name = option.removeprefix("--")
            permitted = allowed[base_role] | ({"cpu-quota", "memory-max"} if b3_streams else set())
            if not option.startswith("--") or name not in permitted or name in options or value.startswith("--"):
                raise CoordinationError(f"{role}: unknown/duplicate option or missing value: {option}")
            options[name] = value
        if b3_streams:
            launcher = Path(__file__).resolve().with_name("run-kafka-streams-b3.sh")
            if first != 1 or Path(command[0]).resolve() != launcher:
                raise CoordinationError(f"{role}: B3 requires the dedicated run-kafka-streams-b3.sh launcher")
            expected = ("100%", "2G") if "streams2" in commands else ("200%", "4G")
            if (options.get("cpu-quota"), options.get("memory-max")) != expected:
                raise CoordinationError(f"{role}: B3 requires --cpu-quota {expected[0]} --memory-max {expected[1]}")
            text(options.get("state-dir"), f"{role}: --state-dir")
        for name, value in managed[base_role].items():
            if name in options and options[name] != value:
                raise CoordinationError(f"{role}: --{name} must be {value}")
            options[name] = value
        required = {"consumer": ["bootstrap-servers", "group-id"],
                    "streams": ["bootstrap-servers", "application-id", "rule"],
                    "producer": ["bootstrap-servers", "random-seed", "initial-event-timestamp",
                                 "timestamp-increment", "target-events-per-second"]}[base_role]
        for name in required:
            text(options.get(name), f"{role}: --{name}")
        if role == "consumer" and options.get("auto-offset-reset", "earliest") not in {"earliest", "latest"}:
            raise CoordinationError("invalid Consumer auto-offset-reset")
        if role in {"streams", "streams2"}:
            rule_parameters = {"HIGH_TEMPERATURE": {"temperature-threshold"},
                               "HIGH_FREQUENCY": {"window-duration-ms", "count-threshold"},
                               "RAPID_CHANGE": {"max-time-difference-ms", "temperature-change-threshold"}}
            rule = options["rule"]
            if rule not in rule_parameters:
                raise CoordinationError("unsupported Streams rule")
            supplied = set(options) & set().union(*rule_parameters.values())
            if supplied != rule_parameters[rule]:
                raise CoordinationError("missing or mixed rule parameters")
        try:
            for name in ("random-seed", "initial-event-timestamp", "timestamp-increment",
                         "window-duration-ms", "count-threshold", "max-time-difference-ms"):
                if name in options:
                    value = int(options[name])
                    if not -(2**63) <= value < 2**63 or (name not in {"random-seed", "initial-event-timestamp"} and value <= 0):
                        raise ValueError(name)
                    if name == "count-threshold" and value >= 2**31:
                        raise ValueError(name)
            for name in ("target-events-per-second", "temperature-threshold", "temperature-change-threshold"):
                if name in options:
                    value = float(options[name])
                    if not math.isfinite(value) or (name != "temperature-threshold" and value <= 0):
                        raise ValueError(name)
        except ValueError as error:
            raise CoordinationError(f"{role}: invalid numeric option: {error}") from error
        if role == "producer":
            acks, idem = options.get("acks", "all"), options.get("enable-idempotence", "true")
            if acks not in {"all", "-1", "0", "1"} or idem not in {"true", "false"} or (idem == "true" and acks not in {"all", "-1"}):
                raise CoordinationError("invalid Producer acks/idempotence combination")
        result["commands"][role] = command[:first] + [item for name, value in options.items() for item in ("--" + name, value)]
    verify_streams_pair(result)
    if config["scenario"] == "B4":
        validate_b4_commands(result)
    return result



def verify_lock_compatibility(directory):
    """Check both directions on the destination filesystem, using real Java locks.

    FileChannel.tryLock() uses POSIX record locks on the supported local POSIX
    runtime. Python must use lockf (fcntl record locks), never flock. Probe the
    actual filesystem rather than assuming that this applies to every mount.
    """
    java = shutil.which("java")
    if not java:
        raise CoordinationError("Java is required to verify metadata lock interoperability")
    with tempfile.TemporaryDirectory(prefix=".metadata-lock-probe-", dir=directory) as temporary:
        source = Path(temporary) / "LockProbe.java"
        lock_path = Path(temporary) / "probe.lock"
        source.write_text('''import java.nio.channels.*;
import java.nio.file.*;
class LockProbe {
 public static void main(String[] args) throws Exception {
  try (var channel = FileChannel.open(Path.of(args[0]),
       StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
   try (var lock = channel.tryLock()) {
    System.out.println(lock == null ? "BUSY" : "LOCKED");
    System.out.flush();
    if (lock != null) System.in.read();
   }
  }
 }
}
''', encoding="utf-8")
        command = [java, str(source), str(lock_path)]
        with lock_path.open("ab") as lock:
            fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = subprocess.run(command, input="\n", capture_output=True, text=True, timeout=20)
            if result.returncode or result.stdout.strip() != "BUSY":
                raise CoordinationError("Java did not respect the Python metadata record lock")
        child = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                if not selector.select(20) or child.stdout.readline().strip() != b"LOCKED":
                    raise CoordinationError("Java metadata lock probe did not acquire its lock")
            with lock_path.open("ab") as lock:
                try:
                    fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    pass
                else:
                    raise CoordinationError("Python did not respect the Java FileChannel lock")
            _, stderr = child.communicate(b"\n", timeout=20)
            if child.returncode:
                raise CoordinationError(f"Java metadata lock probe failed: {stderr.decode(errors='replace')}")
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate(timeout=5)


def reject_json_constant(value):
    raise CoordinationError(f"nonfinite metadata value: {value}")


def strict_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CoordinationError(f"duplicate metadata key: {key}")
        result[key] = value
    return result


CONFIGURATION_TYPES = {"streams": "KAFKA_STREAMS_CONFIGURATION",
                       "streams2": "KAFKA_STREAMS_CONFIGURATION",
                       "consumer": "MEASUREMENT_CONSUMER_CONFIGURATION"}


def configuration_signature(event):
    """Repeated identical settings may have a different actual observation time."""
    return {key: value for key, value in event.items() if key != "observedAtEpochMs"}


def validate_configuration(event, role, config):
    """Validate recorded configuration evidence against the process launch arguments."""
    if (role not in config.get("commands", {}) or role not in CONFIGURATION_TYPES
            or event.get("evidenceType") != CONFIGURATION_TYPES[role]):
        raise CoordinationError("configuration evidence from unexpected source process")
    observed = integer(event.get("observedAtEpochMs"), "configuration observedAtEpochMs")
    if observed >= 2**63:
        raise CoordinationError("configuration observation timestamp exceeds Java epoch range")
    applied = event.get("appliedConfiguration")
    if not isinstance(applied, dict):
        raise CoordinationError("configuration appliedConfiguration must be an object")
    unavailable = event.get("unavailable")
    if not isinstance(unavailable, list) or any(not isinstance(v, str) or not v.strip() for v in unavailable):
        raise CoordinationError("configuration unavailable must be a list of names")
    command = config.get("commands", {}).get(role, [])
    options = {command[i][2:]: command[i + 1] for i in range(len(command) - 1) if command[i].startswith("--")}
    identity, property_name, option = (("applicationId", "application.id", "application-id") if role in {"streams", "streams2"}
                                       else ("groupId", "group.id", "group-id"))
    actual = text(event.get(identity), identity)
    if actual != options.get(option) or applied.get(property_name) != actual:
        raise CoordinationError(f"{role}: conflicting configuration {identity}")
    brokers = applied.get("bootstrap.servers")
    if (not isinstance(brokers, list) or not brokers
            or any(not isinstance(v, str) or not v.strip() for v in brokers)
            or brokers != [v.strip() for v in options.get("bootstrap-servers", "").split(",")]):
        raise CoordinationError(f"{role}: conflicting/malformed bootstrap servers")
    if role in {"streams", "streams2"}:
        if "state-dir" in options:
            actual_state = text(applied.get("state.dir"), f"{role}: effective state.dir")
            if state_directory(actual_state) != state_directory(options["state-dir"]):
                raise CoordinationError(f"{role}: effective state.dir conflicts with launch arguments")
        rule, parameters = event.get("rule"), event.get("ruleParameters")
        if rule != options.get("rule") or not isinstance(parameters, dict):
            raise CoordinationError("Streams configuration rule is missing/conflicting")
        definitions = {"HIGH_TEMPERATURE": {"temperatureThreshold": ("temperature-threshold", float)},
                       "HIGH_FREQUENCY": {"windowDurationMs": ("window-duration-ms", int),
                                          "countThreshold": ("count-threshold", int)},
                       "RAPID_CHANGE": {"maxTimeDifferenceMs": ("max-time-difference-ms", int),
                                        "temperatureChangeThreshold": ("temperature-change-threshold", float)}}
        required = definitions.get(rule)
        if required is None or set(parameters) != set(required):
            raise CoordinationError("Streams configuration rule parameters are missing/conflicting")
        for name, (argument, convert) in required.items():
            value = parameters[name]
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or (convert is int and type(value) is not int)
                    or argument not in options or value != convert(options[argument])):
                raise CoordinationError(f"Streams configuration parameter conflicts: {name}")
        if applied.get("processing.guarantee") != "at_least_once":
            raise CoordinationError("Streams configuration processing guarantee is missing/conflicting")
        if type(applied.get("num.stream.threads")) is not int or applied["num.stream.threads"] != 1:
            raise CoordinationError("Streams configuration thread count is missing/conflicting")
    else:
        if "run-id" in options and event.get("runIdFilter") != options["run-id"]:
            raise CoordinationError("Consumer configuration run isolation is missing/conflicting")
        topics = event.get("subscribedTopics")
        if not isinstance(topics, list) or len(topics) != 2 or any(not isinstance(v, str) for v in topics) or set(topics) != {"sensor-results", "sensor-alerts"}:
            raise CoordinationError("Consumer configuration subscribed topics are missing/conflicting")
        if applied.get("auto.offset.reset") != options.get("auto-offset-reset", "earliest"):
            raise CoordinationError("Consumer configuration offset reset policy is missing/conflicting")
        enabled = event.get("resultCsvEnabled")
        output = event.get("resultCsvOutput")
        expected = options.get("result-csv-output")
        if type(enabled) is not bool or enabled != (expected is not None):
            raise CoordinationError("Consumer configuration CSV enabled flag is missing/conflicting")
        if expected is not None:
            if not isinstance(output, str) or output != str(Path(expected).absolute()):
                raise CoordinationError("Consumer configuration CSV output is missing/conflicting")
        elif output is not None:
            raise CoordinationError("Consumer configuration has an unexpected CSV output")
    return event


def verify_configuration_history(role, coordination, config):
    """Recheck persisted source, order and settings; never trust a verified flag."""
    observed = None
    for record in coordination.get("evidence", []):
        if record.get("evidenceType") != "COMPONENT_EVIDENCE":
            continue
        source, event = record.get("component"), record.get("evidence", {})
        if event.get("evidenceType") in CONFIGURATION_TYPES.values():
            validate_configuration(event, source, config)
            if source == role:
                if observed is not None and configuration_signature(observed) != configuration_signature(event):
                    raise CoordinationError(f"{role}: contradictory repeated configuration evidence")
                observed = observed or event
        if source == role and event.get("status") == "READY":
            if observed is None or integer(event.get("observedAtEpochMs"), "READY timestamp") < observed["observedAtEpochMs"]:
                raise CoordinationError(f"{role}: READY precedes configuration evidence")
    if observed is None:
        raise CoordinationError(f"{role}: required configuration evidence is missing")
    if coordination.get("configurationEvidence", {}).get(role) != observed:
        raise CoordinationError(f"{role}: stored verified configuration differs from observed evidence")


def operational_coverage(coordination, config, start, cutoff):
    """Validate the consumer's reporting cadence.

    After the first successful poll, the consumer reports at intervals
    of at least 1 second using a monotonic clock. Each report is generated
    after processing the polled batch, with an epoch timestamp recorded
    immediately after the poll returns.

    The maximum accepted gap is 2001 ms: 1 second for the reporting
    interval, 1 second for the configured poll timeout, and 1 ms for
    timestamp resolution. This limit also applies at startup and shutdown.

    Larger gaps are rejected because processing delays or clock changes
    cannot be reliably distinguished from missing observations.
    This validation is independent of Kafka's max.poll.interval.ms.
    """
    def require(condition, reason):
        if not condition:
            raise CoordinationError(reason)

    require(type(start) is int and type(cutoff) is int and start < cutoff, "coverage interval is unavailable")
    if "streams2" in config["commands"]:
        verify_b3_streams_history(coordination, config, start, cutoff)
    reporting_cadence_ms = 1000  # reportPoll's monotonic reporting threshold.
    poll_timeout_ms = 1000       # MeasurementKafkaConsumer.POLL_TIMEOUT.
    timestamp_granularity_tolerance_ms = 1
    envelope = reporting_cadence_ms + poll_timeout_ms + timestamp_granularity_tolerance_ms
    records = coordination.get("evidence", [])
    consumer = [r["evidence"] for r in records if r.get("component") == "consumer"
                and r.get("evidenceType") == "COMPONENT_EVIDENCE"]
    readiness = [e for e in consumer if e.get("evidenceType") == "MEASUREMENT_CONSUMER_READINESS" and e.get("status") == "READY"]
    require(bool(readiness), "Consumer readiness is missing")
    ready_at = readiness[0].get("observedAtEpochMs")
    require(type(ready_at) is int and ready_at <= start, "Consumer was not ready before publication")
    termination = [e for e in consumer if e.get("evidenceType") == "MEASUREMENT_CONSUMER_POLLING_TERMINATED"]
    require(len(termination) == 1, "actual polling termination is missing/ambiguous")
    ended = termination[0].get("observedAtEpochMs")
    require(type(ended) is int and ended >= cutoff and termination[0].get("status") == "REQUESTED_SHUTDOWN"
            and termination[0].get("pollingInvoked") is True and termination[0].get("failure") is None,
            "polling failed or terminated before cutoff")
    for e in consumer:
        kind, status, at = e.get("evidenceType"), e.get("status"), e.get("observedAtEpochMs")
        require(type(at) is int and at >= 0, "invalid Consumer lifecycle timestamp")
        if at < ready_at and kind in {"MEASUREMENT_CONSUMER_CONFIGURATION", "MEASUREMENT_CONSUMER_READINESS", "MEASUREMENT_CONSUMER_POLL_PROGRESS"}:
            continue
        if kind == "MEASUREMENT_CONSUMER_READINESS" and e is not readiness[0] and at >= start:
            raise CoordinationError("Consumer assignment was re-established during observation; continuity requires review")
        if kind in {"MEASUREMENT_CONSUMER_PARTITIONS_LOST", "MEASUREMENT_CONSUMER_PARTITIONS_REVOKED"}:
            require(not any(p.get("topic") == "sensor-results" for p in e.get("affectedPartitions", [])),
                    "result partition assignment was interrupted")
        require(status not in {"FAILED", "CLOSED_WITH_WRITE_FAILURE"} and e.get("failure") is None,
                "Consumer execution or CSV failure invalidated observation")
        if kind in {"MEASUREMENT_CONSUMER_READINESS", "MEASUREMENT_CONSUMER_POLL_PROGRESS"}:
            require(status == "READY", "Consumer readiness/polling coverage was invalidated")
    polls = [e for e in consumer if e.get("evidenceType") == "MEASUREMENT_CONSUMER_POLL_PROGRESS" and e.get("observedAtEpochMs", -1) >= ready_at]
    require(bool(polls), "successful polling progress history is missing")
    previous = ready_at
    for poll in polls:
        at = poll.get("observedAtEpochMs")
        require(type(at) is int and previous <= at <= ended, "poll observation timestamp is invalid/out of order")
        require(at - previous <= envelope, "unsupported polling-evidence gap exceeds the 2001 ms cadence-plus-poll-timeout-and-granularity bound")
        count, empty, records_count = poll.get("successfulPolls"), poll.get("emptyPolls"), poll.get("recordCount")
        require(type(count) is int and count > 0 and type(empty) is int and 0 <= empty <= count
                and type(records_count) is int and records_count >= 0 and type(poll.get("lastPollEmpty")) is bool,
                "polling progress counters are missing/invalid")
        previous = at
    require(ended - previous <= envelope, "missing polling evidence near cutoff/termination: tail exceeds the 2001 ms reporting bound")
    expected_last = coordination.get("lastPollProgress", {}).get("evidence") if coordination.get("lastPollProgress") else None
    require(expected_last == polls[-1], "last polling snapshot does not match the retained history")
    for kind in ("SHUTDOWN_REQUESTED", "PROCESS_EXITED"):
        evidence = [e for e in records if e.get("component") == "consumer" and e.get("evidenceType") == kind]
        require(len(evidence) == 1 and type(evidence[0].get("observedAtEpochMs")) is int
                and evidence[0]["observedAtEpochMs"] >= cutoff, "Consumer supervision/shutdown did not span the cutoff")
        if kind == "PROCESS_EXITED":
            require(evidence[0].get("requested") is True and evidence[0].get("exitCode") ==
                    coordination.get("components", {}).get("consumer", {}).get("exitCode"),
                    "Consumer exit does not match requested supervised shutdown")
    for record in records:
        event = record.get("evidence", {})
        if record.get("component") in streams_roles(config) and event.get("evidenceType") == "KAFKA_STREAMS_STATE_CHANGED":
            at = event.get("observedAtEpochMs")
            require(type(at) is int, "Streams lifecycle timestamp is invalid")
            if start <= at < cutoff and config.get("scenario") != "B4":
                require(event.get("currentState") == "RUNNING" and event.get("status") == "READY",
                        "Streams left RUNNING before the observation cutoff")
    require(not any(e.get("evidenceType") == "FORCED_TERMINATION" for e in records),
            "forced component termination invalidated observation")
    return {"status": "COMPLETE", "definition": "BOUNDED_POLL_REPORT_CONTINUITY",
            "observedFromEpochMs": ready_at, "pollingTerminatedEpochMs": ended,
            "requiredCutoffEpochMs": cutoff,
            "reportingCadenceMs": reporting_cadence_ms, "pollTimeoutMs": poll_timeout_ms,
            "schedulingGraceMs": 0, "timestampGranularityToleranceMs": timestamp_granularity_tolerance_ms,
            "maximumReportingGapMs": envelope,
            "pollProgressRecords": len(polls),
            "limitation": "Bounded operational reporting only; pauses up to the reporting bound and individual missing results remain possible."}


def verify_b3_streams_history(coordination, config, start, cutoff):
    """Require independent startup, continuity and supervised exits for both JVMs."""
    verify_streams_pair(config)
    roles = command_roles(config)
    if set(coordination.get("components", {})) != set(roles):
        raise CoordinationError("B3 component inventory does not match configured processes")
    records = coordination.get("evidence", [])
    producer_launch = next((i for i, e in enumerate(records)
                            if e.get("evidenceType") == "PROCESS_STARTED"
                            and e.get("component") == "producer"), -1)
    for role in streams_roles(config):
        launches = [i for i, e in enumerate(records) if e.get("component") == role
                    and e.get("evidenceType") == "PROCESS_STARTED"]
        if len(launches) != 1 or not launches[0] < producer_launch:
            raise CoordinationError(f"{role}: independent launch before Producer is required")
        launch = records[launches[0]]
        if launch.get("command") != config["commands"][role]:
            raise CoordinationError(f"{role}: recorded launch command conflicts")
        states = [(i, e["evidence"]) for i, e in enumerate(records)
                  if e.get("component") == role and e.get("evidenceType") == "COMPONENT_EVIDENCE"
                  and e.get("evidence", {}).get("evidenceType") == "KAFKA_STREAMS_STATE_CHANGED"]
        before = [(i, e) for i, e in states if launches[0] < i < producer_launch]
        if (not before or before[-1][1].get("currentState") != "RUNNING"
                or before[-1][1].get("status") != "READY"
                or integer(before[-1][1].get("observedAtEpochMs"), "Streams READY timestamp") > start):
            raise CoordinationError(f"{role}: latest state before Producer must be RUNNING/READY")
        for i, event in states:
            at = integer(event.get("observedAtEpochMs"), "Streams state timestamp")
            if (i >= producer_launch and at < cutoff
                    and (event.get("currentState") != "RUNNING" or event.get("status") != "READY")):
                raise CoordinationError(f"{role}: Streams continuity was interrupted")
        for kind in ("SHUTDOWN_REQUESTED", "PROCESS_EXITED"):
            found = [e for e in records if e.get("component") == role and e.get("evidenceType") == kind]
            if len(found) != 1 or integer(found[0].get("observedAtEpochMs"), kind) < cutoff:
                raise CoordinationError(f"{role}: supervised {kind} after cutoff is required")
            if kind == "PROCESS_EXITED":
                code = found[0].get("exitCode")
                if (found[0].get("requested") is not True or type(code) is not int
                        or code != coordination["components"][role].get("exitCode")):
                    raise CoordinationError(f"{role}: exit evidence conflicts with supervised shutdown")


def verify_b4_evidence(coordination, config, timing):
    """Read-only B4 evidence revalidation: no controller instantiation or commands."""
    b4 = validate_b4_settings(config)
    validate_b4_commands(config)
    if coordination.get("b4") != b4:
        raise CoordinationError("persisted B4 configuration differs from launch settings")
    records = coordination.get("evidence", [])
    declarations = [r for r in records if r.get("evidenceType") == "B4_CONFIGURATION"]
    if len(declarations) != 1 or declarations[0].get("b4") != b4:
        raise CoordinationError("one matching B4 configuration declaration required")
    anchor = integer(timing.get("workloadStartEpochMs"), "B4 workload anchor")
    observation = anchor + (60 + b4["observationSecondsAfterWarmUp"]) * 1000
    cutoff = observation + b4["drainSeconds"] * 1000
    timelines = [r for r in records if r.get("evidenceType") == "B4_TIMELINE"]
    expected_timeline = dict(workloadStartEpochMs=anchor, warmUpDeadlineEpochMs=anchor + 60000,
                             failurePlannedEpochMs=anchor + 180000, restartPlannedEpochMs=anchor + 240000,
                             generationDeadlineEpochMs=timing.get("measurementDeadlineEpochMs"),
                             observationDeadlineEpochMs=observation, drainCutoffEpochMs=cutoff)
    if len(timelines) != 1 or any(timelines[0].get(k) != v for k, v in expected_timeline.items()):
        raise CoordinationError("B4 explicit timeline differs from Producer-anchored deadlines")
    reached = [r for r in records if r.get("evidenceType") == "B4_OBSERVATION_DEADLINE_REACHED"]
    if len(reached) != 1 or reached[0].get("scheduledAtEpochMs") != observation:
        raise CoordinationError("B4 active observation deadline was not demonstrably reached")
    if integer(reached[0].get("observedAtEpochMs"), "B4 observation reached") < observation:
        # Legacy monotonic-only markers can precede the epoch deadline. Never
        # correct them or grant timing tolerance: require independent evidence
        # through the full observation AND drain, plus successful Producer exit.
        cutoffs = [r for r in records if r.get("evidenceType") == "OBSERVATION_CUTOFF_REACHED"]
        exits = [r for r in records if r.get("evidenceType") == "PROCESS_EXITED"
                 and r.get("component") == "producer"]
        if (len(cutoffs) != 1 or cutoffs[0].get("drainCutoffEpochMs") != cutoff
                or integer(cutoffs[0].get("observedAtEpochMs"), "B4 cutoff reached") < cutoff
                or len(exits) != 1 or exits[0].get("exitCode") != 0
                or exits[0].get("requested") is not False
                or integer(exits[0].get("observedAtEpochMs"), "B4 Producer exit") < observation):
            raise CoordinationError("B4 early observation marker lacks independent deadline/Producer evidence")
        operational_coverage(coordination, config, anchor, cutoff)
    closed = [r for r in records if r.get("evidenceType") == "B4_WORKER_CLOSED"]
    if len(closed) != 1 or closed[0].get("pendingRestoration") is not False:
        raise CoordinationError("B4 worker closure/restoration is unconfirmed")
    if any(r.get("evidenceType") in {"B4_FAILURE", "B4_OPERATOR_RECOVERY_REQUIRED"} for r in records):
        raise CoordinationError("B4 control or restoration failure recorded")
    events = [r["evidence"] for r in records if r.get("evidenceType") == "B4_CONTROL_EVIDENCE"]
    module = b4_module()
    pins = [e for e in events if e.get("evidenceType") == "B4_DEPLOYMENT_PINNED"]
    if len(pins) != 1 or set(pins[0].get("containers", {})) != set(module.BROKERS.values()):
        raise CoordinationError("B4 three-broker deployment pinning evidence missing")
    if set(pins[0].get("dataVolumes", {})) != set(module.BROKERS.values()):
        raise CoordinationError("B4 data volume identity evidence missing")
    samples = {topic: [] for topic in module.TOPICS}
    latest_output, latest_partitions = {}, {}
    selected, actions, commands = None, [], []
    active_job = None
    operations = []
    for event in events:
        kind = event.get("evidenceType")
        at = integer(event.get("observedAtEpochMs"), "B4 evidence timestamp")
        if kind == "B4_WORKER_DISPATCH":
            if active_job is not None:
                raise CoordinationError("overlapping B4 worker operations")
            active_job = event.get("operation")
            operations.append(active_job)
            if active_job not in {"pin", "sample", "fault", "reference", "restart", "restore"}:
                raise CoordinationError("unknown B4 worker operation")
            if b4["condition"] == "NO_FAILURE" and active_job in {"fault", "restart", "restore"}:
                raise CoordinationError("NO_FAILURE contains a broker action")
        elif kind == "B4_WORKER_COMPLETED":
            if active_job != event.get("operation") or event.get("status") != "SUCCEEDED" or event.get("error"):
                raise CoordinationError("B4 worker operation incomplete or failed")
            active_job = None
        else:
            if (event.get("runId") != config["runId"] or event.get("condition") != b4["condition"]
                    or event.get("composeProject") != b4["composeProject"]
                    or event.get("composeFile") != str(module.COMPOSE_FILE)):
                raise CoordinationError("B4 evidence identity or deployment mismatch")
        if kind in {"B4_METADATA_QUERY_FAILED", "B4_BROKER_ACTION_FAILED", "B4_RESTORATION_FAILED",
                    "B4_RESTORATION_ATTEMPT_FAILED"}:
            raise CoordinationError("B4 control/metadata evidence contains an unverified interval or failure")
        if kind == "B4_COMMAND_RESULT":
            command = event.get("command")
            if not isinstance(command, list) or any(not isinstance(arg, str) for arg in command):
                raise CoordinationError("invalid B4 command evidence")
            commands.append(event)
            if event.get("returnCode") != 0 or event.get("status") != "SUCCEEDED" or event.get("error"):
                raise CoordinationError("failed B4 command evidence")
            requested = integer(event.get("requestedAtEpochMs"), "B4 command request")
            completed = integer(event.get("completedAtEpochMs"), "B4 command completion")
            if not requested <= completed <= at:
                raise CoordinationError("invalid B4 command chronology")
            if "--topic" in command and "--describe" in command:
                topic = command[command.index("--topic") + 1]
                latest_output[topic] = event["stdout"]
        elif kind == "B4_METADATA_SAMPLE":
            topic = event.get("topic")
            if topic not in samples or event.get("status") != "OBSERVED" or event.get("samplingGap"):
                raise CoordinationError("invalid B4 metadata sample or sampling gap")
            partitions = event.get("partitions")
            if not isinstance(partitions, list) or len(partitions) != 6:
                raise CoordinationError("B4 sample lacks six partitions")
            observed = partitions[0].get("observedAtEpochMs")
            parsed = module.parse_topic_metadata(latest_output.get(topic, ""), topic, observed)
            if parsed != partitions or observed > at:
                raise CoordinationError("B4 parsed metadata differs from recorded CLI evidence")
            samples[topic].append(observed)
            latest_partitions[topic] = parsed
        elif kind == "B4_BROKER_SELECTED":
            if selected is not None:
                raise CoordinationError("ambiguous B4 target selection")
            selected = event
            if "sensor-events" not in latest_partitions:
                raise CoordinationError("B4 selection lacks preceding metadata sample")
            expected = latest_partitions["sensor-events"][b4["targetPartition"]]
            broker = event.get("selectedBrokerId")
            if (event.get("selectionMetadata") != expected or broker != expected["leaderBrokerId"]
                    or event.get("selectedService") != module.broker_service(broker)
                    or event.get("partition") != b4["targetPartition"]):
                raise CoordinationError("B4 leader selection conflicts with fresh sensor-events metadata")
        elif kind in {"B4_BROKER_ACTION_REQUESTED", "B4_BROKER_ACTION_CONFIRMED"}:
            actions.append(event)
    if active_job is not None:
        raise CoordinationError("B4 worker operation never completed")
    for topic, times in samples.items():
        if (not times or times[0] > anchor + b4["metadataMaxGapMs"]
                or cutoff - times[-1] > b4["metadataMaxGapMs"]
                or any(not 0 <= later - earlier <= b4["metadataMaxGapMs"]
                       for earlier, later in zip(times, times[1:]))):
            raise CoordinationError(f"B4 {topic}: incomplete metadata sampling coverage")
    expected_selection = "fault" if b4["condition"] == "BROKER_FAILURE" else "reference"
    if (operations.count("pin") != 1 or operations.count(expected_selection) != 1
            or operations.count("restart") != (1 if b4["condition"] == "BROKER_FAILURE" else 0)
            or "sample" not in operations or selected is None):
        raise CoordinationError("B4 scheduled pinning, selection, monitoring or restart is incomplete")
    if (not 0 <= selected["observedAtEpochMs"] - anchor - 180000 <= b4["maxActionDelayMs"]
            or not 0 <= selected["observedAtEpochMs"] - selected["metadataObservedAtEpochMs"] <= b4["metadataMaxAgeMs"]):
        raise CoordinationError("B4 target/reference selection missed its deadline or used stale metadata")
    requests = [e for e in actions if e["evidenceType"] == "B4_BROKER_ACTION_REQUESTED"]
    confirmations = [e for e in actions if e["evidenceType"] == "B4_BROKER_ACTION_CONFIRMED"]
    control_commands = [e for e in commands if any(op in e["command"] for op in ("stop", "start"))]
    if b4["condition"] == "NO_FAILURE":
        if actions or control_commands:
            raise CoordinationError("NO_FAILURE contains broker-stop/restart evidence")
    else:
        if selected is None or [e.get("action") for e in requests] != ["stop", "start"] or len(confirmations) != 2:
            raise CoordinationError("B4 requires exactly one verified stop and restart")
        service, broker = selected["selectedService"], selected["selectedBrokerId"]
        for action, offset, state in (("stop", 180, "exited"), ("start", 240, "running")):
            request = next(e for e in requests if e["action"] == action)
            matches = [e for e in confirmations if e.get("action") == action]
            if len(matches) != 1:
                raise CoordinationError("ambiguous B4 action confirmation")
            confirmed = matches[0]
            planned = anchor + offset * 1000
            delay = request["observedAtEpochMs"] - planned
            if not 0 <= delay <= b4["maxActionDelayMs"]:
                raise CoordinationError("B4 action missed its explicit scheduling tolerance")
            if (request.get("selectedBrokerId") != broker or request.get("selectedService") != service
                    or confirmed.get("selectedBrokerId") != broker or confirmed.get("selectedService") != service
                    or request.get("containerId") != pins[0]["containers"][service]
                    or confirmed.get("containerId") != request["containerId"]
                    or confirmed.get("dataVolume") != pins[0]["dataVolumes"][service]
                    or confirmed.get("containerState") != state or confirmed.get("returnCode") != 0
                    or confirmed.get("status") != "CONFIRMED"
                    or not request["observedAtEpochMs"] <= confirmed["requestedAtEpochMs"] <= confirmed["completedAtEpochMs"] <= confirmed["observedAtEpochMs"]):
                raise CoordinationError("B4 action identity, outcome or chronology is unverified")
            actual = [e for e in control_commands if action in e["command"]]
            expected_command = ["docker", "compose", "--project-name", b4["composeProject"], "-f", str(module.COMPOSE_FILE), action]
            expected_command += ["--timeout", str(b4["stopGraceSeconds"]), service] if action == "stop" else [service]
            if (len(actual) != 1 or actual[0]["command"] != expected_command
                    or actual[0]["requestedAtEpochMs"] != confirmed["requestedAtEpochMs"]
                    or actual[0]["completedAtEpochMs"] != confirmed["completedAtEpochMs"]):
                raise CoordinationError("B4 action confirmation lacks matching service-specific command evidence")
            if action == "stop" and not 0 <= confirmed["requestedAtEpochMs"] - selected["metadataObservedAtEpochMs"] <= b4["metadataMaxAgeMs"]:
                raise CoordinationError("B4 target metadata was stale at broker-stop dispatch")
    # Independent supervised exits remain mandatory even during expected failover.
    for role in ROLES:
        exits = [e for e in records if e.get("evidenceType") == "PROCESS_EXITED" and e.get("component") == role]
        if len(exits) != 1 or exits[0].get("exitCode") != coordination["components"][role].get("exitCode"):
            raise CoordinationError(f"B4 {role}: process exit evidence is missing/conflicting")
        if role != "producer" and (exits[0].get("requested") is not True or exits[0]["observedAtEpochMs"] < cutoff):
            raise CoordinationError(f"B4 {role}: premature/unexpected exit")


def validate_finalization(data, coordination, config):
    """Check recorded facts under the metadata lock; never infer coverage from counts.

    Configuration and operational polling coverage are revalidated from evidence.
    No caller-supplied coverage flag replaces lifecycle and supervision checks.
    """
    checks = []

    def check(name, condition, reason):
        checks.append({"condition": name, "status": "VERIFIED" if condition else "BLOCKED",
                       "reason": None if condition else reason})

    def epoch(value):
        return type(value) is int and 0 <= value < 2**63

    check("scenario", config.get("scenario") in {"B1", "B2", "B3", "B4"}, "explicit B1/B2/B3 scenario required")
    measurement_seconds, drain_seconds = experiment_durations(config)
    if "streams2" in config.get("commands", {}):
        try:
            command_roles(config)
            verify_streams_pair(config)
            check("b3StreamsPair", True, "")
        except (CoordinationError, TypeError, ValueError) as error:
            check("b3StreamsPair", False, str(error))
    configuration = data.get("configuration") or {}
    producer = configuration.get("producer") or {}
    options = {}
    for role in command_roles(config):
        command = config.get("commands", {}).get(role, [])
        options[role] = {command[i][2:]: command[i + 1] for i in range(len(command) - 1)
                         if command[i].startswith("--")}
    expected = options["producer"]
    fields = {"run-id": ("runId", str), "random-seed": ("randomSeed", int),
              "initial-event-timestamp": ("initialEventTimestamp", int),
              "timestamp-increment": ("timestampIncrementMs", int),
              "target-events-per-second": ("targetEventsPerSecond", float),
              "csv-output": ("csvOutput", str), "event-csv-output": ("eventCsvOutput", str),
              "run-config-output": ("runConfigOutput", str)}
    matches = bool(expected)
    try:
        for option, (field, convert) in fields.items():
            matches = matches and option in expected and producer.get(field) == convert(expected[option])
    except (ValueError, TypeError):
        matches = False
    kafka = producer.get("kafkaSettings") or {}
    matches = matches and kafka.get("bootstrap.servers") == expected.get("bootstrap-servers")
    matches = matches and kafka.get("acks") == expected.get("acks", "all")
    matches = matches and kafka.get("enable.idempotence") == expected.get("enable-idempotence", "true")
    check("producerConfiguration", matches, "persisted effective Producer configuration is missing/conflicting")
    for role in streams_roles(config) + ("consumer",):
        try:
            verify_configuration_history(role, coordination, config)
            check(role + "EffectiveConfiguration", True, "")
        except (CoordinationError, TypeError, ValueError) as error:
            check(role + "EffectiveConfiguration", False, str(error))
    durations = data.get("phaseDurationsMs") or {}
    check("ordinaryDurations", durations.get("warmUp") == 60000 and durations.get("measurement") == measurement_seconds * 1000,
          f"persisted warm-up/measurement must be 60000/{measurement_seconds * 1000} ms")
    events = coordination.get("evidence", [])
    component_events = [(e.get("component"), e.get("evidence", {})) for e in events
                        if e.get("evidenceType") == "COMPONENT_EVIDENCE"]
    timing = [e for role, e in component_events if role == "producer" and e.get("evidenceType") == "PRODUCER_WORKLOAD_TIMING"]
    anchors, scheduled = data.get("clockAnchors") or {}, data.get("scheduled") or {}
    t = timing[0] if len(timing) == 1 else {}
    anchor = t.get("workloadStartEpochMs")
    clock_valid = (epoch(anchor) and type(t.get("workloadStartNs")) is int
                   and epoch(t.get("sampleSpanNs"))
                   and all(anchors.get(k) == t.get(k) for k in ("workloadStartNs", "workloadStartEpochMs", "sampleSpanNs"))
                   and t.get("warmUpDurationNs") == 60_000_000_000
                   and t.get("measurementDurationNs") == measurement_seconds * 1_000_000_000
                   and t.get("measurementStartEpochMs") == anchor + 60000
                   and t.get("measurementDeadlineEpochMs") == anchor + (60 + measurement_seconds) * 1000
                   and all(scheduled.get(k) == t.get(k) for k in ("measurementStartEpochMs", "measurementDeadlineEpochMs")))
    check("clockAndBoundaries", bool(clock_valid), "one actual timing event must match persisted anchors and fixed boundaries")
    start = t.get("workloadStartEpochMs")
    for role, kind in (("consumer", "MEASUREMENT_CONSUMER_READINESS"),) + tuple(
            (role, "KAFKA_STREAMS_STATE_CHANGED") for role in streams_roles(config)):
        ready = [(index, e) for index, e in enumerate(events)
                 if e.get("component") == role and e.get("evidence", {}).get("evidenceType") == kind
                 and e["evidence"].get("status") == "READY"]
        launched = next((i for i, e in enumerate(events) if e.get("evidenceType") == "PROCESS_STARTED"
                         and e.get("component") == "producer"), -1)
        valid = any(i < launched and epoch(e["evidence"].get("observedAtEpochMs")) and epoch(start)
                    and e["evidence"]["observedAtEpochMs"] <= start for i, e in ready)
        if role == "consumer":
            valid = valid and any(e["evidence"].get("assignedResultPartitions") and
                sorted(p.get("partition") for p in e["evidence"]["assignedResultPartitions"]) ==
                sorted(e["evidence"].get("expectedResultPartitions", [])) and
                all(p.get("topic") == "sensor-results" and epoch(p.get("startingPosition"))
                    for p in e["evidence"]["assignedResultPartitions"]) for _, e in ready)
        else:
            valid = valid and any(e["evidence"].get("currentState") == "RUNNING" for _, e in ready)
        check(role + "Readiness", bool(valid), f"{role} verified readiness before Producer launch/publication is missing")
    failures = coordination.get("failures", [])
    check("coordinationFailures", not failures, "coordination failed or was interrupted: " + repr(failures))
    invalidations = [e for _, e in component_events if
                    e.get("evidenceType") in {"MEASUREMENT_CONSUMER_PARTITIONS_LOST", "MEASUREMENT_CONSUMER_PARTITIONS_REVOKED"}
                    or e.get("status") == "FAILED" or e.get("failure") is not None
                    or (e.get("evidenceType") == "KAFKA_STREAMS_STATE_CHANGED"
                        and epoch(e.get("observedAtEpochMs")) and epoch(start)
                        and e["observedAtEpochMs"] >= start
                        and e.get("currentState") in {"REBALANCING", "ERROR", "PENDING_ERROR"})]
    if config.get("scenario") == "B4":
        invalidations = [e for e in invalidations if e.get("status") == "FAILED"
                         or e.get("failure") is not None
                         or e.get("currentState") in {"ERROR", "PENDING_ERROR"}]
    check("runtimeContinuity", not invalidations, "recorded assignment/readiness interruption or component failure")
    drain = coordination.get("drain", {})
    deadline = t.get("measurementDeadlineEpochMs")
    cutoff = drain.get("scheduledCutoffEpochMs")
    reached = drain.get("cutoffReachedObservedAtEpochMs")
    check("drainCutoff", epoch(deadline) and epoch(cutoff) and cutoff == deadline + drain_seconds * 1000
          and drain.get("scheduledStartEpochMs") == deadline and epoch(reached) and reached >= cutoff,
          ("B4 generation deadline plus configured drain was not demonstrably reached"
           if config.get("scenario") == "B4" else
           "fixed measurement deadline plus 120000 ms drain was not demonstrably reached"))
    last_poll = (coordination.get("lastPollProgress") or {}).get("evidence", {})
    check("pollProgress", last_poll.get("evidenceType") == "MEASUREMENT_CONSUMER_POLL_PROGRESS"
          and last_poll.get("status") == "READY" and type(last_poll.get("successfulPolls")) is int
          and last_poll["successfulPolls"] > 0, "successful Consumer polling progress is missing")
    coverage = None
    try:
        coverage = operational_coverage(coordination, config, start, cutoff)
        check("continuousObservationCoverage", True, "")
    except (CoordinationError, TypeError, ValueError) as error:
        check("continuousObservationCoverage", False, str(error))
    required = {"consumer": {"MEASUREMENT_CONSUMER_POLLING_TERMINATED": "REQUESTED_SHUTDOWN",
                             "MEASUREMENT_CONSUMER_KAFKA_CLOSED": "CLOSED",
                             "MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED": "CLOSED"},
                **{role: {"KAFKA_STREAMS_CLOSE_RESULT": "CLOSED"} for role in streams_roles(config)}}
    for role, kinds in required.items():
        code = coordination.get("components", {}).get(role, {}).get("exitCode")
        check(role + "Exit", type(code) is int, f"{role} actual process exit is missing")
        for kind, status in kinds.items():
            found = [e for r, e in component_events if r == role and e.get("evidenceType") == kind]
            valid = len(found) == 1 and found[0].get("status") == status and found[0].get("failure") is None
            valid = valid and epoch(found[0].get("observedAtEpochMs")) and epoch(cutoff) and found[0]["observedAtEpochMs"] >= cutoff
            if kind.endswith("POLLING_TERMINATED"):
                valid = valid and found[0].get("pollingInvoked") is True
            check((role + ":" if role == "streams2" else "") + kind, bool(valid),
                  f"{role}: successful actual {kind} after cutoff is missing/conflicting")
    observed = data.get("observed") or {}
    publication_only = (bool(data.get("failures"))
                        and any(f.get("category") == "PUBLICATION_FAILURE" for f in data["failures"])
                        and all(f.get("category") in {"PUBLICATION", "PUBLICATION_FAILURE"} for f in data["failures"]))
    complete = data.get("components", {}).get("producer") == "COMPLETED" or publication_only
    cleanup_at = observed.get("producerCleanupCompletedEpochMs")
    if publication_only and cleanup_at is None:
        # The existing Producer records PUBLICATION_FAILURE after both owned resources close.
        cleanup_at = observed.get("failureObservedEpochMs")
    check("producerCompletion", complete and epoch(cleanup_at) and all(epoch(observed.get(k)) for k in (
        "generationStoppedEpochMs", "publicationsDrainedEpochMs", "eventCsvClosedEpochMs")),
        "Producer generation, publication drain, CSV closure and cleanup evidence is incomplete")
    producer_code = coordination.get("components", {}).get("producer", {}).get("exitCode")
    check("producerExit", type(producer_code) is int and (producer_code == 0 or publication_only),
          "Producer exit is unobserved or failed for a reason other than recorded publication outcomes")
    if config.get("scenario") == "B4":
        try:
            verify_b4_evidence(coordination, config, t)
            check("b4ControlAndCoverage", True, "")
        except (CoordinationError, KeyError, TypeError, ValueError) as error:
            check("b4ControlAndCoverage", False, str(error))
    validate_raw_observations(config, checks)
    blockers = [c["reason"] for c in checks if c["status"] == "BLOCKED"]
    return {"status": "VERIFIED" if not blockers else "BLOCKED", "checks": checks,
            "blockingReasons": blockers, "observationCoverage": "NOT_VERIFIED" if blockers else "COMPLETE",
            "operationalCoverage": coverage}


def validate_raw_observations(config, checks):
    """Validate raw schemas and identities; missing results are not a rejection."""
    producer_header = "runId,eventId,deviceId,phase,recordedAt,sentAt,status,partition,offset,error".split(",")
    result_header = "runId,eventId,deviceId,eventTime,sentAt,ruleType,triggered,resultReceivedAt,latencyMs,duplicate,negativeLatency".split(",")
    attempts, outcomes, results = set(), set(), set()
    producer_values, result_values = {}, {}
    for filename, header in (("producer-events.csv", producer_header), ("sensor-results.csv", result_header)):
        try:
            with (config["outputDirectory"] / filename).open(encoding="utf-8", newline="") as source:
                reader = csv.DictReader(source, strict=True)
                if reader.fieldnames != header:
                    raise ValueError("incorrect CSV schema")
                for row in reader:
                    if None in row or any(v is None for v in row.values()):
                        raise ValueError("incorrect field count")
                    if row["runId"] != config["runId"] or not row["eventId"].strip() or not row["deviceId"].strip():
                        raise ValueError("missing/conflicting observation identity")
                    identity = (row["runId"], row["eventId"])
                    names = ("recordedAt", "sentAt") if filename == "producer-events.csv" else ("eventTime", "sentAt", "resultReceivedAt", "latencyMs")
                    for name in names:
                        if not re.fullmatch(r"-?[0-9]+", row[name]) or not -(2**63) <= int(row[name]) < 2**63:
                            raise ValueError(f"invalid {name}")
                    if any(int(row[name]) < 0 for name in names if name not in {"eventTime", "latencyMs"}):
                        raise ValueError("negative actual epoch timestamp")
                    if filename == "producer-events.csv":
                        values = (row["deviceId"], row["sentAt"], row["phase"])
                        if identity in producer_values and producer_values[identity] != values:
                            raise ValueError("conflicting originating event fields")
                        producer_values[identity] = values
                        if row["status"] == "CONFIRMED" and (not row["partition"] or not row["offset"]):
                            raise ValueError("confirmation metadata is missing")
                        if row["phase"] not in {"WARM_UP", "MEASUREMENT"} or row["status"] not in {"ATTEMPTED", "CONFIRMED", "FAILED"}:
                            raise ValueError("invalid phase/status")
                        (attempts if row["status"] == "ATTEMPTED" else outcomes).add(identity)
                        for name in ("partition", "offset"):
                            if row[name] and not re.fullmatch(r"[0-9]+", row[name]):
                                raise ValueError(f"invalid {name}")
                    else:
                        if any(row[k] not in {"true", "false"} for k in ("triggered", "duplicate", "negativeLatency")):
                            raise ValueError("invalid boolean")
                        if row["ruleType"] not in {"HIGH_TEMPERATURE", "HIGH_FREQUENCY", "RAPID_CHANGE"}:
                            raise ValueError("invalid ruleType")
                        latency = int(row["resultReceivedAt"]) - int(row["sentAt"])
                        if latency != int(row["latencyMs"]) or (latency < 0) != (row["negativeLatency"] == "true"):
                            raise ValueError("inconsistent latency observation")
                        values = (row["deviceId"], row["sentAt"])
                        if identity in result_values and result_values[identity] != values:
                            raise ValueError("conflicting result identity fields")
                        result_values[identity] = values
                        results.add(identity)
            checks.append({"condition": filename, "status": "VERIFIED", "reason": None})
        except (OSError, ValueError, csv.Error, UnicodeError) as error:
            checks.append({"condition": filename, "status": "BLOCKED", "reason": f"{filename}: {error}"})
    association = (outcomes.issubset(attempts) and results.issubset(attempts)
                   and all(producer_values.get(key, ())[:2] == value for key, value in result_values.items()))
    checks.append({"condition": "originatingAttempts", "status": "VERIFIED" if association else "BLOCKED",
                   "reason": None if association else "outcome/result without originating ATTEMPTED identity"})


@contextmanager
def metadata_lock(output, create=False):
    """Same lifetime POSIX record lock/inode convention as Java FileChannel."""
    flags = os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    if create:
        flags |= os.O_CREAT
    descriptor = os.open(str(output) + ".lock", flags, 0o600)
    with os.fdopen(descriptor, "ab") as lock:
        if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
            raise CoordinationError("metadata lock must be a regular file")
        try:
            fcntl.lockf(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CoordinationError("metadata is still owned; handoff rejected") from error
        yield
        # Never delete the lock file: all writers must lock the same inode.


def read_metadata(output):
    descriptor = os.open(output, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "r", encoding="utf-8") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise CoordinationError("metadata must be a regular file")
        return json.load(source, object_pairs_hook=strict_json_object, parse_constant=reject_json_constant)


def write_metadata_atomic(output, data):
    """Caller must hold metadata_lock; no non-atomic overwrite fallback."""
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", prefix=".run-config-",
                                         suffix=".tmp", dir=output.parent, delete=False) as destination:
            temporary = Path(destination.name)
            json.dump(data, destination, indent=2, allow_nan=False)
            destination.write("\n")
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def revalidate_run(path):
    """Explicit completed-run update: reuse original evidence, never start processes.

    Compatibility was checked at the original handoff on this filesystem. Require
    that recorded provenance and reuse the identical lockf mechanism, without
    executing the Java interoperability probe again.
    """
    path = Path(path).absolute()
    output = path.parent.resolve() / path.name
    if output.name != "run-config.json":
        raise CoordinationError("revalidation requires the existing run-config.json")
    with metadata_lock(output):
        data = read_metadata(output)
        if not isinstance(data, dict) or data.get("schemaVersion") != 1:
            raise CoordinationError("invalid existing run metadata schema")
        if data.get("experimentStatus") != "NOT_FINALIZED":
            raise CoordinationError("only NOT_FINALIZED runs may be revalidated; FINALIZED runs are immutable")
        previous = data.get("finalization")
        if not isinstance(previous, dict) or previous.get("status") != "BLOCKED":
            raise CoordinationError("previous BLOCKED finalization evidence is required")
        history = data.get("revalidationHistory", [])
        if not isinstance(history, list):
            raise CoordinationError("invalid revalidation history")
        run_id = text(data.get("runId"), "runId")
        try:
            coordination = data["coordination"]
            ownership = coordination["ownership"]
            if (ownership.get("owner") != "COORDINATOR"
                    or ownership.get("locking") != "POSIX_RECORD_LOCK_JAVA_INTEROPERABILITY_VERIFIED"):
                raise CoordinationError("recorded compatible coordinator ownership is required")
            roles = command_roles({"scenario": data.get("scenario"),
                                   "commands": coordination["components"]})
            commands, exits = {}, {}
            for event in coordination["evidence"]:
                kind, role = event.get("evidenceType"), event.get("component")
                if kind == "PROCESS_STARTED":
                    if role not in roles or role in commands:
                        raise CoordinationError("ambiguous component launch evidence")
                    command = event.get("command")
                    if not isinstance(command, list) or not command or any(not isinstance(arg, str) for arg in command):
                        raise CoordinationError("invalid component launch arguments")
                    commands[role] = command
                elif kind == "PROCESS_EXITED":
                    if role not in roles or role in exits or type(event.get("exitCode")) is not int:
                        raise CoordinationError("ambiguous component exit evidence")
                    exits[role] = event["exitCode"]
            if set(commands) != set(roles) or set(exits) != set(roles):
                raise CoordinationError("all original component launches and exits must be recorded")
            for role in roles:
                component = coordination["components"][role]
                if component.get("status") != "EXIT_OBSERVED" or component.get("exitCode") != exits[role]:
                    raise CoordinationError(f"{role}: completion evidence conflicts with recorded exit")
            producer = data["configuration"]["producer"]
            if producer.get("runId") != run_id:
                raise CoordinationError("Producer runId conflicts with existing metadata")
            for recorded, expected in ((producer["runConfigOutput"], output),
                                       (producer["eventCsvOutput"], output.parent / "producer-events.csv"),
                                       (coordination["configurationEvidence"]["consumer"]["resultCsvOutput"],
                                        output.parent / "sensor-results.csv")):
                if not os.path.samefile(recorded, expected):
                    raise CoordinationError("recorded input paths differ from the existing run")
            config = {"runId": run_id, "scenario": data.get("scenario"),
                      "outputDirectory": output.parent, "commands": commands}
            if config["scenario"] == "B4":
                config["b4"] = coordination["b4"]
                config["shutdownTimeoutSeconds"] = coordination["b4ShutdownTimeoutSeconds"]
            verify_streams_pair(config)
            validation = validate_finalization(data, coordination, config)
        except (KeyError, TypeError, AttributeError) as error:
            raise CoordinationError(f"incomplete completed-run evidence: {error}") from error
        if validation["status"] != "VERIFIED":
            raise CoordinationError("revalidation BLOCKED: " + "; ".join(validation["blockingReasons"]))
        observed = time.time_ns() // 1_000_000
        data["revalidationHistory"] = history + [{
            "operation": "REVALIDATE_COMPLETED_RUN", "observedAtEpochMs": observed,
            "previousExperimentStatus": "NOT_FINALIZED", "previousFinalization": previous,
            "newCriterion": validation["operationalCoverage"], "outcome": "VERIFIED",
            "newExperimentStatus": "FINALIZED"}]
        data["finalization"] = dict(validation, observedAtEpochMs=observed)
        coordination["observationCoverage"] = validation["observationCoverage"]
        data["experimentStatus"] = "FINALIZED"
        write_metadata_atomic(output, data)
        return validation


def persist_coordination(config, processes, coordination, verify=verify_lock_compatibility):
    """Acquire ownership only after observed Producer exit; preserve its snapshot."""
    if processes.poll("producer") is None:
        raise CoordinationError("metadata handoff requires an exited Producer")
    output = config["outputDirectory"] / "run-config.json"
    # Verification happens after exit and before claiming ownership.
    verify(output.parent)
    with metadata_lock(output, create=True):
        data = read_metadata(output)
        if not isinstance(data, dict) or data.get("runId") != config["runId"]:
            raise CoordinationError("metadata runId does not match coordinator")
        if data.get("schemaVersion") != 1 or data.get("experimentStatus") != "NOT_FINALIZED":
            raise CoordinationError("metadata schema/status is incompatible or already finalized")
        configuration, components = data.get("configuration"), data.get("components")
        producer = configuration.get("producer") if isinstance(configuration, dict) else None
        if (not isinstance(producer, dict) or producer.get("runId") != config["runId"]
                or not isinstance(components, dict)
                or components.get("producer") not in {"RUNNING", "COMPLETED", "FAILED", "INTERRUPTED"}
                or not isinstance(data.get("observed"), dict)
                or not isinstance(data.get("phaseDurationsMs"), dict)
                or not isinstance(data.get("failures"), list)):
            raise CoordinationError("expected Producer metadata is missing or malformed")
        if any(key in data for key in ("coordination", "finalization", "ownership")):
            raise CoordinationError("existing coordinator ownership/finalization conflicts with handoff")
        if "scenario" in data and data["scenario"] != config["scenario"]:
            raise CoordinationError("metadata scenario conflicts with coordinator")
        data["scenario"] = config["scenario"]
        if config["scenario"] == "B4":
            coordination["b4"] = dict(config["b4"])
            coordination["b4ShutdownTimeoutSeconds"] = config["shutdownTimeoutSeconds"]
        data["coordination"] = dict(coordination, ownership={
            "owner": "COORDINATOR", "handoffObservedAtEpochMs": processes.epoch_ms(),
            "producerExitCode": processes.poll("producer"),
            "locking": "POSIX_RECORD_LOCK_JAVA_INTEROPERABILITY_VERIFIED"})
        validation = validate_finalization(data, data["coordination"], config)
        data["finalization"] = dict(validation, observedAtEpochMs=processes.epoch_ms())
        data["experimentStatus"] = "FINALIZED" if validation["status"] == "VERIFIED" else "NOT_FINALIZED"
        data["coordination"]["observationCoverage"] = validation["observationCoverage"]
        write_metadata_atomic(output, data)
        return validation

class Processes:
    """POSIX pipe multiplexing: continuously drain both streams, preserving bytes."""
    def __init__(self, directory):
        self.directory = directory
        self.selector = selectors.DefaultSelector()
        self.children, self.logs, self.buffers = {}, [], {}

    def now(self):
        return time.monotonic()

    def epoch_ms(self):
        return time.time_ns() // 1_000_000

    def start(self, role, command):
        logs = {stream: (self.directory / f"{role}.{stream}.log").open("xb") for stream in ("stdout", "stderr")}
        self.logs.extend(logs.values())
        child = subprocess.Popen(command, cwd=Path(__file__).resolve().parents[1], shell=False,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        self.children[role] = child
        try:
            for stream in ("stdout", "stderr"):
                pipe = getattr(child, stream)
                os.set_blocking(pipe.fileno(), False)
                self.selector.register(pipe, selectors.EVENT_READ, (role, stream, logs[stream]))
                self.buffers[pipe] = b""
        except Exception:
            self.signal(role, force=True)
            child.wait(timeout=5)
            raise

    def pump(self, timeout):
        events = []
        for key, _ in self.selector.select(max(0, timeout)):
            role, stream, log = key.data
            chunk = os.read(key.fileobj.fileno(), 65536)
            log.write(chunk)
            log.flush()
            buffer = self.buffers[key.fileobj] + chunk
            if len(buffer) > 1_048_576 and b"\n" not in buffer:
                raise CoordinationError(f"{role}: oversized output line")
            parts = buffer.split(b"\n")
            self.buffers[key.fileobj] = parts.pop()
            if not chunk:
                if self.buffers[key.fileobj]:
                    parts.append(self.buffers[key.fileobj])
                del self.buffers[key.fileobj]
                self.selector.unregister(key.fileobj)
                key.fileobj.close()
            events.extend((role, stream, line.decode("utf-8", errors="strict")) for line in parts)
        return events

    def poll(self, role):
        child = self.children.get(role)
        return child.poll() if child else None

    def signal(self, role, force=False):
        child = self.children.get(role)
        if child and child.poll() is None:
            try:
                os.killpg(child.pid, signal.SIGKILL if force else signal.SIGTERM)
            except ProcessLookupError:
                pass

    def pending(self):
        return bool(self.selector.get_map())

    def close(self):
        for key in list(self.selector.get_map().values()):
            key.fileobj.close()
        self.selector.close()
        for log in self.logs:
            log.close()


class Coordinator:
    def __init__(self, config, processes, journal, errors):
        self.config, self.processes, self.journal, self.errors = config, processes, journal, errors
        self.started, self.requested, self.exits = set(), set(), {}
        self.consumer_ready = self.streams_ready = False
        self.streams_readiness = {role: False for role in streams_roles(config)}
        self.timing = None
        self.cutoff = None
        self.closure_evidence = set()
        self.metadata_events = []
        self.configuration_evidence = {}

    def record(self, kind, **fields):
        event = dict(evidenceType=kind, observedAtEpochMs=self.processes.epoch_ms(), **fields)
        self.metadata_events.append(event)
        if kind == "COMPONENT_EVIDENCE" and fields["evidence"].get("evidenceType") == "MEASUREMENT_CONSUMER_POLL_PROGRESS":
            self.last_poll = event
        self.journal.write(json.dumps(event) + "\n")
        self.journal.flush()

    def launch(self, role):
        self.processes.start(role, self.config["commands"][role])
        self.started.add(role)
        self.record("PROCESS_STARTED", component=role, command=self.config["commands"][role])

    def handle(self, role, stream, line):
        if stream == "stderr":
            self.errors.write(f"[{role}] {line}\n")
            self.errors.flush()
            if "evidence" in line.lower() and any(word in line.lower() for word in ("failed", "could not")):
                raise CoordinationError(f"{role}: evidence output failure: {line}")
            return
        if not line.lstrip().startswith("{"):
            return
        try:
            event = json.loads(line, object_pairs_hook=strict_json_object, parse_constant=reject_json_constant)
            kind = text(event.get("evidenceType"), "evidenceType")
        except (ValueError, AttributeError) as error:
            raise CoordinationError(f"{role}: malformed evidence") from error
        self.record("COMPONENT_EVIDENCE", component=role, evidence=event)
        if kind in CONFIGURATION_TYPES.values():
            validate_configuration(event, role, self.config)
            previous = self.configuration_evidence.get(role)
            if previous is not None and configuration_signature(previous) != configuration_signature(event):
                raise CoordinationError(f"{role}: contradictory repeated configuration evidence")
            self.configuration_evidence.setdefault(role, event)
            return
        if role == "producer":
            if kind != "PRODUCER_WORKLOAD_TIMING" or self.timing is not None:
                raise CoordinationError("unexpected or repeated Producer timing evidence")
            if event.get("runId") != self.config["runId"]:
                raise CoordinationError("Producer timing runId mismatch")
            for name in ("workloadStartEpochMs", "sampleSpanNs", "warmUpDurationNs", "measurementDurationNs",
                         "measurementStartEpochMs", "measurementDeadlineEpochMs"):
                integer(event.get(name), name)
            if type(event.get("workloadStartNs")) is not int:
                raise CoordinationError("workloadStartNs is required")
            anchor = event["workloadStartEpochMs"]
            if anchor > self.processes.epoch_ms():
                raise CoordinationError("Producer clock anchor is later than receipt; clock consistency requires review")
            measurement_seconds, drain_seconds = experiment_durations(self.config)
            if (event["warmUpDurationNs"] != 60_000_000_000 or event["measurementDurationNs"] != measurement_seconds * 1_000_000_000
                    or event["measurementStartEpochMs"] != anchor + 60_000
                    or event["measurementDeadlineEpochMs"] != anchor + (60 + measurement_seconds) * 1000):
                raise CoordinationError("Producer timing conflicts with explicit B4 generation intervals"
                                        if self.config["scenario"] == "B4" else
                                        "Producer timing does not describe ordinary B1-B3 intervals")
            self.timing = event
            self.cutoff = event["measurementDeadlineEpochMs"] + drain_seconds * 1000
            remaining = (self.cutoff - self.processes.epoch_ms()) / 1000
            if remaining <= 0:
                raise CoordinationError("Producer timing arrived after the observation cutoff")
            self.stop_at = self.processes.now() + remaining
            self.record("OBSERVATION_CUTOFF_ESTABLISHED", measurementStartEpochMs=event["measurementStartEpochMs"],
                        measurementDeadlineEpochMs=event["measurementDeadlineEpochMs"], drainCutoffEpochMs=self.cutoff)
            return
        integer(event.get("observedAtEpochMs"), "observedAtEpochMs")
        status = text(event.get("status"), "status")
        if event.get("status") == "READY":
            configuration = self.configuration_evidence.get(role)
            if configuration is None or event["observedAtEpochMs"] < configuration["observedAtEpochMs"]:
                raise CoordinationError(f"{role}: READY requires preceding valid configuration evidence")
        options = self.config["commands"][role]
        identity_name = "groupId" if role == "consumer" else "applicationId"
        option = "--group-id" if role == "consumer" else "--application-id"
        if event.get(identity_name) != options[options.index(option) + 1]:
            raise CoordinationError(f"{role}: evidence identity mismatch")
        if role == "consumer":
            if kind == "MEASUREMENT_CONSUMER_READINESS":
                if status not in {"READY", "NOT_READY"}:
                    raise CoordinationError("invalid Consumer readiness status")
                expected, assigned = event.get("expectedResultPartitions"), event.get("assignedResultPartitions")
                if (not isinstance(expected, list) or not expected or any(type(p) is not int or p < 0 for p in expected)
                        or len(set(expected)) != len(expected) or not isinstance(assigned, list)):
                    raise CoordinationError("invalid Consumer assignment evidence")
                partitions = []
                for position in assigned:
                    if not isinstance(position, dict) or position.get("topic") != "sensor-results":
                        raise CoordinationError("invalid result partition evidence")
                    partitions.append(integer(position.get("partition"), "partition"))
                    integer(position.get("startingPosition"), "startingPosition")
                self.consumer_ready = status == "READY" and sorted(partitions) == sorted(expected)
                if status == "READY" and not self.consumer_ready:
                    raise CoordinationError("partial assignment cannot establish full readiness")
            elif kind in {"MEASUREMENT_CONSUMER_PARTITIONS_REVOKED", "MEASUREMENT_CONSUMER_PARTITIONS_LOST"}:
                if any(p.get("topic") == "sensor-results" for p in event.get("affectedPartitions", [])):
                    self.consumer_ready = False
                    if self.config["scenario"] != "B4":
                        raise CoordinationError("result assignment interrupted; coverage requires later review")
            elif kind == "MEASUREMENT_CONSUMER_POLL_PROGRESS":
                if status not in {"READY", "NOT_READY"}:
                    raise CoordinationError("invalid Consumer polling status")
                if status != "READY":
                    self.consumer_ready = False
                    if "producer" in self.started and self.config["scenario"] != "B4":
                        raise CoordinationError("Consumer readiness was invalidated during workload")
            elif kind in {"MEASUREMENT_CONSUMER_POLLING_TERMINATED", "MEASUREMENT_CONSUMER_KAFKA_CLOSED",
                          "MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED"}:
                if role not in self.requested or status not in {"REQUESTED_SHUTDOWN", "CLOSED", "NOT_STARTED"}:
                    raise CoordinationError(f"unexpected Consumer termination/cleanup: {status}")
                self.closure_evidence.add((role, kind))
            else:
                raise CoordinationError("unknown Consumer evidence type")
        else:
            if kind == "KAFKA_STREAMS_STATE_CHANGED":
                if status not in {"READY", "NOT_READY", "FAILED"}:
                    raise CoordinationError("invalid Streams lifecycle status")
                if event.get("currentState") in {"ERROR", "PENDING_ERROR"}:
                    raise CoordinationError("Streams runtime failure state")
                self.streams_readiness[role] = status == "READY" and event.get("currentState") == "RUNNING"
                self.streams_ready = all(self.streams_readiness.values())
                if status == "READY" and not self.streams_readiness[role]:
                    raise CoordinationError("Streams READY without RUNNING")
                if status == "FAILED":
                    raise CoordinationError(f"{role}: Streams runtime failure")
                if ("streams2" in self.streams_readiness and "producer" in self.started
                        and role not in self.requested and not self.streams_readiness[role]):
                    raise CoordinationError(f"{role}: Streams left RUNNING during workload")
            elif kind == "KAFKA_STREAMS_STARTUP":
                self.streams_readiness[role] = False
                self.streams_ready = False
                if status != "NOT_READY":
                    raise CoordinationError("premature Streams readiness")
                if "streams2" in self.streams_readiness and "producer" in self.started:
                    raise CoordinationError(f"{role}: unexpected startup during workload")
            elif kind in {"KAFKA_STREAMS_SHUTDOWN_REQUESTED", "KAFKA_STREAMS_CLOSE_RESULT"}:
                if role not in self.requested or status not in {"SHUTDOWN_REQUESTED", "CLOSED"}:
                    raise CoordinationError(f"unexpected Streams shutdown/cleanup: {status}")
                if kind == "KAFKA_STREAMS_CLOSE_RESULT":
                    self.closure_evidence.add((role, kind))
            else:
                raise CoordinationError("Streams failure or unknown evidence")

    def step(self, timeout=0.1, stopping=False):
        for role, stream, line in self.processes.pump(timeout):
            self.handle(role, stream, line)
        for role in self.started:
            code = self.processes.poll(role)
            if code is not None and role not in self.exits:
                self.exits[role] = code
                self.record("PROCESS_EXITED", component=role, exitCode=code, requested=role in self.requested)
                if role not in self.requested and role != "producer":
                    raise CoordinationError(f"unexpected {role} exit: {code}")

    def wait_for(self, predicate, description):
        deadline = self.processes.now() + self.config["startupTimeoutSeconds"]
        while not predicate():
            if self.processes.now() >= deadline:
                raise CoordinationError(f"startup timeout waiting for {description}")
            self.step(min(0.1, deadline - self.processes.now()))

    def shutdown(self):
        for role in ("producer", "consumer") + streams_roles(self.config):
            if role in self.started and self.processes.poll(role) is None:
                self.requested.add(role)
                self.record("SHUTDOWN_REQUESTED", component=role)
                self.processes.signal(role)
        deadline = self.processes.now() + self.config["shutdownTimeoutSeconds"]
        failures = []
        while (any(self.processes.poll(role) is None for role in self.started) or self.processes.pending()) and self.processes.now() < deadline:
            try:
                self.step(min(0.1, deadline - self.processes.now()), stopping=True)
            except (CoordinationError, OSError, UnicodeError) as error:
                failures.append(str(error))
        for role in self.started:
            if self.processes.poll(role) is None:
                self.record("FORCED_TERMINATION", component=role)
                failures.append(f"{role}: forced termination after shutdown timeout")
                self.processes.signal(role, force=True)
        # Bounded reaping/draining after forced termination.
        reap_deadline = self.processes.now() + self.config["shutdownTimeoutSeconds"]
        while (any(self.processes.poll(role) is None for role in self.started) or self.processes.pending()) and self.processes.now() < reap_deadline:
            try:
                self.step(0.1, stopping=True)
            except (CoordinationError, OSError, UnicodeError) as error:
                failures.append(str(error))
        try:
            self.step(0, stopping=True)
        except (CoordinationError, OSError, UnicodeError) as error:
            failures.append(str(error))
        if any(self.processes.poll(role) is None for role in self.started) or self.processes.pending():
            failures.append("process exit/output draining could not be confirmed")
        required = {
            "consumer": {"MEASUREMENT_CONSUMER_POLLING_TERMINATED", "MEASUREMENT_CONSUMER_KAFKA_CLOSED",
                         "MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED"},
            **{role: {"KAFKA_STREAMS_CLOSE_RESULT"} for role in streams_roles(self.config)},
        }
        for role, evidence in required.items():
            if role in self.requested and not {(role, kind) for kind in evidence}.issubset(self.closure_evidence):
                failures.append(f"{role}: successful cleanup evidence is incomplete; exit code alone is insufficient")
        return failures

    def run(self):
        failures = []
        validation = None
        self.record("SUPERVISION_STARTED", runId=self.config["runId"], scenario=self.config["scenario"],
                    experimentStatus="NOT_FINALIZED")
        try:
            self.launch("consumer")
            self.wait_for(lambda: self.consumer_ready, "Consumer READY")
            for role in streams_roles(self.config):
                self.launch(role)
            self.wait_for(lambda: self.consumer_ready and self.streams_ready, "Streams RUNNING and Consumer READY")
            self.launch("producer")
            self.wait_for(lambda: self.timing is not None, "Producer workload timing")
            while True:
                # The one-time epoch/monotonic conversion may drift. Both real
                # clocks must reach their unchanged deadlines before shutdown.
                remaining = max(self.stop_at - self.processes.now(),
                                (self.cutoff - self.processes.epoch_ms()) / 1000)
                if remaining <= 0:
                    break
                self.step(min(0.1, remaining))
            self.record("OBSERVATION_CUTOFF_REACHED", drainCutoffEpochMs=self.cutoff)
            if self.processes.poll("producer") is None:
                raise CoordinationError("Producer has not completed at the observation cutoff")
        except (Exception, KeyboardInterrupt) as error:
            failures.append(f"{type(error).__name__}: {error}")
        finally:
            failures.extend(self.shutdown())
        if "producer" in self.started:
            try:
                validation = persist_coordination(self.config, self.processes, {
                    "status": "INCOMPLETE" if failures else "STOPPED",
                    "observationCoverage": "NOT_VERIFIED",
                    "finishedObservedAtEpochMs": self.processes.epoch_ms(),
                    "drain": {"scheduledStartEpochMs": self.timing["measurementDeadlineEpochMs"] if self.timing else None,
                              "scheduledCutoffEpochMs": self.cutoff,
                              "cutoffReachedObservedAtEpochMs": next((e["observedAtEpochMs"] for e in self.metadata_events
                                  if e["evidenceType"] == "OBSERVATION_CUTOFF_REACHED"), None)},
                    "components": {role: {"exitCode": self.exits.get(role),
                                          "status": "EXIT_OBSERVED" if role in self.exits else "NOT_RECORDED"}
                                   for role in command_roles(self.config)},
                    "evidence": self.metadata_events,
                    "lastPollProgress": getattr(self, "last_poll", None),
                    "configurationEvidence": dict(self.configuration_evidence),
                    "failures": list(failures), "journal": "coordination.jsonl"})
                self.record("METADATA_HANDOFF_COMPLETED", experimentStatus="FINALIZED" if validation["status"] == "VERIFIED" else "NOT_FINALIZED")
                self.record("FINALIZATION_VALIDATED", **validation)
            except (Exception, KeyboardInterrupt) as error:
                failures.append(f"metadata handoff failed: {type(error).__name__}: {error}")
        for failure in failures:
            self.errors.write(f"[coordinator] {failure}\n")
            self.record("SUPERVISION_FAILURE", message=failure)
        self.errors.flush()
        self.record("SUPERVISION_FINISHED", outcome="FAILED" if failures else "STOPPED",
                    experimentStatus="FINALIZED" if validation and validation["status"] == "VERIFIED" else "NOT_FINALIZED",
                    observationCoverage=validation["observationCoverage"] if validation else "NOT_VERIFIED", exitCodes=self.exits)
        return 1 if failures else 0


class B4Worker:
    """One owned non-daemon worker; only the coordinator thread writes journals."""
    def __init__(self, config):
        self.config = config
        self.events = queue.Queue()
        self.jobs = queue.Queue(maxsize=1)
        self.busy = False
        b4 = config["b4"]
        self.control = b4_module().BrokerControl(config["runId"], b4["condition"], b4["composeProject"],
                                                b4["commandTimeoutSeconds"], emit=self.events.put)
        self.thread = threading.Thread(target=self._work, name="b4-control", daemon=False)
        self.thread.start()

    def submit(self, operation, scheduled_epoch=None):
        if self.busy:
            raise CoordinationError("conflicting B4 worker operations")
        self.busy = True
        self.jobs.put_nowait((operation, scheduled_epoch))

    def _work(self):
        while True:
            job = self.jobs.get()
            if job is None:
                return
            operation, scheduled = job
            b4 = self.config["b4"]
            now = time.time_ns() // 1_000_000
            if operation in {"fault", "restart"} and scheduled is not None:
                # The coordinator's monotonic/epoch mapping can wake us early.
                # Recheck epoch after every sleep, including partial wake-ups.
                while now < scheduled:
                    time.sleep((scheduled - now) / 1000)
                    now = time.time_ns() // 1_000_000
            self.events.put(dict(evidenceType="B4_WORKER_DISPATCH", operation=operation,
                                 observedAtEpochMs=now, scheduledAtEpochMs=scheduled,
                                 schedulingDelayMs=None if scheduled is None else now - scheduled))
            error = None
            try:
                if operation in {"fault", "restart", "restore"} and b4["condition"] != "BROKER_FAILURE":
                    raise CoordinationError("NO_FAILURE cannot dispatch broker actions")
                if operation in {"fault", "restart"} and (scheduled is None
                        or not 0 <= now - scheduled <= b4["maxActionDelayMs"]):
                    raise CoordinationError("B4 broker action missed its declared scheduling tolerance")
                if operation == "pin":
                    self.control.pin_deployment()
                elif operation == "sample":
                    for topic in ("sensor-events", "sensor-results", "sensor-alerts"):
                        self.control.sample_metadata(topic, b4["metadataMaxGapMs"])
                elif operation in {"fault", "reference"}:
                    self.control.select_leader(b4["targetPartition"], b4["metadataMaxAgeMs"], b4["metadataMaxGapMs"])
                    if operation == "fault":
                        self.control.stop_selected(b4["stopGraceSeconds"])
                elif operation == "restart":
                    self.control.start_selected()
                elif operation == "restore":
                    record = self.control.restore_selected(b4["restorationAttempts"])
                    if record.get("status") == "FAILED":
                        raise CoordinationError("B4 broker restoration failed; manual inspection required")
                else:
                    raise CoordinationError("unknown B4 worker operation")
            except Exception as failure:
                error = f"{type(failure).__name__}: {failure}"
            self.events.put(dict(evidenceType="B4_WORKER_COMPLETED", operation=operation,
                                 observedAtEpochMs=time.time_ns() // 1_000_000,
                                 status="SUCCEEDED" if error is None else "FAILED", error=error))

    def drain(self):
        records = []
        while True:
            try:
                event = self.events.get_nowait()
            except queue.Empty:
                return records
            if event["evidenceType"] == "B4_WORKER_COMPLETED":
                self.busy = False
            records.append(event)

    def close(self):
        if self.busy:
            raise CoordinationError("B4 operation still in progress; restoration requires manual inspection")
        self.jobs.put_nowait(None)
        self.thread.join(timeout=1)
        if self.thread.is_alive():
            raise CoordinationError("B4 worker termination was not confirmed")


class B4Coordinator(Coordinator):
    """B4-only scheduling; application supervision stays in Coordinator."""
    def __init__(self, config, processes, journal, errors):
        super().__init__(config, processes, journal, errors)
        self.worker = None
        self.b4_failures = []
        self.b4_stopping = False
        self.b4_fault_submitted = self.b4_restart_submitted = False
        self.b4_observation_reached = False
        self.b4_timeline_recorded = False
        self.next_sample = None

    def run(self):
        self.record("B4_CONFIGURATION", b4=dict(self.config["b4"]),
                    timingInterpretation="480 seconds of active observation AFTER 60-second warm-up; review required")
        try:
            self.worker = B4Worker(self.config)
            self.worker.submit("pin")
            self.wait_for(lambda: not self.worker.busy, "B4 deployment pinning")
        except (Exception, KeyboardInterrupt) as error:
            self.b4_failures.append(f"B4 preflight failed: {error}")
            failures = self.shutdown()
            for failure in failures:
                self.record("B4_FAILURE", message=failure)
                self.errors.write(f"[b4] {failure}\n")
            return 1
        return super().run()

    def step(self, timeout=0.1, stopping=False):
        super().step(timeout, stopping)
        if self.worker is None:
            return
        for event in self.worker.drain():
            self.record("B4_CONTROL_EVIDENCE", evidence=event)
            if event["evidenceType"] == "B4_WORKER_COMPLETED" and event["status"] != "SUCCEEDED":
                self.b4_failures.append(event["error"])
                if not self.b4_stopping:
                    raise CoordinationError(event["error"])
        if self.b4_stopping or self.timing is None:
            return
        b4 = self.config["b4"]
        anchor = self.timing["workloadStartEpochMs"]
        if not self.b4_timeline_recorded:
            self.record("B4_TIMELINE", workloadStartEpochMs=anchor,
                        warmUpDeadlineEpochMs=anchor + 60000,
                        failurePlannedEpochMs=anchor + 180000,
                        restartPlannedEpochMs=anchor + 240000,
                        generationDeadlineEpochMs=self.timing["measurementDeadlineEpochMs"],
                        observationDeadlineEpochMs=anchor + (60 + b4["observationSecondsAfterWarmUp"]) * 1000,
                        drainCutoffEpochMs=self.cutoff)
            self.b4_timeline_recorded = True
        now = self.processes.now()
        # Reuse the Producer-derived monotonic/epoch mapping already established by handle().
        elapsed = now - (self.stop_at - (self.cutoff - anchor) / 1000)
        observation = 60 + b4["observationSecondsAfterWarmUp"]
        observation_epoch = anchor + observation * 1000
        observed_epoch = self.processes.epoch_ms()
        # As at the drain cutoff, the initial clock mapping is only an estimate.
        # Both clocks must reach the deadline before recording it.
        if elapsed >= observation and observed_epoch >= observation_epoch and not self.b4_observation_reached:
            self.record("B4_OBSERVATION_DEADLINE_REACHED", scheduledAtEpochMs=observation_epoch,
                        schedulingDelayMs=observed_epoch - observation_epoch)
            self.b4_observation_reached = True
        if self.worker.busy:
            return
        failure_offset = 60 + b4["preFailureSeconds"]
        restart_offset = failure_offset + b4["interruptionSeconds"]
        if elapsed >= failure_offset and not self.b4_fault_submitted:
            operation = "fault" if b4["condition"] == "BROKER_FAILURE" else "reference"
            self.worker.submit(operation, anchor + failure_offset * 1000)
            self.b4_fault_submitted = True
            return
        if b4["condition"] == "BROKER_FAILURE":
            if elapsed >= restart_offset and not self.b4_restart_submitted:
                if not self.b4_fault_submitted:
                    raise CoordinationError("B4 restart cannot precede fault")
                self.worker.submit("restart", anchor + restart_offset * 1000)
                self.b4_restart_submitted = True
                return
        if self.next_sample is None or now >= self.next_sample:
            self.worker.submit("sample")
            self.next_sample = now + b4["metadataIntervalSeconds"]

    def shutdown(self):
        self.b4_stopping = True
        failures = []
        if self.worker is not None:
            deadline = time.monotonic() + self.config["shutdownTimeoutSeconds"]
            def wait_worker():
                while self.worker.busy and time.monotonic() < deadline:
                    try:
                        self.step(0.05, stopping=True)
                    except (CoordinationError, OSError, UnicodeError) as error:
                        failures.append(str(error))
                if self.worker.busy:
                    raise CoordinationError("B4 worker cleanup deadline exceeded; manual broker inspection required")
            try:
                wait_worker()  # Never race a restart against an in-flight stop.
                if self.worker.control.pending_restore:
                    self.worker.submit("restore")
                    wait_worker()
                if self.worker.control.pending_restore:
                    raise CoordinationError("B4 restoration unconfirmed; operator recovery required")
                self.worker.close()
                self.record("B4_WORKER_CLOSED", pendingRestoration=self.worker.control.pending_restore)
            except (CoordinationError, OSError) as error:
                failures.append(str(error))
                if not self.worker.busy:
                    try:
                        self.worker.close()
                    except CoordinationError as close_error:
                        failures.append(str(close_error))
                self.record("B4_OPERATOR_RECOVERY_REQUIRED", message=str(error))
        failures.extend(self.b4_failures)
        failures.extend(super().shutdown())
        return failures


def execute(config, processes_factory=Processes):
    config = validate(config)
    directory = config["outputDirectory"]
    directory.mkdir(parents=True, exist_ok=False)
    with (directory / "coordination.jsonl").open("x", encoding="utf-8") as journal, \
            (directory / "errors.log").open("x", encoding="utf-8") as errors:
        processes = processes_factory(directory)
        try:
            coordinator_type = B4Coordinator if config["scenario"] == "B4" else Coordinator
            return coordinator_type(config, processes, journal, errors).run()
        finally:
            processes.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--config", type=Path, help="launch and supervise a new experiment")
    operation.add_argument("--revalidate-run", type=Path, help="revalidate and atomically finalize an existing BLOCKED run; starts no processes")
    args = parser.parse_args(argv)
    try:
        if args.revalidate_run is not None:
            revalidate_run(args.revalidate_run)
            print(f"Revalidation persisted: FINALIZED ({args.revalidate_run})")
            return 0
        with args.config.open(encoding="utf-8") as source:
            config = json.load(source)
        def aborted(signum, frame):
            raise KeyboardInterrupt("termination requested")
        previous = signal.signal(signal.SIGTERM, aborted)
        try:
            result = execute(config)
        finally:
            signal.signal(signal.SIGTERM, previous)
        if result:
            print(f"Experiment supervision failed; see {config['outputDirectory']}/errors.log", file=sys.stderr)
        return result
    except (OSError, ValueError) as error:
        label = "Run revalidation failed" if args.revalidate_run is not None else "Experiment supervision failed"
        print(f"{label}: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
