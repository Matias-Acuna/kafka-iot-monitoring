"""Deterministic process/evidence simulation; never launches Kafka; lock interoperability uses a real Java subprocess."""

import copy
import contextlib
import csv
import importlib.util
import io
import json
import fcntl
import os
import subprocess
from unittest import mock
from pathlib import Path
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("run_experiment", Path(__file__).resolve().parents[1] / "run-experiment.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def producer_metadata():
    return {"schemaVersion": 1, "runId": "run-1", "experimentStatus": "NOT_FINALIZED",
            "configuration": {"producer": {"runId": "run-1", "randomSeed": 123},
                              "consumer": None, "streams": None},
            "components": {"producer": "COMPLETED", "consumer": "NOT_RECORDED"},
            "phaseDurationsMs": {"warmUp": 60000, "measurement": 300000},
            "clockAnchors": None, "scheduled": None, "observed": {}, "failures": []}


def configuration_event(role, command, timestamp=100000):
    options = {command[i][2:]: command[i + 1] for i in range(len(command)-1) if command[i].startswith("--")}
    applied = {"bootstrap.servers": [v.strip() for v in options["bootstrap-servers"].split(",")],
               "security.protocol": "PLAINTEXT"}
    event = {"evidenceType": MODULE.CONFIGURATION_TYPES[role], "observedAtEpochMs": timestamp,
             "appliedConfiguration": applied, "unavailable": ["brokerEffectiveTopicConfiguration"]}
    if role == "streams":
        applied.update({"application.id": options["application-id"], "processing.guarantee": "at_least_once",
                        "num.stream.threads": 1, "commit.interval.ms": 30000})
        rule = options["rule"]
        parameters = {"HIGH_TEMPERATURE": {"temperatureThreshold": ("temperature-threshold", float)},
                      "HIGH_FREQUENCY": {"windowDurationMs": ("window-duration-ms", int), "countThreshold": ("count-threshold", int)},
                      "RAPID_CHANGE": {"maxTimeDifferenceMs": ("max-time-difference-ms", int),
                                       "temperatureChangeThreshold": ("temperature-change-threshold", float)}}[rule]
        event.update(applicationId=options["application-id"], rule=rule,
                     ruleParameters={key: convert(options[name]) for key, (name, convert) in parameters.items()})
    else:
        applied.update({"group.id": options["group-id"], "auto.offset.reset": options.get("auto-offset-reset", "earliest"),
                        "enable.auto.commit": True, "max.poll.records": 500, "max.poll.interval.ms": 300000})
        output = options.get("result-csv-output")
        event.update(groupId=options["group-id"], subscribedTopics=["sensor-results", "sensor-alerts"],
                     resultCsvEnabled=output is not None, runIdFilter=options.get("run-id"),
                     resultCsvOutput=str(Path(output).absolute()) if output is not None else None)
    return event


class FakeProcesses:
    def __init__(self, directory, mode="normal"):
        self.directory, self.mode = directory, mode
        self.clock, self.queue = 0.0, []
        self.started, self.signals, self.codes = [], [], {}
        self.producer_start = None
        self.closed = False

    def now(self):
        return self.clock

    def epoch_ms(self):
        return 100000 + round(self.clock * 1000)

    def event(self, role, kind, status=None, **fields):
        event = dict(evidenceType=kind, observedAtEpochMs=self.epoch_ms(), **fields)
        if role == "consumer":
            event["groupId"] = "group-1"
        if role == "streams":
            event["applicationId"] = "app-1"
        if status:
            event["status"] = status
        self.queue.append((role, "stdout", json.dumps(event)))

    def start(self, role, command):
        self.started.append((role, self.clock, list(command)))
        self.codes[role] = None
        if role in MODULE.CONFIGURATION_TYPES and self.mode != "no_" + role + "_configuration":
            self.queue.append((role, "stdout", json.dumps(configuration_event(role, command, self.epoch_ms()))))
        if role == "consumer":
            self.queue.append((role, "stdout", "process started, not readiness"))
            self.queue.append((role, "stderr", "diagnostic from Consumer"))
            if self.mode != "no_consumer_ready":
                self.event(role, "MEASUREMENT_CONSUMER_READINESS", "READY", expectedResultPartitions=[0, 1],
                           assignedResultPartitions=[dict(topic="sensor-results", partition=p, startingPosition=0)
                                                     for p in ([0] if self.mode == "partial" else [0, 1])])
        elif role == "streams":
            self.event(role, "KAFKA_STREAMS_STARTUP", "NOT_READY", previousState="CREATED", currentState="CREATED")
            self.queue.append((role, "stdout", "Kafka Streams start requested"))
            if self.mode != "no_streams_ready":
                self.event(role, "KAFKA_STREAMS_STATE_CHANGED", "READY", previousState="REBALANCING",
                           currentState="CREATED" if self.mode == "premature" else "RUNNING")
        else:
            (self.directory / "run-config.json").write_text(json.dumps(producer_metadata()))
            self.producer_start = self.clock
            if self.mode == "bad_json":
                self.queue.append((role, "stdout", "{broken"))
            elif self.mode != "no_timing":
                anchor = self.epoch_ms()
                if self.mode == "bad_timing":
                    anchor -= 1
                self.event(role, "PRODUCER_WORKLOAD_TIMING", runId="run-1", workloadStartNs=123,
                           workloadStartEpochMs=anchor, sampleSpanNs=10, warmUpDurationNs=60_000_000_000,
                           measurementDurationNs=300_000_000_000,
                           measurementStartEpochMs=anchor + 60000,
                           measurementDeadlineEpochMs=anchor + (1 if self.mode == "bad_timing" else 360000))

    def pump(self, timeout):
        self.clock += timeout
        if self.mode == "abort" and self.producer_start is not None:
            self.mode = "aborted"
            raise KeyboardInterrupt("test abort")
        if self.mode == "exit" and self.producer_start is not None:
            self.codes["streams"] = 7
            self.mode = "exit_done"
        if self.mode == "evidence_failure" and self.producer_start is not None:
            self.queue.append(("consumer", "stderr", "Consumer evidence stdout could not be written"))
            self.mode = "evidence_failure_done"
        if self.producer_start is not None and self.clock - self.producer_start >= 360:
            self.codes["producer"] = 0
        events, self.queue = self.queue, []
        for role, stream, line in events:
            with (self.directory / f"{role}.{stream}.log").open("a", encoding="utf-8") as log:
                log.write(line + "\n")
        return events

    def poll(self, role):
        return self.codes.get(role)

    def signal(self, role, force=False):
        self.signals.append((role, force, self.clock))
        if self.mode == "stubborn" and not force:
            return
        self.codes[role] = -9 if force else -15
        if self.mode == "missing_cleanup" or force:
            return
        if role == "consumer":
            self.event(role, "MEASUREMENT_CONSUMER_POLLING_TERMINATED", "REQUESTED_SHUTDOWN")
            self.event(role, "MEASUREMENT_CONSUMER_KAFKA_CLOSED", "CLOSED")
            self.event(role, "MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED", "CLOSED")
        elif role == "streams":
            self.event(role, "KAFKA_STREAMS_SHUTDOWN_REQUESTED", "SHUTDOWN_REQUESTED")
            self.event(role, "KAFKA_STREAMS_CLOSE_RESULT", "CLOSED")

    def pending(self):
        return bool(self.queue)

    def close(self):
        self.closed = True


class CoordinatorTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        original_persist = MODULE.persist_coordination
        patcher = mock.patch.object(MODULE, "persist_coordination",
            side_effect=lambda config, processes, data: original_persist(config, processes, data, verify=lambda _: None))
        patcher.start()
        self.addCleanup(patcher.stop)
        self.output = self.root / "new-run"
        self.config = {
            "runId": "run-1", "scenario": "B1", "outputDirectory": str(self.output),
            "startupTimeoutSeconds": 1, "shutdownTimeoutSeconds": 0.3,
            "commands": {
                "consumer": [sys.executable, "consumer.py", "--bootstrap-servers", "unused:9092", "--group-id", "group-1"],
                "streams": [sys.executable, "streams.py", "--bootstrap-servers", "unused:9092", "--application-id", "app-1",
                            "--rule", "HIGH_TEMPERATURE", "--temperature-threshold", "30"],
                "producer": [sys.executable, "producer.py", "--bootstrap-servers", "unused:9092", "--random-seed", "123",
                             "--initial-event-timestamp", "1700000000000", "--timestamp-increment", "1",
                             "--target-events-per-second", "10"]}}

    def run_fake(self, mode="normal"):
        self.fake = FakeProcesses(self.output, mode)
        return MODULE.execute(self.config, lambda directory: self.fake)

    def journal(self):
        return [json.loads(line) for line in (self.output / "coordination.jsonl").read_text().splitlines()]

    def test_valid_b1_b2_b3_configuration_and_evidence_flags(self):
        for scenario in ("B1", "B2", "B3"):
            self.config["scenario"] = scenario
            validated = MODULE.validate(self.config)
            for role, option in (("consumer", "--readiness-stdout"), ("streams", "--lifecycle-stdout"),
                                 ("producer", "--timing-stdout")):
                command = validated["commands"][role]
                self.assertEqual(command[command.index(option) + 1], "true")
        self.assertFalse(self.output.exists())

    def test_rejects_b4_and_invalid_configuration_before_start(self):
        for field, value in (("scenario", "B4"), ("scenario", "unknown"), ("runId", ""),
                             ("commands", "shell string"), ("startupTimeoutSeconds", 0)):
            with self.subTest(field=field, value=value):
                config = copy.deepcopy(self.config)
                config[field] = value
                with self.assertRaises(MODULE.CoordinationError):
                    MODULE.execute(config, lambda _: self.fail("must not create processes"))
                self.assertFalse(self.output.exists())

    def test_rejects_shell_strings_duplicate_options_and_wrong_durations(self):
        for command in ("arbitrary shell text", ["bash", "-c", "echo bad"],
                        self.config["commands"]["producer"] + ["--warm-up-seconds", "0"],
                        self.config["commands"]["producer"] + ["--random-seed", "1"]):
            config = copy.deepcopy(self.config)
            config["commands"]["producer"] = command
            with self.assertRaises(MODULE.CoordinationError):
                MODULE.validate(config)

    def test_consumer_then_streams_then_producer_and_full_fixed_drain(self):
        self.assertEqual(self.run_fake(), 0)
        self.assertEqual([role for role, _, _ in self.fake.started], ["consumer", "streams", "producer"])
        journal = self.journal()
        types = [event["evidenceType"] for event in journal]
        cutoff = next(event for event in journal if event["evidenceType"] == "OBSERVATION_CUTOFF_ESTABLISHED")
        self.assertEqual(cutoff["drainCutoffEpochMs"] - cutoff["measurementDeadlineEpochMs"], 120000)
        self.assertEqual(cutoff["measurementDeadlineEpochMs"] - cutoff["measurementStartEpochMs"], 300000)
        self.assertIn("OBSERVATION_CUTOFF_REACHED", types)
        for role, force, when in self.fake.signals:
            self.assertFalse(force)
            self.assertGreaterEqual(100000 + round(when * 1000), cutoff["drainCutoffEpochMs"])
        self.assertEqual(journal[-1]["experimentStatus"], "NOT_FINALIZED")
        self.assertEqual(journal[-1]["observationCoverage"], "NOT_VERIFIED")
        self.assertEqual(json.loads((self.output / "run-config.json").read_text())["experimentStatus"], "NOT_FINALIZED")
        self.assertFalse((self.output / "summary.json").exists())
        self.assertFalse((self.output / "throughput.csv").exists())

    def test_no_consumer_ready_prevents_streams_and_producer_startup(self):
        self.assertEqual(self.run_fake("no_consumer_ready"), 1)
        self.assertEqual([role for role, _, _ in self.fake.started], ["consumer"])
        self.assertIn("startup timeout", (self.output / "errors.log").read_text())

    def test_start_requested_log_does_not_substitute_for_streams_running(self):
        self.assertEqual(self.run_fake("no_streams_ready"), 1)
        self.assertNotIn("producer", [role for role, _, _ in self.fake.started])

    def test_premature_streams_ready_and_partial_consumer_assignment_fail(self):
        for mode in ("premature", "partial"):
            with self.subTest(mode=mode):
                self.output = self.root / mode
                self.config["outputDirectory"] = str(self.output)
                self.assertEqual(self.run_fake(mode), 1)
                self.assertNotIn("producer", [role for role, _, _ in self.fake.started])

    def test_missing_timing_and_invalid_timing_fail(self):
        for mode in ("no_timing", "bad_timing", "bad_json"):
            with self.subTest(mode=mode):
                self.output = self.root / mode
                self.config["outputDirectory"] = str(self.output)
                self.assertEqual(self.run_fake(mode), 1)
                self.assertTrue(self.fake.closed)
                self.assertTrue((self.output / "errors.log").read_text())

    def test_unexpected_exit_preserves_code_and_diagnostics(self):
        self.assertEqual(self.run_fake("exit"), 1)
        self.assertEqual(self.journal()[-1]["exitCodes"]["streams"], 7)
        self.assertIn("unexpected streams exit: 7", (self.output / "errors.log").read_text())

    def test_evidence_output_failure_aborts_execution(self):
        self.assertEqual(self.run_fake("evidence_failure"), 1)
        self.assertIn("evidence output failure", (self.output / "errors.log").read_text())

    def test_forced_termination_is_explicit_after_timeout(self):
        self.assertEqual(self.run_fake("stubborn"), 1)
        self.assertTrue(any(force for _, force, _ in self.fake.signals))
        self.assertTrue(any(event["evidenceType"] == "FORCED_TERMINATION" for event in self.journal()))
        self.assertEqual(self.journal()[-1]["exitCodes"]["consumer"], -9)

    def test_signal_exit_alone_does_not_prove_successful_cleanup(self):
        self.assertEqual(self.run_fake("missing_cleanup"), 1)
        self.assertIn("cleanup evidence is incomplete", (self.output / "errors.log").read_text())

    def test_graceful_requested_shutdown_preserves_negative_exit_codes(self):
        self.assertEqual(self.run_fake(), 0)
        self.assertEqual(self.journal()[-1]["exitCodes"], {"producer": 0, "consumer": -15, "streams": -15})
        self.assertTrue(self.fake.closed)

    def test_original_output_and_evidence_are_retained(self):
        self.assertEqual(self.run_fake(), 0)
        self.assertIn("process started, not readiness", (self.output / "consumer.stdout.log").read_text())
        self.assertIn("diagnostic from Consumer", (self.output / "consumer.stderr.log").read_text())
        self.assertIn("diagnostic from Consumer", (self.output / "errors.log").read_text())
        self.assertTrue(any(event["evidenceType"] == "COMPONENT_EVIDENCE" for event in self.journal()))

    def test_existing_directory_and_configuration_file_are_preserved(self):
        config_path = self.root / "launch.json"
        config_path.write_text(json.dumps(self.config))
        original = config_path.read_bytes()
        self.output.mkdir()
        marker = self.output / "prior.csv"
        marker.write_text("old observations")
        with self.assertRaises(MODULE.CoordinationError):
            self.run_fake()
        self.assertEqual(marker.read_text(), "old observations")
        self.assertEqual(config_path.read_bytes(), original)

    def test_abort_requests_cleanup_without_finalization(self):
        self.assertEqual(self.run_fake("abort"), 1)
        self.assertTrue(self.fake.signals)
        self.assertTrue(self.fake.closed)
        self.assertEqual(self.journal()[-1]["experimentStatus"], "NOT_FINALIZED")
        self.assertIn("test abort", (self.output / "errors.log").read_text())
        metadata = json.loads((self.output / "run-config.json").read_text())
        self.assertEqual(metadata["coordination"]["status"], "INCOMPLETE")
        self.assertTrue(metadata["coordination"]["failures"])

    def test_early_zero_producer_exit_without_timing_still_times_out(self):
        class NoTimingExit(FakeProcesses):
            def start(inner, role, command):
                super().start(role, command)
                if role == "producer":
                    inner.codes[role] = 0
        self.fake = NoTimingExit(self.output, "no_timing")
        self.assertEqual(MODULE.execute(self.config, lambda _: self.fake), 1)
        self.assertIn("Producer workload timing", (self.output / "errors.log").read_text())

    def test_missing_metadata_handoff_failure_is_reported_without_fabrication(self):
        original = MODULE.persist_coordination.side_effect
        def missing(config, processes, evidence):
            (self.output / "run-config.json").unlink(missing_ok=True)
            return original(config, processes, evidence)
        MODULE.persist_coordination.side_effect = missing
        self.assertEqual(self.run_fake(), 1)
        self.assertIn("metadata handoff failed", (self.output / "errors.log").read_text())
        self.assertFalse((self.output / "run-config.json").exists())
        self.assertEqual(self.journal()[-1]["outcome"], "FAILED")

    def test_epoch_clock_lag_cannot_cause_early_cutoff_or_shutdown(self):
        class LaggingEpoch(FakeProcesses):
            def epoch_ms(inner):
                epoch = super().epoch_ms()
                return epoch - (24 if inner.clock > 100 else 0)
        self.fake = LaggingEpoch(self.output)
        self.assertEqual(MODULE.execute(self.config, lambda _: self.fake), 0)
        cutoff = next(e for e in self.journal() if e["evidenceType"] == "OBSERVATION_CUTOFF_ESTABLISHED")["drainCutoffEpochMs"]
        for event in self.journal():
            if event["evidenceType"] in {"OBSERVATION_CUTOFF_REACHED", "SHUTDOWN_REQUESTED"}:
                self.assertGreaterEqual(event["observedAtEpochMs"], cutoff)
        self.assertTrue(any(when > 480 for _, _, when in self.fake.signals))

    def test_coordinator_requires_consumer_run_isolation(self):
        config = MODULE.validate(self.config)
        command = config["commands"]["consumer"]
        self.assertEqual(command[command.index("--run-id") + 1], "run-1")
        event = configuration_event("consumer", command)
        MODULE.validate_configuration(event, "consumer", config)
        event["runIdFilter"] = "another-run"
        with self.assertRaisesRegex(MODULE.CoordinationError, "run isolation"):
            MODULE.validate_configuration(event, "consumer", config)
        event.pop("runIdFilter")
        with self.assertRaises(MODULE.CoordinationError):
            MODULE.validate_configuration(event, "consumer", config)
        self.config["commands"]["consumer"] += ["--run-id", "other"]
        with self.assertRaises(MODULE.CoordinationError):
            MODULE.validate(self.config)

    def test_input_configuration_is_not_mutated(self):
        original = copy.deepcopy(self.config)
        MODULE.validate(self.config)
        self.assertEqual(self.config, original)


class MetadataHandoffTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.path = self.root / "run-config.json"
        self.config = {"outputDirectory": self.root, "runId": "run-1", "scenario": "B2"}
        self.processes = FakeProcesses(self.root)
        self.processes.codes["producer"] = 0
        self.original = producer_metadata()
        self.path.write_text(json.dumps(self.original))
        self.coordination = {"status": "INCOMPLETE", "observationCoverage": "NOT_VERIFIED",
                             "failures": ["interrupted"], "components": {"consumer": {"status": "NOT_RECORDED"}}}

    def persist(self):
        MODULE.persist_coordination(self.config, self.processes, self.coordination, verify=lambda _: None)

    def test_matching_handoff_preserves_all_producer_fields(self):
        self.persist()
        data = json.loads(self.path.read_text())
        for field, value in self.original.items():
            self.assertEqual(data[field], value)
        self.assertEqual(data["scenario"], "B2")
        self.assertEqual(data["coordination"]["ownership"]["producerExitCode"], 0)
        self.assertEqual(data["coordination"]["ownership"]["owner"], "COORDINATOR")
        self.assertTrue(Path(str(self.path) + ".lock").exists())

    def test_running_producer_is_rejected_before_probe(self):
        self.processes.codes["producer"] = None
        with self.assertRaisesRegex(MODULE.CoordinationError, "exited Producer"):
            MODULE.persist_coordination(self.config, self.processes, {}, verify=lambda _: self.fail("no probe before exit"))
        self.assertEqual(json.loads(self.path.read_text()), self.original)

    def test_missing_metadata_is_not_created(self):
        self.path.unlink()
        with self.assertRaises(FileNotFoundError):
            self.persist()
        self.assertFalse(self.path.exists())

    def test_invalid_json_is_preserved(self):
        self.path.write_text("{invalid")
        with self.assertRaises(ValueError):
            self.persist()
        self.assertEqual(self.path.read_text(), "{invalid")

    def test_duplicate_json_keys_and_nonfinite_values_are_rejected(self):
        for source in ('{"runId":"wrong","runId":"run-1"}', '{"value":NaN}'):
            self.path.write_text(source)
            with self.assertRaises(MODULE.CoordinationError):
                self.persist()
            self.assertEqual(self.path.read_text(), source)

    def test_different_run_and_missing_producer_metadata_are_rejected(self):
        for field, value in (("runId", "another"), ("configuration", None),
                             ("components", {}), ("schemaVersion", 2)):
            data = dict(self.original, **{field: value})
            self.path.write_text(json.dumps(data))
            before = self.path.read_bytes()
            with self.subTest(field=field), self.assertRaises(MODULE.CoordinationError):
                self.persist()
            self.assertEqual(self.path.read_bytes(), before)

    def test_existing_ownership_finalization_and_scenario_conflicts(self):
        for field, value in (("coordination", {}), ("ownership", {}), ("finalization", {}),
                             ("experimentStatus", "FINALIZED"), ("scenario", "B4")):
            self.path.write_text(json.dumps(dict(self.original, **{field: value})))
            before = self.path.read_bytes()
            with self.subTest(field=field), self.assertRaises(MODULE.CoordinationError):
                self.persist()
            self.assertEqual(self.path.read_bytes(), before)

    def test_interrupted_run_and_missing_evidence_remain_incomplete(self):
        self.processes.codes["producer"] = -15
        self.persist()
        data = json.loads(self.path.read_text())
        self.assertEqual(data["experimentStatus"], "NOT_FINALIZED")
        self.assertEqual(data["coordination"]["status"], "INCOMPLETE")
        self.assertEqual(data["coordination"]["failures"], ["interrupted"])
        self.assertEqual(data["coordination"]["components"]["consumer"]["status"], "NOT_RECORDED")

    def test_atomic_replacement_failure_preserves_original_and_removes_temp(self):
        before = self.path.read_bytes()
        with mock.patch.object(MODULE.os, "replace", side_effect=OSError("atomic rename unavailable")):
            with self.assertRaisesRegex(OSError, "atomic rename"):
                self.persist()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(list(self.root.glob(".run-config-*.tmp")), [])

    def test_atomic_replacement_uses_sibling_and_valid_complete_json(self):
        real_replace = os.replace
        def replace(source, target):
            self.assertEqual(Path(source).parent, self.path.parent)
            self.assertEqual(json.loads(self.path.read_text()), self.original)
            self.assertEqual(json.loads(Path(source).read_text())["coordination"]["status"], "INCOMPLETE")
            real_replace(source, target)
        with mock.patch.object(MODULE.os, "replace", side_effect=replace) as replacement:
            self.persist()
        replacement.assert_called_once()

    def test_record_lock_contention_from_another_process(self):
        lock = str(self.path) + ".lock"
        code = "import fcntl,sys; f=open(sys.argv[1], 'ab'); fcntl.lockf(f, fcntl.LOCK_EX); print('LOCKED', flush=True); sys.stdin.read(1)"
        child = subprocess.Popen([sys.executable, "-c", code, lock], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            self.assertEqual(child.stdout.readline().strip(), b"LOCKED")
            with self.assertRaisesRegex(MODULE.CoordinationError, "still owned"):
                self.persist()
            self.assertEqual(json.loads(self.path.read_text()), self.original)
        finally:
            child.communicate(b"x", timeout=5)

    def test_real_java_python_record_lock_interoperability_both_directions(self):
        MODULE.verify_lock_compatibility(self.root)
        self.assertEqual(list(self.root.glob(".metadata-lock-probe-*")), [])
        # Exercise the real verified handoff as well, without another expensive probe.
        self.persist()
        self.assertEqual(json.loads(self.path.read_text())["experimentStatus"], "NOT_FINALIZED")

    def test_failed_interoperability_probe_cannot_claim_ownership(self):
        before = self.path.read_bytes()
        with self.assertRaisesRegex(MODULE.CoordinationError, "incompatible"):
            MODULE.persist_coordination(self.config, self.processes, {},
                verify=mock.Mock(side_effect=MODULE.CoordinationError("incompatible locks")))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertFalse(Path(str(self.path) + ".lock").exists())


class FinalizationTest(unittest.TestCase):
    setUp = CoordinatorTest.setUp
    run_fake = CoordinatorTest.run_fake
    journal = CoordinatorTest.journal

    # Reuse deterministic launch fixtures without rerunning existing tests.
    def complete_evidence(self):
        self.assertEqual(self.run_fake(), 0)
        data = json.loads((self.output / "run-config.json").read_text())
        coordination = data["coordination"]
        validated = MODULE.validate(dict(self.config, outputDirectory=str(self.root / "unused")))
        validated["outputDirectory"] = self.output
        validated["commands"] = {role: command for role, _, command in self.fake.started}
        producer = data["configuration"]["producer"]
        args = validated["commands"]["producer"]
        options = {args[i][2:]: args[i + 1] for i in range(len(args)-1) if args[i].startswith("--")}
        fields = {"run-id": ("runId", str), "random-seed": ("randomSeed", int),
                  "initial-event-timestamp": ("initialEventTimestamp", int),
                  "timestamp-increment": ("timestampIncrementMs", int),
                  "target-events-per-second": ("targetEventsPerSecond", float),
                  "csv-output": ("csvOutput", str), "event-csv-output": ("eventCsvOutput", str),
                  "run-config-output": ("runConfigOutput", str)}
        for name, (field, convert) in fields.items():
            producer[field] = convert(options[name])
        producer["kafkaSettings"] = {"bootstrap.servers": "unused:9092", "acks": "all", "enable.idempotence": "true"}
        timing = next(e["evidence"] for e in coordination["evidence"]
                      if e.get("evidence", {}).get("evidenceType") == "PRODUCER_WORKLOAD_TIMING")
        data["clockAnchors"] = {k: timing[k] for k in ("workloadStartNs", "workloadStartEpochMs", "sampleSpanNs")}
        data["scheduled"] = {k: timing[k] for k in ("measurementStartEpochMs", "measurementDeadlineEpochMs")}
        data["observed"] = {k: timing["measurementDeadlineEpochMs"] for k in (
            "generationStoppedEpochMs", "publicationsDrainedEpochMs", "eventCsvClosedEpochMs", "producerCleanupCompletedEpochMs")}
        for item in coordination["evidence"]:
            if item.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_POLLING_TERMINATED":
                item["evidence"]["pollingInvoked"] = True
        coordination["lastPollProgress"] = {"evidence": {"evidenceType": "MEASUREMENT_CONSUMER_POLL_PROGRESS",
                "status": "READY", "successfulPolls": 1}}
        self.data, self.coordination, self.validated = data, coordination, validated
        self.producer_rows = [["run-1", "event-1", "device-1", "MEASUREMENT", "160001", "160000", "ATTEMPTED", "", "", ""],
                              ["run-1", "event-1", "device-1", "MEASUREMENT", "160002", "160000", "CONFIRMED", "0", "1", ""]]
        self.result_rows = [["run-1", "event-1", "device-1", "1", "160000", "HIGH_TEMPERATURE", "true", "160003", "3", "false", "false"]]
        self.write_raw()

    def write_raw(self):
        headers = {"producer-events.csv": "runId,eventId,deviceId,phase,recordedAt,sentAt,status,partition,offset,error",
                   "sensor-results.csv": "runId,eventId,deviceId,eventTime,sentAt,ruleType,triggered,resultReceivedAt,latencyMs,duplicate,negativeLatency"}
        for name, rows in (("producer-events.csv", self.producer_rows), ("sensor-results.csv", self.result_rows)):
            with (self.output / name).open("w", newline="") as output:
                writer = csv.writer(output)
                writer.writerow(headers[name].split(","))
                writer.writerows(rows)

    def checked(self, name):
        outcome = MODULE.validate_finalization(self.data, self.coordination, self.validated)
        return next(c["status"] for c in outcome["checks"] if c["condition"] == name)

    def test_complete_available_synthetic_evidence_still_fails_closed(self):
        self.complete_evidence()
        result = MODULE.validate_finalization(self.data, self.coordination, self.validated)
        self.assertEqual(result["status"], "BLOCKED")
        blocked = {c["condition"] for c in result["checks"] if c["status"] == "BLOCKED"}
        self.assertEqual(blocked, {"continuousObservationCoverage"})
        self.assertTrue(all(result["blockingReasons"]))

    def test_missing_producer_completion_blocks(self):
        self.complete_evidence()
        self.data["observed"].pop("producerCleanupCompletedEpochMs")
        self.assertEqual(self.checked("producerCompletion"), "BLOCKED")

    def test_missing_consumer_and_streams_readiness_block(self):
        self.complete_evidence()
        self.coordination["evidence"] = [e for e in self.coordination["evidence"] if e.get("evidence", {}).get("status") != "READY"]
        self.assertEqual(self.checked("consumerReadiness"), "BLOCKED")
        self.assertEqual(self.checked("streamsReadiness"), "BLOCKED")

    def test_partition_loss_and_rebalance_block(self):
        self.complete_evidence()
        self.coordination["evidence"].append({"evidenceType": "COMPONENT_EVIDENCE", "component": "consumer", "evidence": {
            "evidenceType": "MEASUREMENT_CONSUMER_PARTITIONS_LOST", "affectedPartitions": [{"topic": "sensor-results", "partition": 0}]}})
        self.assertEqual(self.checked("runtimeContinuity"), "BLOCKED")

    def test_missing_poll_progress_blocks(self):
        self.complete_evidence()
        self.coordination["lastPollProgress"] = None
        self.assertEqual(self.checked("pollProgress"), "BLOCKED")

    def test_interrupted_observation_blocks(self):
        self.complete_evidence()
        self.coordination["failures"] = ["KeyboardInterrupt: interrupted"]
        self.assertEqual(self.checked("coordinationFailures"), "BLOCKED")

    def test_invalid_drain_cutoff_blocks_without_moving_deadline(self):
        self.complete_evidence()
        before = copy.deepcopy(self.data["scheduled"])
        self.coordination["drain"]["scheduledCutoffEpochMs"] += 1
        self.assertEqual(self.checked("drainCutoff"), "BLOCKED")
        self.assertEqual(self.data["scheduled"], before)

    def test_failed_csv_and_streams_closures_block(self):
        self.complete_evidence()
        kinds = {"MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED", "KAFKA_STREAMS_CLOSE_RESULT"}
        for item in self.coordination["evidence"]:
            if item.get("evidence", {}).get("evidenceType") in kinds:
                item["evidence"]["status"] = "FAILED"
        for kind in kinds:
            self.assertEqual(self.checked(kind), "BLOCKED")

    def test_publication_failure_is_not_itself_a_coordination_failure(self):
        self.complete_evidence()
        self.producer_rows[1][6] = "FAILED"
        self.producer_rows[1][-1] = "Kafka rejected publication"
        self.write_raw()
        self.data["components"]["producer"] = "FAILED"
        self.data["failures"] = [{"category": "PUBLICATION_FAILURE", "message": "publication failed"}]
        self.coordination["components"]["producer"]["exitCode"] = 1
        self.assertEqual(self.checked("producerCompletion"), "VERIFIED")
        self.assertEqual(self.checked("producerExit"), "VERIFIED")
        self.assertEqual(self.checked("coordinationFailures"), "VERIFIED")

    def test_confirmed_event_without_result_is_not_coverage_failure(self):
        self.complete_evidence()
        self.result_rows = []
        self.write_raw()
        self.assertEqual(self.checked("sensor-results.csv"), "VERIFIED")
        self.assertEqual(self.checked("originatingAttempts"), "VERIFIED")

    def test_conflicting_run_and_malformed_raw_observations_block(self):
        self.complete_evidence()
        self.result_rows[0][0] = "another-run"
        self.write_raw()
        self.assertEqual(self.checked("sensor-results.csv"), "BLOCKED")
        (self.output / "producer-events.csv").write_text("broken header")
        self.assertEqual(self.checked("producer-events.csv"), "BLOCKED")

    def test_missing_raw_file_and_missing_attempt_block(self):
        self.complete_evidence()
        self.producer_rows.pop(0)
        self.write_raw()
        self.assertEqual(self.checked("originatingAttempts"), "BLOCKED")
        (self.output / "sensor-results.csv").unlink()
        self.assertEqual(self.checked("sensor-results.csv"), "BLOCKED")

    def test_conflicting_effective_producer_configuration_blocks(self):
        self.complete_evidence()
        self.data["configuration"]["producer"]["kafkaSettings"]["acks"] = "0"
        self.assertEqual(self.checked("producerConfiguration"), "BLOCKED")

    def test_timing_must_match_persisted_clock_and_deadline(self):
        self.complete_evidence()
        self.data["scheduled"]["measurementDeadlineEpochMs"] += 1
        self.assertEqual(self.checked("clockAndBoundaries"), "BLOCKED")

    def test_readiness_after_publication_cannot_validate_startup(self):
        self.complete_evidence()
        for item in self.coordination["evidence"]:
            if item.get("evidence", {}).get("status") == "READY":
                item["evidence"]["observedAtEpochMs"] = 999999
        self.assertEqual(self.checked("consumerReadiness"), "BLOCKED")
        self.assertEqual(self.checked("streamsReadiness"), "BLOCKED")

    def test_inconsistent_result_sent_at_blocks_association(self):
        self.complete_evidence()
        self.result_rows[0][4] = "159999"
        self.result_rows[0][8] = "4"
        self.write_raw()
        self.assertEqual(self.checked("originatingAttempts"), "BLOCKED")

    def test_nonzero_producer_exit_is_deferred_to_persisted_failure_evidence(self):
        self.complete_evidence()
        self.coordination["components"]["producer"]["exitCode"] = 1
        self.assertEqual(self.checked("producerExit"), "BLOCKED")
        self.data["failures"] = [{"category": "PUBLICATION_FAILURE"}]
        self.assertEqual(self.checked("producerExit"), "VERIFIED")

    def test_finalization_preserves_input_and_does_not_trust_complete_flags(self):
        self.complete_evidence()
        self.coordination["observationCoverage"] = "COMPLETE"
        before = copy.deepcopy((self.data, self.coordination))
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")
        self.assertEqual((self.data, self.coordination), before)
        persisted = json.loads((self.output / "run-config.json").read_text())
        self.assertEqual(persisted["experimentStatus"], "NOT_FINALIZED")
        self.assertEqual(persisted["finalization"]["status"], "BLOCKED")


class ConfigurationIntegrationTest(unittest.TestCase):
    setUp = CoordinatorTest.setUp
    run_fake = CoordinatorTest.run_fake
    journal = CoordinatorTest.journal

    def coordinator(self):
        config = MODULE.validate(self.config)
        return MODULE.Coordinator(config, FakeProcesses(self.output), io.StringIO(), io.StringIO())

    def send(self, coordinator, role, event):
        coordinator.handle(role, "stdout", json.dumps(event))

    def test_valid_configuration_is_preserved_under_coordination_without_finalization(self):
        self.assertEqual(self.run_fake(), 0)
        data = json.loads((self.output / "run-config.json").read_text())
        actual = data["coordination"]["configurationEvidence"]
        self.assertEqual(set(actual), {"consumer", "streams"})
        self.assertEqual(actual["streams"]["appliedConfiguration"]["commit.interval.ms"], 30000)
        self.assertEqual(actual["consumer"]["appliedConfiguration"]["enable.auto.commit"], True)
        self.assertEqual(data["configuration"], producer_metadata()["configuration"])
        self.assertEqual(data["experimentStatus"], "NOT_FINALIZED")
        self.assertEqual(data["coordination"]["observationCoverage"], "NOT_VERIFIED")
        checks = {c["condition"]: c["status"] for c in data["finalization"]["checks"]}
        self.assertEqual(checks["streamsEffectiveConfiguration"], "VERIFIED")
        self.assertEqual(checks["consumerEffectiveConfiguration"], "VERIFIED")
        self.assertEqual(checks["continuousObservationCoverage"], "BLOCKED")

    def test_configuration_does_not_itself_establish_readiness(self):
        coordinator = self.coordinator()
        for role in ("consumer", "streams"):
            self.send(coordinator, role, configuration_event(role, coordinator.config["commands"][role]))
        self.assertFalse(coordinator.consumer_ready)
        self.assertFalse(coordinator.streams_ready)

    def test_ready_without_configuration_prevents_producer_launch(self):
        for role in ("consumer", "streams"):
            with self.subTest(role=role):
                self.output = self.root / role
                self.config["outputDirectory"] = str(self.output)
                self.assertEqual(self.run_fake("no_" + role + "_configuration"), 1)
                self.assertNotIn("producer", [r for r, _, _ in self.fake.started])
                self.assertIn("preceding valid configuration", (self.output / "errors.log").read_text())

    def test_wrong_source_process_is_rejected(self):
        coordinator = self.coordinator()
        for source, actual in (("producer", "streams"), ("consumer", "streams"), ("streams", "consumer")):
            with self.subTest(source=source, actual=actual), self.assertRaisesRegex(MODULE.CoordinationError, "source"):
                self.send(coordinator, source, configuration_event(actual, coordinator.config["commands"][actual]))

    def test_missing_or_malformed_configuration_fields_are_rejected(self):
        for role in ("consumer", "streams"):
            coordinator = self.coordinator()
            original = configuration_event(role, coordinator.config["commands"][role])
            for field in original:
                event = copy.deepcopy(original)
                event.pop(field)
                with self.subTest(role=role, field=field), self.assertRaises(MODULE.CoordinationError):
                    MODULE.validate_configuration(event, role, coordinator.config)
            for field, value in (("observedAtEpochMs", True), ("observedAtEpochMs", -1),
                                 ("appliedConfiguration", []), ("unavailable", "unknown")):
                event = dict(original, **{field: value})
                with self.subTest(role=role, field=field), self.assertRaises(MODULE.CoordinationError):
                    self.send(coordinator, role, event)

    def test_identity_and_bootstrap_conflicts_are_rejected(self):
        for role, identity, property_name in (("consumer", "groupId", "group.id"), ("streams", "applicationId", "application.id")):
            coordinator = self.coordinator()
            original = configuration_event(role, coordinator.config["commands"][role])
            for change in ("identity", "property", "brokers"):
                event = copy.deepcopy(original)
                if change == "identity": event[identity] = "wrong"
                elif change == "property": event["appliedConfiguration"][property_name] = "wrong"
                else: event["appliedConfiguration"]["bootstrap.servers"] = ["wrong:9092"]
                with self.subTest(role=role, change=change), self.assertRaises(MODULE.CoordinationError):
                    self.send(coordinator, role, event)

    def test_every_rule_has_matching_typed_parameters(self):
        for rule, arguments in (("HIGH_TEMPERATURE", ["--temperature-threshold", "30"]),
                                ("HIGH_FREQUENCY", ["--window-duration-ms", "1000", "--count-threshold", "2"]),
                                ("RAPID_CHANGE", ["--max-time-difference-ms", "1000", "--temperature-change-threshold", "2.5"])):
            coordinator = self.coordinator()
            command = coordinator.config["commands"]["streams"]
            command[command.index("--rule")+1:] = [rule] + arguments + ["--lifecycle-stdout", "true"]
            event = configuration_event("streams", command)
            self.send(coordinator, "streams", event)
            altered = copy.deepcopy(event)
            key = next(iter(altered["ruleParameters"]))
            altered["ruleParameters"][key] += 1
            with self.assertRaises(MODULE.CoordinationError):
                self.send(coordinator, "streams", altered)
            altered["ruleParameters"][key] = True
            with self.assertRaises(MODULE.CoordinationError):
                self.send(coordinator, "streams", altered)

    def test_streams_guarantee_and_thread_count_require_actual_values(self):
        coordinator = self.coordinator()
        for field, value in (("processing.guarantee", None), ("processing.guarantee", "exactly_once_v2"),
                             ("num.stream.threads", True), ("num.stream.threads", 2)):
            event = configuration_event("streams", coordinator.config["commands"]["streams"])
            event["appliedConfiguration"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(MODULE.CoordinationError):
                self.send(coordinator, "streams", event)

    def test_consumer_topic_offset_and_csv_conflicts_are_rejected(self):
        coordinator = self.coordinator()
        for field, value in (("subscribedTopics", ["sensor-results"]), ("subscribedTopics", [{}, {}]),
                             ("resultCsvEnabled", False), ("resultCsvEnabled", 1),
                             ("resultCsvOutput", "wrong.csv"), ("resultCsvOutput", None)):
            event = configuration_event("consumer", coordinator.config["commands"]["consumer"])
            event[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(MODULE.CoordinationError):
                self.send(coordinator, "consumer", event)
        event = configuration_event("consumer", coordinator.config["commands"]["consumer"])
        event["appliedConfiguration"]["auto.offset.reset"] = "latest"
        with self.assertRaises(MODULE.CoordinationError):
            self.send(coordinator, "consumer", event)

    def test_contradictory_repeats_rejected_and_identical_settings_preserved(self):
        coordinator = self.coordinator()
        event = configuration_event("streams", coordinator.config["commands"]["streams"])
        self.send(coordinator, "streams", event)
        repeated = copy.deepcopy(event)
        repeated["observedAtEpochMs"] += 1
        self.send(coordinator, "streams", repeated)
        self.assertEqual(coordinator.configuration_evidence["streams"], event)
        repeated["appliedConfiguration"]["commit.interval.ms"] = 123
        with self.assertRaisesRegex(MODULE.CoordinationError, "contradictory"):
            self.send(coordinator, "streams", repeated)

    def test_recorded_configuration_timestamp_must_precede_ready(self):
        coordinator = self.coordinator()
        event = configuration_event("streams", coordinator.config["commands"]["streams"], timestamp=100001)
        self.send(coordinator, "streams", event)
        with self.assertRaisesRegex(MODULE.CoordinationError, "preceding"):
            self.send(coordinator, "streams", {"evidenceType": "KAFKA_STREAMS_STATE_CHANGED", "observedAtEpochMs": 100000,
                      "status": "READY", "applicationId": "app-1", "currentState": "RUNNING"})

    def test_finalization_rechecks_history_and_detects_missing_or_tampered_config(self):
        self.assertEqual(self.run_fake(), 0)
        data = json.loads((self.output / "run-config.json").read_text())
        config = dict(self.config, commands={role: command for role, _, command in self.fake.started})
        for role in ("consumer", "streams"):
            altered = copy.deepcopy(data["coordination"])
            altered["configurationEvidence"].pop(role)
            with self.assertRaises(MODULE.CoordinationError):
                MODULE.verify_configuration_history(role, altered, config)
            altered = copy.deepcopy(data["coordination"])
            altered["evidence"] = [e for e in altered["evidence"] if e.get("evidence", {}).get("evidenceType") != MODULE.CONFIGURATION_TYPES[role]]
            with self.assertRaises(MODULE.CoordinationError):
                MODULE.verify_configuration_history(role, altered, config)


class OperationalCoverageTest(unittest.TestCase):
    setUp = FinalizationTest.setUp
    run_fake = FinalizationTest.run_fake
    journal = FinalizationTest.journal
    complete_evidence = FinalizationTest.complete_evidence
    write_raw = FinalizationTest.write_raw
    checked = FinalizationTest.checked
    # Invoke the established full fixture but run only the new tests below.
    def operational_fixture(self):
        self.complete_evidence()
        events = self.coordination["evidence"]
        ready_at = next(e["evidence"]["observedAtEpochMs"] for e in events if
                        e.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_READINESS")
        ended = next(e["evidence"]["observedAtEpochMs"] for e in events if
                     e.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_POLLING_TERMINATED")
        for at in list(range(ready_at + 1000, ended, 1000)) + [ended]:
            event = {"evidenceType": "COMPONENT_EVIDENCE", "component": "consumer", "observedAtEpochMs": at,
                     "evidence": {"evidenceType": "MEASUREMENT_CONSUMER_POLL_PROGRESS", "groupId": "group-1",
                                  "observedAtEpochMs": at, "status": "READY", "successfulPolls": 1,
                                  "emptyPolls": 1, "recordCount": 0, "lastPollEmpty": True}}
            events.append(event)
            self.coordination["lastPollProgress"] = event
        self.coordination["evidence"] = sorted(events, key=lambda e: e["observedAtEpochMs"])

    def test_complete_operational_evidence_can_finalize_atomically(self):
        self.operational_fixture()
        before = copy.deepcopy(self.data)
        output = self.output / "run-config.json"
        producer_only = {key: value for key, value in self.data.items() if key not in {"coordination", "finalization", "scenario"}}
        output.write_text(json.dumps(producer_only))
        raw_before = (self.output / "sensor-results.csv").read_bytes()
        original_persist = MODULE.persist_coordination.side_effect
        result = original_persist(self.validated, self.fake, self.coordination)
        self.assertEqual(result["status"], "VERIFIED")
        data = json.loads(output.read_text())
        self.assertEqual(data["experimentStatus"], "FINALIZED")
        self.assertEqual(data["coordination"]["observationCoverage"], "COMPLETE")
        self.assertEqual(data["finalization"]["blockingReasons"], [])
        self.assertEqual(data["finalization"]["operationalCoverage"]["maximumReportingGapMs"], 2001)
        for key in ("configuration", "observed", "scheduled", "failures", "clockAnchors"):
            self.assertEqual(data[key], before[key])
        self.assertEqual((self.output / "sensor-results.csv").read_bytes(), raw_before)

    def test_missing_poll_history_is_not_substituted_by_last_snapshot(self):
        self.operational_fixture()
        self.coordination["evidence"] = [e for e in self.coordination["evidence"] if
            e.get("evidence", {}).get("evidenceType") != "MEASUREMENT_CONSUMER_POLL_PROGRESS"]
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")

    def test_unsupported_observation_gap_blocks(self):
        self.operational_fixture()
        self.coordination["evidence"] = [e for e in self.coordination["evidence"] if
            e.get("evidence", {}).get("evidenceType") != "MEASUREMENT_CONSUMER_POLL_PROGRESS"
            or e["evidence"]["observedAtEpochMs"] < 101000 or e["evidence"]["observedAtEpochMs"] > 500000]
        result = MODULE.validate_finalization(self.data, self.coordination, self.validated)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertTrue(any("gap" in reason for reason in result["blockingReasons"]))

    def test_kafka_liveness_setting_does_not_determine_reporting_gap(self):
        self.operational_fixture()
        self.coordination["configurationEvidence"]["consumer"]["appliedConfiguration"].pop("max.poll.interval.ms")
        self.assertEqual(self.checked("continuousObservationCoverage"), "VERIFIED")

    def test_premature_polling_termination_is_rejected(self):
        self.operational_fixture()
        for event in self.coordination["evidence"]:
            if event.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_POLLING_TERMINATED":
                event["evidence"]["observedAtEpochMs"] = self.coordination["drain"]["scheduledCutoffEpochMs"] - 1
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")

    def test_partition_loss_invalidates_otherwise_complete_coverage(self):
        self.operational_fixture()
        self.coordination["evidence"].append({"component": "consumer", "evidenceType": "COMPONENT_EVIDENCE",
            "evidence": {"evidenceType": "MEASUREMENT_CONSUMER_PARTITIONS_LOST", "observedAtEpochMs": 200000,
                         "affectedPartitions": [{"topic": "sensor-results", "partition": 0}], "status": "NOT_READY"}})
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")

    def test_requested_shutdown_after_cutoff_and_signal_exit_are_acceptable(self):
        self.operational_fixture()
        result = MODULE.validate_finalization(self.data, self.coordination, self.validated)
        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(self.coordination["components"]["consumer"]["exitCode"], -15)

    def test_csv_failure_prevents_finalization(self):
        self.operational_fixture()
        for event in self.coordination["evidence"]:
            if event.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_RESULT_CSV_CLOSED":
                event["evidence"]["status"] = "FAILED"
        self.assertEqual(MODULE.validate_finalization(self.data, self.coordination, self.validated)["status"], "BLOCKED")

    def test_missing_result_does_not_prevent_finalization(self):
        self.operational_fixture()
        self.result_rows = []
        self.write_raw()
        self.assertEqual(MODULE.validate_finalization(self.data, self.coordination, self.validated)["status"], "VERIFIED")

    def test_gap_persists_not_finalized_with_concrete_reasons(self):
        self.operational_fixture()
        self.coordination["evidence"] = [e for e in self.coordination["evidence"] if
            e.get("evidence", {}).get("evidenceType") != "MEASUREMENT_CONSUMER_POLL_PROGRESS"]
        output = self.output / "run-config.json"
        output.write_text(json.dumps({key: value for key, value in self.data.items()
                                      if key not in {"coordination", "finalization", "scenario"}}))
        MODULE.persist_coordination.side_effect(self.validated, self.fake, self.coordination)
        data = json.loads(output.read_text())
        self.assertEqual(data["experimentStatus"], "NOT_FINALIZED")
        self.assertEqual(data["coordination"]["observationCoverage"], "NOT_VERIFIED")
        self.assertTrue(any("history is missing" in reason for reason in data["finalization"]["blockingReasons"]))

    def test_supervision_journal_reports_the_actual_persisted_finalization_outcome(self):
        verified = {"status": "VERIFIED", "observationCoverage": "COMPLETE", "checks": [], "blockingReasons": []}
        # Isolate journal propagation; the real atomic finalization path is tested above.
        with mock.patch.object(MODULE, "persist_coordination", return_value=verified):
            self.assertEqual(self.run_fake(), 0)
        self.assertEqual(self.journal()[-1]["experimentStatus"], "FINALIZED")
        self.assertEqual(self.journal()[-1]["observationCoverage"], "COMPLETE")

    def test_assignment_reestablishment_and_premature_streams_shutdown_block(self):
        self.operational_fixture()
        original = copy.deepcopy(self.coordination["evidence"])
        self.coordination["evidence"].append({"component": "consumer", "evidenceType": "COMPONENT_EVIDENCE",
            "evidence": {"evidenceType": "MEASUREMENT_CONSUMER_READINESS", "observedAtEpochMs": 200000, "status": "READY"}})
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")
        self.coordination["evidence"] = original
        self.coordination["evidence"].append({"component": "streams", "evidenceType": "COMPONENT_EVIDENCE",
            "evidence": {"evidenceType": "KAFKA_STREAMS_STATE_CHANGED", "observedAtEpochMs": 200000,
                         "currentState": "PENDING_SHUTDOWN", "status": "NOT_READY"}})
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")

    def replace_poll_times(self, times):
        original = next(e for e in self.coordination["evidence"] if
                        e.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_POLL_PROGRESS")
        events = [e for e in self.coordination["evidence"] if
                  e.get("evidence", {}).get("evidenceType") != "MEASUREMENT_CONSUMER_POLL_PROGRESS"]
        for at in times:
            event = copy.deepcopy(original)
            event["observedAtEpochMs"] = at
            event["evidence"]["observedAtEpochMs"] = at
            events.append(event)
            self.coordination["lastPollProgress"] = event
        self.coordination["evidence"] = sorted(events, key=lambda e: e["observedAtEpochMs"])

    def poll_times(self):
        return [e["evidence"]["observedAtEpochMs"] for e in self.coordination["evidence"] if
                e.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_POLL_PROGRESS"]

    def test_legitimate_report_variation_within_one_additional_poll_is_valid(self):
        self.operational_fixture()
        times = self.poll_times()
        for i in range(1, len(times)-1, 2):
            if times[i] + 250 < times[i + 1]:
                times[i] += 250  # Alternating 1250/750 ms reports.
        self.replace_poll_times(times)
        result = MODULE.validate_finalization(self.data, self.coordination, self.validated)
        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(result["operationalCoverage"]["maximumReportingGapMs"], 2001)
        self.assertEqual(result["operationalCoverage"]["schedulingGraceMs"], 0)
        self.assertEqual(result["operationalCoverage"]["timestampGranularityToleranceMs"], 1)

    def test_ten_and_sixty_second_evidence_gaps_block(self):
        self.operational_fixture()
        times = self.poll_times()
        for gap in (10000, 60000):
            with self.subTest(gap=gap):
                self.replace_poll_times([at for at in times if not 200000 < at < 200000 + gap])
                result = MODULE.validate_finalization(self.data, self.coordination, self.validated)
                self.assertEqual(result["status"], "BLOCKED")
                self.assertTrue(any("gap exceeds" in reason for reason in result["blockingReasons"]))

    def test_missing_initial_reports_block(self):
        self.operational_fixture()
        times = self.poll_times()
        self.replace_poll_times(times[3:])
        self.assertEqual(self.checked("continuousObservationCoverage"), "BLOCKED")

    def test_missing_reports_near_cutoff_block(self):
        self.operational_fixture()
        times = self.poll_times()
        cutoff = self.coordination["drain"]["scheduledCutoffEpochMs"]
        self.replace_poll_times([at for at in times if at < cutoff - 10000])
        result = MODULE.validate_finalization(self.data, self.coordination, self.validated)
        self.assertEqual(result["status"], "BLOCKED")
        self.assertTrue(any("near cutoff" in reason for reason in result["blockingReasons"]))

    def test_reporting_bound_accepts_2001_ms_but_rejects_2002_ms(self):
        self.operational_fixture()
        times = self.poll_times()
        # Removing one report leaves the original 2000 ms interval.
        self.replace_poll_times([at for at in times if at != 201000])
        self.assertEqual(self.checked("continuousObservationCoverage"), "VERIFIED")
        for gap, expected in ((2001, "VERIFIED"), (2002, "BLOCKED")):
            with self.subTest(gap=gap):
                self.replace_poll_times([199000 + gap if at == 201000 else at for at in times if at != 200000])
                self.assertEqual(self.checked("continuousObservationCoverage"), expected)

    def test_initial_evidence_has_only_one_ms_granularity_tolerance(self):
        self.operational_fixture()
        times = self.poll_times()
        ready_at = next(e["evidence"]["observedAtEpochMs"] for e in self.coordination["evidence"] if
                        e.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_READINESS")
        for gap, expected in ((2001, "VERIFIED"), (2002, "BLOCKED")):
            with self.subTest(gap=gap):
                first = ready_at + gap
                self.replace_poll_times([first] + [at for at in times if at > first])
                self.assertEqual(self.checked("continuousObservationCoverage"), expected)

    def test_cutoff_tail_has_only_one_ms_granularity_tolerance(self):
        self.operational_fixture()
        times = self.poll_times()
        ended = next(e["evidence"]["observedAtEpochMs"] for e in self.coordination["evidence"] if
                     e.get("evidence", {}).get("evidenceType") == "MEASUREMENT_CONSUMER_POLLING_TERMINATED")
        for gap, expected in ((2001, "VERIFIED"), (2002, "BLOCKED")):
            with self.subTest(gap=gap):
                last = ended - gap
                self.replace_poll_times([at for at in times if at < last] + [last])
                self.assertEqual(self.checked("continuousObservationCoverage"), expected)

    def test_actual_publication_failure_metadata_does_not_prevent_finalization(self):
        self.operational_fixture()
        self.data["components"]["producer"] = "FAILED"
        self.data["observed"]["failureObservedEpochMs"] = self.data["observed"].pop("producerCleanupCompletedEpochMs")
        self.data["failures"] = [{"category": "PUBLICATION", "count": 1}, {"category": "PUBLICATION_FAILURE"}]
        self.coordination["components"]["producer"]["exitCode"] = 1
        self.producer_rows[1][6] = "FAILED"
        self.producer_rows[1][-1] = "Kafka publication failed"
        self.write_raw()
        self.assertEqual(MODULE.validate_finalization(self.data, self.coordination, self.validated)["status"], "VERIFIED")


class RevalidationTest(unittest.TestCase):
    def setUp(self):
        self.fixture = OperationalCoverageTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.operational_fixture()
        times = self.fixture.poll_times()
        self.fixture.replace_poll_times([201001 if at == 201000 else at for at in times if at != 200000])
        self.root = self.fixture.output
        self.path = self.root / "run-config.json"
        producer_only = {key: value for key, value in self.fixture.data.items()
                         if key not in {"coordination", "finalization", "scenario"}}
        self.path.write_text(json.dumps(producer_only))
        MODULE.persist_coordination.side_effect(self.fixture.validated, self.fixture.fake, self.fixture.coordination)
        data = json.loads(self.path.read_text())
        self.assertEqual(data["experimentStatus"], "FINALIZED")
        # Model the previous 2000 ms decision, using real synthetic 2001 ms evidence.
        data["experimentStatus"] = "NOT_FINALIZED"
        data["coordination"]["observationCoverage"] = "NOT_VERIFIED"
        data["finalization"]["status"] = "BLOCKED"
        data["finalization"]["observationCoverage"] = "NOT_VERIFIED"
        data["finalization"]["blockingReasons"] = ["poll gap exceeded original 2000 ms criterion"]
        data["finalization"]["operationalCoverage"] = None
        data["revalidationHistory"] = [{"operation": "PREVIOUS_AUDIT_ENTRY", "outcome": "BLOCKED"}]
        self.path.write_text(json.dumps(data))
        self.original = copy.deepcopy(data)
        self.original_bytes = self.path.read_bytes()
        self.raw = {p.name: p.read_bytes() for p in self.root.iterdir() if p.is_file() and p.name != "run-config.json"}

    def assert_raw_unchanged(self):
        for name, value in self.raw.items():
            self.assertEqual((self.root / name).read_bytes(), value, name)

    def test_successful_revalidation_preserves_observations_and_audits_old_decision(self):
        result = MODULE.revalidate_run(self.path)
        self.assertEqual(result["status"], "VERIFIED")
        data = json.loads(self.path.read_text())
        self.assertEqual(data["experimentStatus"], "FINALIZED")
        self.assertEqual(data["coordination"]["observationCoverage"], "COMPLETE")
        self.assertEqual(len(data["revalidationHistory"]), 2)
        audit = data["revalidationHistory"][-1]
        self.assertEqual(audit["previousFinalization"], self.original["finalization"])
        self.assertEqual(audit["previousExperimentStatus"], "NOT_FINALIZED")
        self.assertEqual(audit["outcome"], "VERIFIED")
        self.assertEqual(audit["newCriterion"]["timestampGranularityToleranceMs"], 1)
        self.assertEqual(audit["newCriterion"]["maximumReportingGapMs"], 2001)
        self.assertGreater(audit["observedAtEpochMs"], 0)
        self.assertEqual(data["revalidationHistory"][0], self.original["revalidationHistory"][0])
        for key in ("configuration", "observed", "clockAnchors", "scheduled", "components", "failures"):
            self.assertEqual(data[key], self.original[key])
        self.assertEqual(data["coordination"]["evidence"], self.original["coordination"]["evidence"])
        self.assertEqual(data["coordination"]["ownership"], self.original["coordination"]["ownership"])
        self.assert_raw_unchanged()

    def test_cli_is_explicit_and_starts_no_java_kafka_or_other_processes(self):
        with mock.patch.object(MODULE, "execute", side_effect=AssertionError("no supervision")), \
                mock.patch.object(MODULE, "verify_lock_compatibility", side_effect=AssertionError("no Java probe")), \
                mock.patch.object(MODULE.subprocess, "Popen", side_effect=AssertionError("no subprocess")), \
                mock.patch.object(MODULE.subprocess, "run", side_effect=AssertionError("no subprocess")), \
                contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(MODULE.main(["--revalidate-run", str(self.path)]), 0)
        self.assertIn("Revalidation persisted: FINALIZED", output.getvalue())
        self.assert_raw_unchanged()

    def test_blocked_coverage_preserves_original_metadata_bytes(self):
        data = copy.deepcopy(self.original)
        data["coordination"]["evidence"] = [e for e in data["coordination"]["evidence"] if
                e.get("evidence", {}).get("evidenceType") != "MEASUREMENT_CONSUMER_POLL_PROGRESS"]
        self.path.write_text(json.dumps(data))
        before = self.path.read_bytes()
        with self.assertRaisesRegex(MODULE.CoordinationError, "revalidation BLOCKED"):
            MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_bytes(), before)
        self.assert_raw_unchanged()

    def test_invalid_raw_csv_preserves_metadata_and_diagnostics(self):
        (self.root / "sensor-results.csv").write_text("invalid schema")
        with contextlib.redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(MODULE.main(["--revalidate-run", str(self.path)]), 1)
        self.assertIn("Run revalidation failed", errors.getvalue())
        self.assertEqual(self.path.read_bytes(), self.original_bytes)
        self.assertEqual((self.root / "errors.log").read_bytes(), self.raw["errors.log"])

    def test_already_finalized_run_is_never_modified_again(self):
        MODULE.revalidate_run(self.path)
        before = self.path.read_bytes()
        with self.assertRaisesRegex(MODULE.CoordinationError, "immutable"):
            MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_missing_compatible_ownership_blocks_without_a_java_probe(self):
        data = copy.deepcopy(self.original)
        data["coordination"]["ownership"]["locking"] = "UNKNOWN"
        self.path.write_text(json.dumps(data))
        before = self.path.read_bytes()
        with mock.patch.object(MODULE, "verify_lock_compatibility", side_effect=AssertionError("no Java")):
            with self.assertRaisesRegex(MODULE.CoordinationError, "compatible coordinator ownership"):
                MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_missing_process_exit_evidence_is_not_inferred(self):
        data = copy.deepcopy(self.original)
        data["coordination"]["evidence"] = [e for e in data["coordination"]["evidence"] if not
                (e.get("evidenceType") == "PROCESS_EXITED" and e.get("component") == "producer")]
        self.path.write_text(json.dumps(data))
        before = self.path.read_bytes()
        with self.assertRaisesRegex(MODULE.CoordinationError, "launches and exits"):
            MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_conflicting_producer_run_id_is_rejected(self):
        data = copy.deepcopy(self.original)
        data["configuration"]["producer"]["runId"] = "other-run"
        self.path.write_text(json.dumps(data))
        before = self.path.read_bytes()
        with self.assertRaisesRegex(MODULE.CoordinationError, "runId"):
            MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_bytes(), before)

    def test_atomic_replacement_failure_preserves_original_and_removes_temporary(self):
        with mock.patch.object(MODULE.os, "replace", side_effect=OSError("atomic replacement failed")):
            with self.assertRaisesRegex(OSError, "atomic replacement failed"):
                MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_bytes(), self.original_bytes)
        self.assertEqual(list(self.root.glob(".run-config-*.tmp")), [])
        self.assert_raw_unchanged()

    def test_atomic_replacement_is_under_lock_and_contains_complete_audit(self):
        replace = os.replace
        def checked(source, target):
            self.assertEqual(Path(source).parent, self.root)
            self.assertEqual(self.path.read_bytes(), self.original_bytes)
            staged = json.loads(Path(source).read_text())
            self.assertEqual(staged["experimentStatus"], "FINALIZED")
            self.assertEqual(staged["revalidationHistory"][-1]["previousFinalization"]["status"], "BLOCKED")
            # A separate process must still see this shared lock as busy at commit.
            code = "import fcntl,sys; f=open(sys.argv[1],'ab'); fcntl.lockf(f,fcntl.LOCK_EX|fcntl.LOCK_NB)"
            result = subprocess.run([sys.executable, "-c", code, str(self.path)+".lock"], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            replace(source, target)
        with mock.patch.object(MODULE.os, "replace", side_effect=checked):
            MODULE.revalidate_run(self.path)
        self.assert_raw_unchanged()

    def test_existing_lock_contention_rejects_revalidation_without_changes(self):
        code = "import fcntl,sys; f=open(sys.argv[1],'ab'); fcntl.lockf(f,fcntl.LOCK_EX); print('LOCKED',flush=True); sys.stdin.read(1)"
        child = subprocess.Popen([sys.executable, "-c", code, str(self.path)+".lock"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        try:
            self.assertEqual(child.stdout.readline().strip(), b"LOCKED")
            with self.assertRaisesRegex(MODULE.CoordinationError, "still owned"):
                MODULE.revalidate_run(self.path)
            self.assertEqual(self.path.read_bytes(), self.original_bytes)
        finally:
            child.communicate(b"x", timeout=5)
        self.assert_raw_unchanged()

    def test_missing_or_invalid_metadata_is_not_created_or_repaired(self):
        self.path.write_text("{broken")
        with self.assertRaises(ValueError):
            MODULE.revalidate_run(self.path)
        self.assertEqual(self.path.read_text(), "{broken")
        self.path.unlink()
        with self.assertRaises(FileNotFoundError):
            MODULE.revalidate_run(self.path)
        self.assertFalse(self.path.exists())



if __name__ == "__main__":
    unittest.main()
