"""Scientific integration using complete synthetic finalized runs; no Kafka."""
import contextlib
import copy
import csv
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock

import test_run_experiment as COORDINATOR_TESTS
COORDINATOR = COORDINATOR_TESTS.MODULE

SPEC = importlib.util.spec_from_file_location("generate_results", Path(__file__).resolve().parents[1] / "generate-results.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class GenerateResultsTest(unittest.TestCase):
    def setUp(self):
        self.fixture = COORDINATOR_TESTS.OperationalCoverageTest()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.fixture.operational_fixture()
        self.root = self.fixture.output
        self.config = self.root / "run-config.json"
        self.start = self.fixture.data["scheduled"]["measurementStartEpochMs"]
        self.end = self.fixture.data["scheduled"]["measurementDeadlineEpochMs"]
        self.cutoff = self.fixture.coordination["drain"]["scheduledCutoffEpochMs"]
        self.fixture.producer_rows = []
        self.fixture.result_rows = []
        self.add_event("a", self.start, self.start + 1)
        self.add_result("a", self.start, self.start + 10)
        self.save_raw()
        producer_only = {key: value for key, value in self.fixture.data.items()
                         if key not in {"coordination", "finalization", "scenario"}}
        self.config.write_text(json.dumps(producer_only))
        COORDINATOR.persist_coordination.side_effect(self.fixture.validated, self.fixture.fake, self.fixture.coordination)
        self.assertEqual(self.data()["experimentStatus"], "FINALIZED")

    def data(self):
        return json.loads(self.config.read_text())

    def add_event(self, event, sent, outcome_at=None, phase="MEASUREMENT", status="CONFIRMED"):
        self.fixture.producer_rows.append(["run-1", event, "device-1", phase, str(sent), str(sent), "ATTEMPTED", "", "", ""])
        if outcome_at is not None:
            self.fixture.producer_rows.append(["run-1", event, "device-1", phase, str(outcome_at), str(sent), status,
                                               "0" if status == "CONFIRMED" else "",
                                               "1" if status == "CONFIRMED" else "", "error" if status == "FAILED" else ""])

    def add_result(self, event, sent, received, duplicate=False):
        latency = received - sent
        self.fixture.result_rows.append(["run-1", event, "device-1", "1", str(sent), "HIGH_TEMPERATURE", "true",
                                         str(received), str(latency), str(duplicate).lower(), str(latency < 0).lower()])

    def save_raw(self):
        self.fixture.write_raw()

    def generate(self):
        return MODULE.generate(self.config)

    def test_both_outputs_and_raw_metadata_preservation(self):
        originals = {name: (self.root / name).read_bytes() for name in
                     ("run-config.json", "producer-events.csv", "sensor-results.csv", "coordination.jsonl")}
        summary = self.generate()
        self.assertEqual(summary["runId"], "run-1")
        self.assertEqual(summary["scenario"], "B1")
        self.assertEqual(summary["reportStatus"], "COMPLETE")
        self.assertEqual(json.loads((self.root / "summary.json").read_text()), summary)
        with (self.root / "throughput.csv").open(newline="") as source:
            reader = csv.DictReader(source)
            self.assertEqual(reader.fieldnames, ["second", "attempted", "confirmed", "uniqueResults"])
            rows = list(reader)
        self.assertEqual(len(rows), 300)
        self.assertEqual(rows[0], {"second": "0", "attempted": "1", "confirmed": "1", "uniqueResults": "1"})
        self.assertEqual(sum(int(row["attempted"]) for row in rows[1:]), 0)
        self.assertEqual(summary["latency"]["p50Ms"], 10)
        self.assertEqual(summary["throughput"]["attempted"]["meanEventsPerSecond"], 1/300)
        for name, before in originals.items():
            self.assertEqual((self.root / name).read_bytes(), before)

    def test_phase_window_drain_cutoff_and_interpolated_latency(self):
        self.add_event("warm", self.start-1000, self.start+2, phase="WARM_UP")
        self.add_result("warm", self.start-1000, self.start+20)
        self.add_event("drain", self.end-1, self.end+100)
        self.add_result("drain", self.end-1, self.end+19)  # latency 20, throughput excluded.
        self.add_result("drain", self.end-1, self.end+29, duplicate=True)
        self.add_event("cutoff", self.end-2, self.end-1)
        self.add_result("cutoff", self.end-2, self.cutoff)
        self.add_event("too-late", self.end-3, self.end-2)
        self.add_result("too-late", self.end-3, self.cutoff+1)
        self.save_raw()
        summary = self.generate()
        self.assertEqual(summary["throughput"]["attempted"]["total"], 4)
        self.assertEqual(summary["throughput"]["confirmed"]["total"], 3)
        self.assertEqual(summary["throughput"]["uniqueResults"]["total"], 1)
        self.assertEqual(summary["latency"]["uniqueResults"], 3)
        self.assertEqual(summary["latency"]["duplicateResultObservations"], 1)
        self.assertEqual(summary["latency"]["p50Ms"], 20)
        self.assertAlmostEqual(summary["latency"]["p95Ms"], 20 + (120002-20)*0.9)
        self.assertEqual(summary["reliability"]["confirmedEventsWithoutObservedResultAfterCutoff"], 1)

    def test_failed_publications_and_unresolved_attempts_are_separate(self):
        self.add_event("failed", self.start+20, self.start+21, status="FAILED")
        self.add_event("unresolved", self.start+30)
        self.add_event("missing", self.start+40, self.start+41)
        self.save_raw()
        metrics = self.generate()["reliability"]
        self.assertEqual(metrics["failedPublicationOutcomes"], 1)
        self.assertEqual(metrics["attemptsWithoutRecordedOutcome"], 1)
        self.assertEqual(metrics["confirmedEventsWithoutObservedResultAfterCutoff"], 1)
        self.assertIsNone(metrics["provenKafkaDataLossCount"])
        self.assertEqual(metrics["publicationErrors"][0]["error"], "error")

    def test_empty_latency_population_is_unavailable(self):
        self.fixture.result_rows = []
        self.save_raw()
        latency = self.generate()["latency"]
        self.assertEqual(latency["validLatencyObservations"], 0)
        for name in ("p50Ms", "p95Ms", "p99Ms"):
            self.assertIsNone(latency[name])

    def test_negative_latency_is_preserved_and_not_in_percentiles(self):
        self.add_event("negative", self.start+100, self.start+101)
        self.add_result("negative", self.start+100, self.start+90)
        self.save_raw()
        latency = self.generate()["latency"]
        self.assertEqual(latency["negativeLatencyObservations"], 1)
        self.assertEqual(latency["negativeLatencies"][0]["latencyMs"], -10)
        self.assertEqual(latency["p99Ms"], 10)

    def test_not_finalized_and_forged_coverage_are_rejected(self):
        original = self.data()
        for altered in (dict(original, experimentStatus="NOT_FINALIZED"),
                        dict(original, finalization={"status": "VERIFIED"})):
            self.config.write_text(json.dumps(altered))
            with self.assertRaises(ValueError):
                self.generate()
            self.assertFalse((self.root / "summary.json").exists())
        self.assertIn("[analysis]", (self.root / "errors.log").read_text())

    def test_evidence_gap_is_revalidated_despite_finalized_status(self):
        data = self.data()
        data["coordination"]["evidence"] = [e for e in data["coordination"]["evidence"]
            if e.get("evidence", {}).get("evidenceType") != "MEASUREMENT_CONSUMER_POLL_PROGRESS"]
        self.config.write_text(json.dumps(data))
        with self.assertRaises(ValueError):
            self.generate()
        self.assertFalse((self.root / "throughput.csv").exists())

    def test_missing_malformed_and_wrong_run_inputs_are_rejected(self):
        producer = self.root / "producer-events.csv"
        original = producer.read_bytes()
        for invalid in (b"bad schema", original.replace(b"run-1", b"other")):
            producer.write_bytes(invalid)
            with self.assertRaises(ValueError):
                self.generate()
        producer.unlink()
        with self.assertRaises(ValueError):
            self.generate()
        self.assertFalse((self.root / "summary.json").exists())

    def test_existing_outputs_are_never_replaced(self):
        for name in ("throughput.csv", "summary.json"):
            path = self.root / name
            path.write_text("prior report")
            with self.assertRaises(FileExistsError):
                self.generate()
            self.assertEqual(path.read_text(), "prior report")
            path.unlink()

    def test_second_publication_failure_rolls_back_first_output(self):
        real_link = MODULE.os.link
        def link(source, target):
            if Path(target).name == "summary.json":
                raise OSError("summary publication failed")
            real_link(source, target)
        metadata_before = self.config.read_bytes()
        with mock.patch.object(MODULE.os, "link", side_effect=link):
            with self.assertRaisesRegex(OSError, "summary publication"):
                self.generate()
        self.assertFalse((self.root / "throughput.csv").exists())
        self.assertFalse((self.root / "summary.json").exists())
        self.assertEqual(self.config.read_bytes(), metadata_before)
        self.assertIn("summary publication failed", (self.root / "errors.log").read_text())
        self.assertEqual(list(self.root.glob(".analysis-*")), [])

    def test_summary_serialization_failure_publishes_nothing(self):
        with mock.patch.object(MODULE.json, "dump", side_effect=ValueError("serialization failed")):
            with self.assertRaisesRegex(ValueError, "serialization failed"):
                self.generate()
        self.assertFalse((self.root / "throughput.csv").exists())
        self.assertFalse((self.root / "summary.json").exists())

    def test_b4_and_old_coverage_policy_are_rejected(self):
        original = self.data()
        for change in ("scenario", "policy"):
            data = copy.deepcopy(original)
            if change == "scenario":
                data["scenario"] = "B4"
            else:
                data["finalization"]["operationalCoverage"]["maximumReportingGapMs"] = 302000
            self.config.write_text(json.dumps(data))
            with self.assertRaises(ValueError):
                self.generate()
            self.assertFalse((self.root / "summary.json").exists())

    def test_explicit_throughput_inputs_must_belong_to_finalized_run(self):
        replacement = self.root / "other-results.csv"
        replacement.write_bytes((self.root / "sensor-results.csv").read_bytes())
        with self.assertRaisesRegex(ValueError, "paths differ"):
            MODULE.THROUGHPUT.analyze(self.config, self.root / "producer-events.csv", replacement, self.root / "throughput.csv")
        self.assertFalse((self.root / "throughput.csv").exists())

    def test_concurrent_summary_creation_is_preserved_and_own_csv_is_removed(self):
        real_link = MODULE.os.link
        def link(source, target):
            if Path(target).name == "summary.json":
                Path(target).write_text("other writer's report")
            real_link(source, target)
        with mock.patch.object(MODULE.os, "link", side_effect=link):
            with self.assertRaises(FileExistsError):
                self.generate()
        self.assertFalse((self.root / "throughput.csv").exists())
        self.assertEqual((self.root / "summary.json").read_text(), "other writer's report")

    def test_cli_reports_failure_and_success(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(MODULE.main(["--run-config", str(self.config)]), 0)
        with contextlib.redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(MODULE.main(["--run-config", str(self.config)]), 1)
        self.assertIn("Scientific report generation failed", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
