"""Broker-free throughput tests; all generated files live in temporary directories."""

import contextlib
import csv
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "analyze_throughput", Path(__file__).resolve().parents[1] / "analyze-throughput.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ThroughputTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.config, self.producer, self.consumer, self.output = (
            root / name for name in ("run-config.json", "producer-events.csv",
                                     "sensor-results.csv", "throughput.csv"))
        # Use a synthetic status and mock finalization to test
        # throughput calculations independently of the coordinator.
        self.completion_patch = patch.object(MODULE, "coordinator_completion_verified", return_value=True)
        self.completion_verifier = self.completion_patch.start()
        self.addCleanup(self.completion_patch.stop)
        self.data = {"runId": "run-1", "scenario": "B1", "experimentStatus": "TEST_ONLY_VERIFIED",
                     "scheduled": {"measurementStartEpochMs": 10000,
                                   "measurementDeadlineEpochMs": 13000}}
        self.save_config()
        self.inputs([], [])

    def save_config(self):
        self.config.write_text(json.dumps(self.data), encoding="utf-8")

    def write_csv(self, path, header, rows):
        with path.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.writer(destination)
            writer.writerow(header)
            writer.writerows(rows)

    def inputs(self, producer, consumer):
        self.write_csv(self.producer, ["runId", "eventId", "phase", "status", "recordedAt"], producer)
        self.write_csv(self.consumer, ["runId", "eventId", "resultReceivedAt"], consumer)

    def event(self, event_id, status, observed, phase="MEASUREMENT"):
        return ["run-1", event_id, phase, status, observed]

    def result(self, event_id, observed):
        return ["run-1", event_id, observed]

    def analyze(self):
        return MODULE.analyze(self.config, self.producer, self.consumer, self.output)

    def rows(self):
        self.analyze()
        with self.output.open(newline="", encoding="utf-8") as source:
            reader = csv.reader(source)
            self.assertEqual(next(reader), ["second", "attempted", "confirmed", "uniqueResults"])
            return [[int(value) for value in row] for row in reader]

    def test_independent_binning_and_empty_seconds(self):
        self.inputs([self.event("a", "ATTEMPTED", 10000),
                     self.event("a", "CONFIRMED", 12000)], [self.result("a", 12999)])
        self.assertEqual(self.rows(), [[0, 1, 0, 0], [1, 0, 0, 0], [2, 0, 1, 1]])

    def test_start_inclusive_and_deadline_exclusive(self):
        self.inputs([self.event("a", "ATTEMPTED", 10000),
                     self.event("a", "CONFIRMED", 10000),
                     self.event("b", "ATTEMPTED", 13000),
                     self.event("b", "CONFIRMED", 13000)],
                    [self.result("a", 10000), self.result("b", 13000)])
        self.assertEqual(self.rows(), [[0, 1, 1, 1], [1, 0, 0, 0], [2, 0, 0, 0]])

    def test_warm_up_origin_is_excluded_even_when_observed_during_measurement(self):
        self.inputs([self.event("a", "ATTEMPTED", 9999, "WARM_UP"),
                     self.event("a", "CONFIRMED", 11000, "WARM_UP")], [self.result("a", 11001)])
        self.assertEqual(self.rows(), [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0]])

    def test_late_confirmations_and_results_are_not_shifted_back(self):
        self.inputs([self.event("a", "ATTEMPTED", 12001),
                     self.event("a", "CONFIRMED", 14000)], [self.result("a", 14001)])
        original = self.producer.read_bytes(), self.consumer.read_bytes()
        self.assertEqual(self.rows(), [[0, 0, 0, 0], [1, 0, 0, 0], [2, 1, 0, 0]])
        self.assertEqual(original, (self.producer.read_bytes(), self.consumer.read_bytes()))

    def test_duplicate_producer_and_consumer_observations_use_earliest_timestamp(self):
        self.inputs([self.event("a", "ATTEMPTED", 12000),
                     self.event("a", "ATTEMPTED", 10000),
                     self.event("a", "CONFIRMED", 12001),
                     self.event("a", "CONFIRMED", 11000),
                     self.event("a", "CONFIRMED", 11000)],
                    [self.result("a", 12002), self.result("a", 11002), self.result("a", 11002)])
        self.assertEqual(self.rows(), [[0, 1, 0, 0], [1, 0, 1, 1], [2, 0, 0, 0]])

    def test_deduplication_precedes_measurement_window_filter(self):
        self.inputs([self.event("a", "ATTEMPTED", 9998),
                     self.event("a", "ATTEMPTED", 10001)],
                    [self.result("a", 11000), self.result("a", 9999)])
        self.assertEqual(self.rows(), [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0]])

    def test_missing_producer_match_is_an_error_even_for_late_results(self):
        self.inputs([], [self.result("unknown", 14000)])
        with self.assertRaisesRegex(MODULE.AnalysisError, "no matching Producer"):
            self.analyze()
        self.assertFalse(self.output.exists())

    def test_mismatched_run_ids_are_rejected_in_both_csvs(self):
        for producer_mismatch in (True, False):
            with self.subTest(producer=producer_mismatch):
                event = self.event("a", "ATTEMPTED", 10000)
                result = self.result("a", 10001)
                (event if producer_mismatch else result)[0] = "other-run"
                self.inputs([event], [result])
                with self.assertRaisesRegex(MODULE.AnalysisError, "runId differs"):
                    self.analyze()
                self.assertFalse(self.output.exists())

    def test_missing_input_files_are_not_zero_throughput(self):
        for path in (self.config, self.producer, self.consumer):
            with self.subTest(path=path):
                saved = path.read_bytes()
                path.unlink()
                with self.assertRaises(FileNotFoundError):
                    self.analyze()
                path.write_bytes(saved)
                self.assertFalse(self.output.exists())

    def test_invalid_metadata_and_intervals_are_rejected(self):
        invalid = [None, {}, {"runId": "run-1"}, {"runId": " "}]
        for start, end in ((13000, 10000), (10000, 10000), (10000, 10500),
                           (None, 13000), (True, 13000), (-1, 13000), (10000, "bad")):
            invalid.append({"runId": "run-1", "scheduled": {
                "measurementStartEpochMs": start, "measurementDeadlineEpochMs": end}})
        for data in invalid:
            with self.subTest(data=data):
                self.config.write_text(json.dumps(data), encoding="utf-8")
                with self.assertRaises(MODULE.AnalysisError):
                    self.analyze()
                self.assertFalse(self.output.exists())
        self.config.write_text("{broken", encoding="utf-8")
        with self.assertRaises(json.JSONDecodeError):
            self.analyze()

    def test_malformed_csv_headers_rows_and_required_fields(self):
        for content in ("", "wrong,header\n", "runId,runId,eventId,phase,status,recordedAt\n",
                        "runId,eventId,phase,status,recordedAt\nrun-1,a,MEASUREMENT\n",
                        "runId,eventId,phase,status,recordedAt\nrun-1,a,MEASUREMENT,ATTEMPTED,10000,extra\n",
                        'runId,eventId,phase,status,recordedAt\n"unterminated',
                        "runId,eventId,phase,status,recordedAt\nrun-1,,MEASUREMENT,ATTEMPTED,10000\n"):
            with self.subTest(content=content):
                self.producer.write_text(content, encoding="utf-8")
                with self.assertRaises((MODULE.AnalysisError, csv.Error)):
                    self.analyze()
                self.assertFalse(self.output.exists())

    def test_invalid_timestamps_status_and_phase_conflicts(self):
        for observed in ("NaN", "1.5", "-1", "", str(2**63)):
            with self.subTest(observed=observed):
                self.inputs([self.event("a", "ATTEMPTED", observed)], [])
                with self.assertRaises(MODULE.AnalysisError):
                    self.analyze()
                self.inputs([self.event("a", "ATTEMPTED", 10000)], [self.result("a", observed)])
                with self.assertRaises(MODULE.AnalysisError):
                    self.analyze()
        for events in ([self.event("a", "UNKNOWN", 10000)],
                       [self.event("a", "ATTEMPTED", 10000, "DRAIN")],
                       [self.event("a", "ATTEMPTED", 10000),
                        self.event("a", "CONFIRMED", 11000, "WARM_UP")]):
            self.inputs(events, [])
            with self.assertRaises(MODULE.AnalysisError):
                self.analyze()

    def test_payload_timestamps_and_duplicate_flags_do_not_control_binning(self):
        self.inputs([self.event("a", "ATTEMPTED", 10001)], [])
        self.write_csv(self.consumer,
                       ["runId", "eventId", "resultReceivedAt", "eventTime", "sentAt", "duplicate"],
                       [["run-1", "a", 12000, 10000, 10000, "false"],
                        ["run-1", "a", 11000, 10000, 10000, "true"]])
        self.assertEqual(self.rows(), [[0, 1, 0, 0], [1, 0, 0, 1], [2, 0, 0, 0]])

    def test_nested_configuration_run_id_must_match(self):
        self.data["configuration"] = {"producer": {"runId": "other-run"}}
        self.save_config()
        with self.assertRaisesRegex(MODULE.AnalysisError, "configuration.producer.runId"):
            self.analyze()

    def test_header_only_inputs_have_explicit_zero_bins(self):
        self.assertEqual(self.rows(), [[0, 0, 0, 0], [1, 0, 0, 0], [2, 0, 0, 0]])

    def test_standard_run_has_300_bins_and_short_run_uses_recorded_duration(self):
        for seconds in (300, 2):
            with self.subTest(seconds=seconds):
                self.data["scheduled"]["measurementDeadlineEpochMs"] = 10000 + seconds * 1000
                self.save_config()
                rows = self.rows()
                self.assertEqual(len(rows), seconds)
                self.assertEqual(rows[-1], [seconds - 1, 0, 0, 0])
                self.output.unlink()

    def test_existing_output_or_input_destination_is_never_overwritten(self):
        self.output.write_text("preserved", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            self.analyze()
        self.assertEqual(self.output.read_text(), "preserved")
        with self.assertRaises(FileExistsError):
            MODULE.analyze(self.config, self.producer, self.consumer, self.producer)

    def test_b4_is_rejected_when_identified_in_metadata(self):
        self.data["scenario"] = "B4"
        self.save_config()
        with self.assertRaisesRegex(MODULE.AnalysisError, "B4"):
            self.analyze()

    def test_not_finalized_metadata_is_rejected_even_with_header_only_csvs(self):
        self.data["experimentStatus"] = "NOT_FINALIZED"
        self.save_config()
        with self.assertRaisesRegex(MODULE.AnalysisError, "NOT_FINALIZED"):
            self.analyze()
        self.completion_verifier.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_missing_finalization_status_is_rejected(self):
        del self.data["experimentStatus"]
        self.save_config()
        with self.assertRaisesRegex(MODULE.AnalysisError, "experimentStatus"):
            self.analyze()
        self.assertFalse(self.output.exists())

    def test_absent_coverage_verification_rejects_header_only_and_nonempty_inputs(self):
        self.completion_patch.stop()
        for populated in (False, True):
            with self.subTest(populated=populated):
                if populated:
                    self.inputs([self.event("a", "ATTEMPTED", 10000)], [self.result("a", 10001)])
                with self.assertRaisesRegex(MODULE.AnalysisError, "FINALIZED"):
                    self.analyze()
                self.assertFalse(self.output.exists())

    def test_negative_or_missing_completeness_evidence_is_rejected(self):
        for evidence in (False, None):
            with self.subTest(evidence=evidence):
                self.completion_verifier.return_value = evidence
                with self.assertRaisesRegex(MODULE.AnalysisError, "missing verified"):
                    self.analyze()
                self.assertFalse(self.output.exists())

    def test_results_require_attempt_not_just_confirmation_or_failure(self):
        for status in ("CONFIRMED", "FAILED"):
            with self.subTest(status=status):
                self.inputs([self.event("a", status, 10000)], [self.result("a", 10001)])
                with self.assertRaisesRegex(MODULE.AnalysisError, "missing originating ATTEMPTED"):
                    self.analyze()
                self.assertFalse(self.output.exists())

    def test_orphan_producer_outcomes_are_rejected_without_consumer_results(self):
        self.inputs([self.event("a", "CONFIRMED", 10000)], [])
        with self.assertRaisesRegex(MODULE.AnalysisError, "missing originating ATTEMPTED"):
            self.analyze()

    def test_result_can_match_attempt_without_successful_confirmation(self):
        self.inputs([self.event("a", "FAILED", 11000),
                     self.event("a", "ATTEMPTED", 10000)], [self.result("a", 12000)])
        self.assertEqual(self.rows(), [[0, 1, 0, 0], [1, 0, 0, 0], [2, 0, 0, 1]])

    def test_explicit_supported_scenarios_pass_scope_check_with_verified_completeness(self):
        for scenario in ("B1", "B2", "B3"):
            with self.subTest(scenario=scenario):
                self.data["scenario"] = scenario
                self.save_config()
                self.assertEqual(self.analyze(), 3)
                self.output.unlink()

    def test_missing_unknown_and_b4_scenarios_are_rejected(self):
        for scenario in (None, "", "unknown", "B4"):
            with self.subTest(scenario=scenario):
                if scenario is None:
                    self.data.pop("scenario", None)
                else:
                    self.data["scenario"] = scenario
                self.save_config()
                with self.assertRaisesRegex(MODULE.AnalysisError, "scenario must explicitly"):
                    self.analyze()
                self.assertFalse(self.output.exists())

    def test_cli_rejects_unverified_status_without_override(self):
        self.completion_patch.stop()
        arguments = ["--run-config", str(self.config), "--producer-events", str(self.producer),
                     "--consumer-results", str(self.consumer), "--output", str(self.output)]
        with contextlib.redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(MODULE.main(arguments), 1)
        self.assertIn("FINALIZED", errors.getvalue())
        self.assertFalse(self.output.exists())

    def test_cli_reports_errors_and_accepts_explicit_paths(self):
        arguments = ["--run-config", str(self.config), "--producer-events", str(self.producer),
                     "--consumer-results", str(self.consumer), "--output", str(self.output)]
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(MODULE.main(arguments), 0)
        self.assertIn("3 one-second bins", output.getvalue())
        with contextlib.redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(MODULE.main(arguments), 1)
        self.assertIn("Throughput analysis failed", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
