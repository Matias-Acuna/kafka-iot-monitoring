"""Tests calculation only; fixtures do not certify experimental completeness."""

import copy
import csv
import importlib.util
import io
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "latency_metrics", Path(__file__).resolve().parents[1] / "latency_metrics.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
calculate = MODULE.calculate_latency_metrics


class LatencyMetricsTest(unittest.TestCase):
    def row(self, event="event-1", sent=1700000000000, received=1700000000010, run="run-1"):
        return {"runId": run, "eventId": event, "sentAt": sent, "resultReceivedAt": received}

    def assert_percentiles(self, metrics, p50, p95, p99):
        for field, expected in (("p50Ms", p50), ("p95Ms", p95), ("p99Ms", p99)):
            if expected is None:
                self.assertIsNone(metrics[field])
            else:
                self.assertAlmostEqual(metrics[field], expected)

    def test_one_valid_latency(self):
        result = calculate([self.row()])
        self.assertEqual(result, {
            "uniqueResults": 1, "validLatencyObservations": 1,
            "negativeLatencyObservations": 0, "duplicateResultObservations": 0,
            "p50Ms": 10, "p95Ms": 10, "p99Ms": 10, "negativeLatencies": []})

    def test_type_seven_interpolation_for_multiple_latencies(self):
        rows = [self.row(str(index), 1000, 1000 + latency)
                for index, latency in enumerate((0, 10, 20, 30, 40))]
        result = calculate(rows)
        self.assert_percentiles(result, 20, 38, 39.6)
        self.assertEqual(result["validLatencyObservations"], 5)

    def test_two_values_interpolate_instead_of_nearest_rank(self):
        result = calculate([self.row("a", 100, 110), self.row("b", 100, 130)])
        self.assert_percentiles(result, 20, 29, 29.8)

    def test_unsorted_input_is_sorted_for_percentiles(self):
        result = calculate([self.row("c", 100, 140), self.row("a", 100, 100),
                            self.row("b", 100, 120)])
        self.assert_percentiles(result, 20, 38, 39.6)

    def test_duplicate_results_select_earliest_receipt_not_first_row(self):
        rows = [self.row(received=1700000000050), self.row(), self.row()]
        result = calculate(rows)
        self.assertEqual(result["uniqueResults"], 1)
        self.assertEqual(result["duplicateResultObservations"], 2)
        self.assertEqual(result["validLatencyObservations"], 1)
        self.assert_percentiles(result, 10, 10, 10)

    def test_earliest_receipt_retains_its_own_sent_timestamp(self):
        result = calculate([self.row(sent=200, received=250), self.row(sent=100, received=120)])
        self.assert_percentiles(result, 20, 20, 20)

    def test_negative_latency_is_retained_and_excluded_from_percentiles(self):
        result = calculate([self.row("negative", 200, 190), self.row("valid", 200, 220)])
        self.assertEqual(result["uniqueResults"], 2)
        self.assertEqual(result["validLatencyObservations"], 1)
        self.assertEqual(result["negativeLatencyObservations"], 1)
        self.assertEqual(result["negativeLatencies"], [{
            "runId": "run-1", "eventId": "negative", "sentAt": 200,
            "resultReceivedAt": 190, "latencyMs": -10}])
        self.assert_percentiles(result, 20, 20, 20)

    def test_later_positive_duplicate_does_not_replace_earlier_negative(self):
        result = calculate([self.row(received=1700000000005),
                            self.row(received=1699999999995)])
        self.assertEqual(result["negativeLatencyObservations"], 1)
        self.assertEqual(result["duplicateResultObservations"], 1)
        self.assertEqual(result["validLatencyObservations"], 0)
        self.assert_percentiles(result, None, None, None)

    def test_zero_latency_is_valid(self):
        result = calculate([self.row(sent=100, received=100)])
        self.assertEqual(result["validLatencyObservations"], 1)
        self.assertEqual(result["negativeLatencyObservations"], 0)
        self.assert_percentiles(result, 0, 0, 0)

    def test_empty_dataset_has_unavailable_percentiles(self):
        result = calculate([])
        for field in ("uniqueResults", "validLatencyObservations",
                      "negativeLatencyObservations", "duplicateResultObservations"):
            self.assertEqual(result[field], 0)
        self.assertEqual(result["negativeLatencies"], [])
        self.assert_percentiles(result, None, None, None)

    def test_all_negative_population_has_unavailable_percentiles(self):
        result = calculate([self.row("a", 100, 99), self.row("b", 100, 98)])
        self.assertEqual(result["negativeLatencyObservations"], 2)
        self.assert_percentiles(result, None, None, None)

    def test_malformed_timestamps_are_rejected_in_both_fields(self):
        for field in ("sentAt", "resultReceivedAt"):
            for value in (None, "", " ", "NaN", "Infinity", "1.5", 1.0, True,
                          "123ms", [], {}, 2**63, -(2**63) - 1):
                with self.subTest(field=field, value=value):
                    row = self.row()
                    row[field] = value
                    with self.assertRaisesRegex(MODULE.LatencyValidationError, field):
                        calculate([row])

    def test_missing_required_fields_are_rejected(self):
        for field in ("runId", "eventId", "sentAt", "resultReceivedAt"):
            with self.subTest(field=field):
                row = self.row()
                del row[field]
                with self.assertRaisesRegex(MODULE.LatencyValidationError, field):
                    calculate([row])

    def test_invalid_identifiers_and_non_mapping_rows_are_rejected(self):
        for field in ("runId", "eventId"):
            for value in (None, "", " ", 123):
                row = self.row()
                row[field] = value
                with self.assertRaisesRegex(MODULE.LatencyValidationError, field):
                    calculate([row])
        for row in (None, [], "invalid"):
            with self.assertRaisesRegex(MODULE.LatencyValidationError, "mapping"):
                calculate([row])

    def test_malformed_duplicate_is_not_silently_discarded(self):
        duplicate = self.row()
        duplicate["sentAt"] = "invalid"
        with self.assertRaisesRegex(MODULE.LatencyValidationError, "sentAt"):
            calculate([self.row(), duplicate])

    def test_same_event_id_in_different_runs_is_not_duplicate(self):
        result = calculate([self.row(run="run-a"), self.row(run="run-b")])
        self.assertEqual(result["uniqueResults"], 2)
        self.assertEqual(result["duplicateResultObservations"], 0)

    def test_actual_csv_schema_ignores_payload_time_and_derived_fields(self):
        source = io.StringIO(
            "runId,eventId,deviceId,eventTime,sentAt,ruleType,triggered,"
            "resultReceivedAt,latencyMs,duplicate,negativeLatency\n"
            "run-1,event-1,device-1,999999,100,HIGH_TEMPERATURE,true,120,999,true,true\n")
        result = calculate(csv.DictReader(source))
        self.assert_percentiles(result, 20, 20, 20)
        self.assertEqual(result["duplicateResultObservations"], 0)
        self.assertEqual(result["negativeLatencyObservations"], 0)

    def test_input_data_and_order_are_preserved(self):
        rows = [self.row("b"), self.row("a", 200, 190), self.row("b")]
        before = copy.deepcopy(rows)
        result = calculate(iter(rows))
        self.assertEqual(rows, before)
        result["negativeLatencies"][0]["sentAt"] = 0
        self.assertEqual(rows, before)

    def test_signed_epoch_timestamps_and_exact_integer_subtraction(self):
        result = calculate([self.row("a", -100, -90), self.row("b", 2**63 - 11, 2**63 - 1)])
        self.assert_percentiles(result, 10, 10, 10)


if __name__ == "__main__":
    unittest.main()
