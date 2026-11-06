"""Calculation tests only; no experiment finalization or coverage claims."""

import copy
import csv
import importlib.util
import io
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location(
    "reliability_metrics", Path(__file__).resolve().parents[1] / "reliability_metrics.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
calculate = MODULE.calculate_reliability_metrics
ERROR = MODULE.ReliabilityValidationError


class ReliabilityMetricsTest(unittest.TestCase):
    def producer(self, status="ATTEMPTED", event="e1", run="r1", **extra):
        return {"runId": run, "eventId": event, "status": status, **extra}

    def consumer(self, event="e1", run="r1", **extra):
        return {"runId": run, "eventId": event, **extra}

    def test_successful_flow(self):
        result = calculate([self.producer(), self.producer("CONFIRMED")], [self.consumer()])
        self.assertEqual(result, {
            "runId": "r1", "uniqueAttemptedEvents": 1, "uniqueConfirmedEvents": 1,
            "failedPublicationOutcomes": 0, "attemptsWithoutRecordedOutcome": 0,
            "uniqueConsumerResults": 1, "duplicateConsumerResultObservations": 0,
            "provisionalMissingResultCount": 0, "provisionalMissingResultCandidates": [],
            "definitiveMissingResultCount": None, "publicationErrors": [], "anomalies": []})

    def test_confirmed_event_without_result_is_only_provisional(self):
        result = calculate([self.producer(), self.producer("CONFIRMED")], [])
        self.assertEqual(result["provisionalMissingResultCount"], 1)
        self.assertEqual(result["provisionalMissingResultCandidates"], [{"runId": "r1", "eventId": "e1"}])
        self.assertIsNone(result["definitiveMissingResultCount"])
        self.assertEqual(result["failedPublicationOutcomes"], 0)

    def test_failed_publication_is_separate_from_missing_results(self):
        result = calculate([self.producer(), self.producer("FAILED", error="Kafka rejected publication")], [])
        self.assertEqual(result["failedPublicationOutcomes"], 1)
        self.assertEqual(result["uniqueConfirmedEvents"], 0)
        self.assertEqual(result["attemptsWithoutRecordedOutcome"], 0)
        self.assertEqual(result["provisionalMissingResultCount"], 0)
        self.assertEqual(result["publicationErrors"], [
            {"runId": "r1", "eventId": "e1", "error": "Kafka rejected publication"}])

    def test_attempt_without_outcome_is_unresolved_not_missing(self):
        result = calculate([self.producer()], [])
        self.assertEqual(result["attemptsWithoutRecordedOutcome"], 1)
        self.assertEqual(result["provisionalMissingResultCount"], 0)
        self.assertEqual(result["failedPublicationOutcomes"], 0)

    def test_duplicate_consumer_deliveries_count_separately(self):
        result = calculate([self.producer(), self.producer("CONFIRMED")],
                           [self.consumer(duplicate="false"), self.consumer(duplicate="false"), self.consumer()])
        self.assertEqual(result["uniqueConsumerResults"], 1)
        self.assertEqual(result["duplicateConsumerResultObservations"], 2)

    def test_duplicate_producer_statuses_and_errors_do_not_inflate_counts(self):
        rows = [self.producer(), self.producer("FAILED", error="failure")]
        result = calculate(rows * 3, [])
        self.assertEqual(result["uniqueAttemptedEvents"], 1)
        self.assertEqual(result["failedPublicationOutcomes"], 1)
        self.assertEqual(len(result["publicationErrors"]), 1)
        result = calculate([self.producer(), self.producer("CONFIRMED"), self.producer("CONFIRMED")], [])
        self.assertEqual(result["uniqueConfirmedEvents"], 1)

    def test_caller_selected_phases_are_not_filtered_again(self):
        producer = [self.producer(event="warm", phase="WARM_UP"),
                    self.producer("CONFIRMED", "warm", phase="WARM_UP"),
                    self.producer(event="measurement", phase="MEASUREMENT")]
        result = calculate(producer, [self.consumer("warm"), self.consumer("measurement")])
        self.assertEqual(result["uniqueAttemptedEvents"], 2)
        self.assertEqual(result["uniqueConsumerResults"], 2)
        self.assertEqual(result["attemptsWithoutRecordedOutcome"], 1)

    def test_unknown_consumer_identity_is_rejected(self):
        with self.assertRaisesRegex(ERROR, "no matching Producer ATTEMPTED"):
            calculate([self.producer()], [self.consumer("unknown")])
        with self.assertRaisesRegex(ERROR, "no matching Producer ATTEMPTED"):
            calculate([], [self.consumer()])

    def test_outcomes_require_attempt_even_if_rows_are_unsorted(self):
        for status in ("CONFIRMED", "FAILED"):
            with self.subTest(status=status):
                with self.assertRaisesRegex(ERROR, "missing originating ATTEMPTED"):
                    calculate([self.producer(status)], [])
        result = calculate([self.producer("CONFIRMED"), self.producer()], [self.consumer()])
        self.assertEqual(result["uniqueConfirmedEvents"], 1)

    def test_missing_and_malformed_required_fields_are_rejected(self):
        for field in ("runId", "eventId", "status"):
            for value in (None, "", " ", 123):
                with self.subTest(field=field, value=value):
                    row = self.producer()
                    row[field] = value
                    with self.assertRaisesRegex(ERROR, field):
                        calculate([row], [])
            row = self.producer()
            del row[field]
            with self.assertRaisesRegex(ERROR, field):
                calculate([row], [])
        for field in ("runId", "eventId"):
            row = self.consumer()
            del row[field]
            with self.assertRaisesRegex(ERROR, field):
                calculate([self.producer()], [row])
        for bad in (None, [], "invalid"):
            with self.assertRaisesRegex(ERROR, "mapping"):
                calculate([bad], [])
            with self.assertRaisesRegex(ERROR, "mapping"):
                calculate([self.producer()], [bad])

    def test_unknown_status_and_malformed_optional_error_are_rejected(self):
        with self.assertRaisesRegex(ERROR, "unknown publication status"):
            calculate([self.producer("SUCCESS")], [])
        with self.assertRaisesRegex(ERROR, "error must be text"):
            calculate([self.producer(), self.producer("FAILED", error=123)], [])

    def test_conflicting_outcomes_are_retained_as_anomalies(self):
        result = calculate([self.producer("FAILED", error="failed"),
                            self.producer("CONFIRMED"), self.producer()], [])
        self.assertEqual(result["uniqueConfirmedEvents"], 1)
        self.assertEqual(result["failedPublicationOutcomes"], 1)
        self.assertEqual(result["provisionalMissingResultCount"], 1)
        self.assertEqual(result["anomalies"], [{
            "runId": "r1", "eventId": "e1", "type": "CONFLICTING_PRODUCER_OUTCOMES",
            "statuses": ["CONFIRMED", "FAILED"]}])
        self.assertEqual(result["publicationErrors"][0]["error"], "failed")

    def test_result_without_confirmation_does_not_fabricate_success(self):
        for failed in (False, True):
            with self.subTest(failed=failed):
                rows = [self.producer()]
                if failed:
                    rows.append(self.producer("FAILED"))
                result = calculate(rows, [self.consumer()])
                self.assertEqual(result["uniqueConfirmedEvents"], 0)
                self.assertEqual(result["provisionalMissingResultCount"], 0)
                self.assertEqual(result["anomalies"][0]["type"], "RESULT_WITHOUT_RECORDED_CONFIRMATION")

    def test_empty_inputs_leave_definitive_count_unavailable(self):
        result = calculate([], [])
        self.assertIsNone(result["runId"])
        self.assertIsNone(result["definitiveMissingResultCount"])
        for field in ("uniqueAttemptedEvents", "uniqueConfirmedEvents", "failedPublicationOutcomes",
                      "attemptsWithoutRecordedOutcome", "uniqueConsumerResults",
                      "duplicateConsumerResultObservations", "provisionalMissingResultCount"):
            self.assertEqual(result[field], 0)
        self.assertEqual(calculate([], [], run_id="known")["runId"], "known")

    def test_run_id_mismatches_are_rejected(self):
        for producer, consumer in (([self.producer(), self.producer(run="other")], []),
                                   ([self.producer()], [self.consumer(run="other")])):
            with self.assertRaisesRegex(ERROR, "runId mismatch"):
                calculate(producer, consumer)
        with self.assertRaisesRegex(ERROR, "runId mismatch"):
            calculate([self.producer()], [], run_id="other")
        with self.assertRaisesRegex(ERROR, "run_id"):
            calculate([], [], run_id=" ")

    def test_original_observations_are_preserved(self):
        rows = [self.producer(), self.producer("FAILED", error="error")]
        results = [self.consumer(), self.consumer()]
        before = copy.deepcopy((rows, results))
        metrics = calculate(iter(rows), iter(results))
        metrics["publicationErrors"][0]["error"] = "changed"
        self.assertEqual((rows, results), before)

    def test_csv_schemas_are_accepted_without_timestamp_or_phase_arithmetic(self):
        producer = csv.DictReader(io.StringIO(
            "runId,eventId,deviceId,phase,recordedAt,sentAt,status,partition,offset,error\n"
            "r1,e1,d1,MEASUREMENT,100,90,ATTEMPTED,,,\n"
            "r1,e1,d1,MEASUREMENT,110,90,CONFIRMED,0,1,\n"))
        consumer = csv.DictReader(io.StringIO(
            "runId,eventId,deviceId,eventTime,sentAt,ruleType,triggered,"
            "resultReceivedAt,latencyMs,duplicate,negativeLatency\n"
            "r1,e1,d1,1,90,HIGH_TEMPERATURE,true,120,30,false,false\n"))
        result = calculate(producer, consumer)
        self.assertEqual(result["uniqueConsumerResults"], 1)
        self.assertEqual(result["provisionalMissingResultCount"], 0)

    def test_distinct_errors_are_retained_without_counting_multiple_failures(self):
        result = calculate([self.producer(), self.producer("FAILED", error="a"),
                            self.producer("FAILED", error="b"), self.producer("FAILED")], [])
        self.assertEqual(result["failedPublicationOutcomes"], 1)
        self.assertEqual([row["error"] for row in result["publicationErrors"]], [None, "a", "b"])


if __name__ == "__main__":
    unittest.main()
