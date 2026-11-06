"""Reliability accounting for one caller-selected run, without file I/O.

The caller owns phase/cutoff selection and coverage checks. Missing-result
candidates are provisional; definitive missing counts are always unavailable.
"""

from collections.abc import Mapping


class ReliabilityValidationError(ValueError):
    """Malformed observations or inconsistent run/originating-event evidence."""


def _text(value, location):
    if not isinstance(value, str) or not value.strip():
        raise ReliabilityValidationError(f"{location}: required nonblank text")
    return value


def _identity(row, location):
    if not isinstance(row, Mapping):
        raise ReliabilityValidationError(f"{location}: expected a mapping")
    return (_text(row.get("runId"), f"{location}:runId"),
            _text(row.get("eventId"), f"{location}:eventId"))


def calculate_reliability_metrics(producer_observations, consumer_observations, run_id=None):
    """Count unique statuses and deliveries, retaining errors and anomalies.

    Required fields: Producer runId/eventId/status; Consumer runId/eventId.
    Optional Producer error must be text or null. Other CSV fields are untouched
    and unused: this component neither schedules phases nor validates clocks.
    A supplied run_id is enforced; otherwise infer one from the first row and
    reject mixed runs. Repeated Producer statuses count once per identity.
    """
    if run_id is not None:
        _text(run_id, "run_id")
    statuses = {}
    errors = set()
    for number, row in enumerate(producer_observations, start=1):
        location = f"Producer observation {number}"
        key = _identity(row, location)
        if run_id is None:
            run_id = key[0]
        if key[0] != run_id:
            raise ReliabilityValidationError(f"{location}: runId mismatch")
        status = _text(row.get("status"), f"{location}:status")
        if status not in {"ATTEMPTED", "CONFIRMED", "FAILED"}:
            raise ReliabilityValidationError(f"{location}: unknown publication status {status!r}")
        error = row.get("error")
        if error is not None and not isinstance(error, str):
            raise ReliabilityValidationError(f"{location}:error must be text or null")
        statuses.setdefault(key, set()).add(status)
        if status == "FAILED":
            errors.add((key, error))

    for key, recorded in statuses.items():
        if "ATTEMPTED" not in recorded:
            raise ReliabilityValidationError(f"missing originating ATTEMPTED record for {key}")

    results = set()
    deliveries = 0
    for number, row in enumerate(consumer_observations, start=1):
        location = f"Consumer observation {number}"
        key = _identity(row, location)
        if run_id is None:
            run_id = key[0]
        if key[0] != run_id:
            raise ReliabilityValidationError(f"{location}: runId mismatch")
        if key not in statuses:
            raise ReliabilityValidationError(f"{location}: no matching Producer ATTEMPTED record for {key}")
        results.add(key)
        deliveries += 1

    confirmed = {key for key, recorded in statuses.items() if "CONFIRMED" in recorded}
    failed = {key for key, recorded in statuses.items() if "FAILED" in recorded}
    unresolved = set(statuses) - (confirmed | failed)
    candidates = [{"runId": run, "eventId": event} for run, event in sorted(confirmed - results)]
    anomalies = []
    for (run, event), recorded in sorted(statuses.items()):
        if "CONFIRMED" in recorded and "FAILED" in recorded:
            anomalies.append({"runId": run, "eventId": event,
                              "type": "CONFLICTING_PRODUCER_OUTCOMES",
                              "statuses": ["CONFIRMED", "FAILED"]})
        if (run, event) in results and "CONFIRMED" not in recorded:
            anomalies.append({"runId": run, "eventId": event,
                              "type": "RESULT_WITHOUT_RECORDED_CONFIRMATION",
                              "statuses": sorted(recorded)})
    publication_errors = [
        {"runId": run, "eventId": event, "error": error}
        for ((run, event), error) in sorted(errors, key=lambda item: (
            item[0], item[1] is not None, item[1] or ""))
    ]
    return {
        "runId": run_id,
        "uniqueAttemptedEvents": len(statuses),
        "uniqueConfirmedEvents": len(confirmed),
        "failedPublicationOutcomes": len(failed),
        "attemptsWithoutRecordedOutcome": len(unresolved),
        "uniqueConsumerResults": len(results),
        "duplicateConsumerResultObservations": deliveries - len(results),
        "provisionalMissingResultCount": len(candidates),
        "provisionalMissingResultCandidates": candidates,
        "definitiveMissingResultCount": None,
        "publicationErrors": publication_errors,
        "anomalies": anomalies,
    }
