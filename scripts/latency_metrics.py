"""Latency arithmetic for caller-selected Consumer result observations.

Accepts mappings such as csv.DictReader rows; performs no file I/O. The caller
owns originating-phase selection, drain cutoffs, and coverage validation before
using these values in definitive experimental results.
"""

from collections.abc import Mapping
import re


class LatencyValidationError(ValueError):
    """A result observation lacks valid identifiers or epoch timestamps."""


def _timestamp(value, location):
    if isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
        value = int(value)
    if type(value) is not int or not -(2**63) <= value < 2**63:
        raise LatencyValidationError(f"{location}: expected integer Unix epoch milliseconds")
    return value


def _percentile(values, percent):
    """Type 7: position=(n-1)*p, interpolating adjacent sorted observations."""
    if not values:
        return None
    lower, remainder = divmod((len(values) - 1) * percent, 100)
    if remainder == 0:
        return values[lower]
    return values[lower] + (values[lower + 1] - values[lower]) * remainder / 100


def calculate_latency_metrics(observations):
    """Return counts, valid-population percentiles in ms, and negative observations.

    Deduplicates by (runId, eventId), retaining the row with earliest
    resultReceivedAt and its sentAt. Equal receipt timestamps retain the first
    row. Every supplied row is validated, including duplicates. CSV latencyMs,
    duplicate, negativeLatency, and synthetic eventTime fields are not used.
    """
    earliest = {}
    total = 0
    for number, row in enumerate(observations, start=1):
        if not isinstance(row, Mapping):
            raise LatencyValidationError(f"observation {number}: expected a mapping")
        for field in ("runId", "eventId"):
            if not isinstance(row.get(field), str) or not row[field].strip():
                raise LatencyValidationError(f"observation {number}: missing or invalid {field}")
        sent = _timestamp(row.get("sentAt"), f"observation {number}:sentAt")
        received = _timestamp(row.get("resultReceivedAt"), f"observation {number}:resultReceivedAt")
        key = row["runId"], row["eventId"]
        if key not in earliest or received < earliest[key][0]:
            earliest[key] = received, sent
        total += 1

    valid = []
    negative = []
    for (run_id, event_id), (received, sent) in earliest.items():
        latency = received - sent
        if latency < 0:
            negative.append({"runId": run_id, "eventId": event_id, "sentAt": sent,
                             "resultReceivedAt": received, "latencyMs": latency})
        else:
            valid.append(latency)
    valid.sort()
    return {
        "uniqueResults": len(earliest),
        "validLatencyObservations": len(valid),
        "negativeLatencyObservations": len(negative),
        "duplicateResultObservations": total - len(earliest),
        "p50Ms": _percentile(valid, 50),
        "p95Ms": _percentile(valid, 95),
        "p99Ms": _percentile(valid, 99),
        "negativeLatencies": negative,
    }
