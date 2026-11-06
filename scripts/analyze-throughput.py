#!/usr/bin/env python3
"""D.6.42 throughput only: second is zero-based from measurement start.

Each row is a full one-second interval; attempted, confirmed and uniqueResults
are event counts (events/s). Inputs are never changed. Scientific output requires revalidated coordinator
finalization and bounded operational observation coverage.
"""

import argparse
import importlib.util
from functools import lru_cache
import csv
import json
import re
import sys
from pathlib import Path


class AnalysisError(ValueError):
    """Missing or inconsistent analysis input."""


def timestamp(value, location):
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        value = int(value)
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise AnalysisError(f"{location}: expected nonnegative epoch milliseconds")
    return value


def required_text(value, location):
    if not isinstance(value, str) or not value.strip():
        raise AnalysisError(f"{location}: required nonblank text")
    return value


@lru_cache(maxsize=1)
def coordinator_module():
    spec = importlib.util.spec_from_file_location("result_coordinator", Path(__file__).with_name("run-experiment.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def coordinator_completion_verified(data):
    """Revalidate D.6.79-D.6.82 evidence and the actual raw files; no override."""
    try:
        if data.get("experimentStatus") != "FINALIZED":
            raise AnalysisError("experiment must be FINALIZED")
        finalization, coordination = data.get("finalization", {}), data.get("coordination", {})
        if (finalization.get("status") != "VERIFIED" or finalization.get("blockingReasons") != []
                or finalization.get("observationCoverage") != "COMPLETE"
                or coordination.get("observationCoverage") != "COMPLETE"
                or coordination.get("ownership", {}).get("owner") != "COORDINATOR"):
            raise AnalysisError("missing verified finalization/coverage/ownership evidence")
        producer = data["configuration"]["producer"]
        directory = Path(producer["runConfigOutput"]).resolve().parent
        if (Path(producer["runConfigOutput"]).resolve() != directory / "run-config.json"
                or Path(producer["eventCsvOutput"]).resolve() != directory / "producer-events.csv"
                or Path(coordination["configurationEvidence"]["consumer"]["resultCsvOutput"]).resolve()
                != directory / "sensor-results.csv"):
            raise AnalysisError("finalized observation paths do not match the supervised run directory")
        commands = {}
        for record in coordination.get("evidence", []):
            if record.get("evidenceType") == "PROCESS_STARTED":
                role = record.get("component")
                if role not in ("consumer", "streams", "producer") or role in commands:
                    raise AnalysisError("ambiguous component launch evidence")
                command = record.get("command")
                if not isinstance(command, list) or any(not isinstance(arg, str) for arg in command):
                    raise AnalysisError("invalid component launch command evidence")
                commands[role] = command
        if set(commands) != {"consumer", "streams", "producer"}:
            raise AnalysisError("missing component launch evidence")
        config = {"runId": data["runId"], "scenario": data["scenario"],
                  "outputDirectory": directory, "commands": commands}
        verified = coordinator_module().validate_finalization(data, coordination, config)
        if verified["status"] != "VERIFIED":
            raise AnalysisError("finalization revalidation failed: " + "; ".join(verified["blockingReasons"]))
        if any(finalization.get(key) != verified[key] for key in
               ("checks", "blockingReasons", "observationCoverage", "operationalCoverage")):
            raise AnalysisError("persisted finalization differs from revalidated D.6.82 evidence")
        return True
    except (KeyError, TypeError, AttributeError) as error:
        raise AnalysisError(f"incomplete finalization metadata: {error}") from error


def require_complete_run(data):
    status = required_text(data.get("experimentStatus"), "run-config.experimentStatus")
    if status == "NOT_FINALIZED":
        raise AnalysisError("experiment is NOT_FINALIZED; scientific throughput requires a complete run")
    if coordinator_completion_verified(data) is not True:
        raise AnalysisError("missing verified experiment finalization or observation coverage")


def metadata(path):
    with Path(path).open(encoding="utf-8") as source:
        data = json.load(source)
    if not isinstance(data, dict):
        raise AnalysisError("run-config: expected a JSON object")
    run_id = required_text(data.get("runId"), "run-config.runId")
    configuration = data.get("configuration")
    producer = configuration.get("producer") if isinstance(configuration, dict) else None
    if isinstance(producer, dict) and "runId" in producer and producer["runId"] != run_id:
        raise AnalysisError("configuration.producer.runId differs from run-config.runId")
    if data.get("scenario") not in ("B1", "B2", "B3"):
        raise AnalysisError("scenario must explicitly identify B1, B2 or B3; B4 is unsupported")
    scheduled = data.get("scheduled")
    if not isinstance(scheduled, dict):
        raise AnalysisError("run-config.scheduled: measurement boundaries are required")
    start = timestamp(scheduled.get("measurementStartEpochMs"), "measurementStartEpochMs")
    end = timestamp(scheduled.get("measurementDeadlineEpochMs"), "measurementDeadlineEpochMs")
    if end <= start or (end - start) % 1000:
        raise AnalysisError("measurement interval must be positive and contain whole seconds")
    require_complete_run(data)
    return run_id, start, end


def records(path, required):
    with Path(path).open(encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source, strict=True)
        headers = reader.fieldnames
        if (not headers or len(set(headers)) != len(headers)
                or not required.issubset(headers)):
            raise AnalysisError(f"{path}: missing or duplicate required CSV columns")
        for row in reader:
            location = f"{path}:line {reader.line_num}"
            if None in row or any(value is None for value in row.values()):
                raise AnalysisError(f"{location}: incorrect CSV field count")
            for column in required:
                required_text(row[column], f"{location}:{column}")
            yield location, row


def identity(row, run_id, location):
    if row["runId"] != run_id:
        raise AnalysisError(f"{location}: runId differs from run-config")
    return row["runId"], row["eventId"]


def analyze(run_config, producer_events, consumer_results, output):
    """Validate inputs completely before exclusively creating the output file."""
    run_id, start, end = metadata(run_config)
    # Bind explicit input paths to the files whose finalization was verified.
    with Path(run_config).open(encoding="utf-8") as source:
        data = json.load(source)
    if data.get("experimentStatus") == "FINALIZED":
        producer = data["configuration"]["producer"]
        expected = (producer["runConfigOutput"], producer["eventCsvOutput"],
                    data["coordination"]["configurationEvidence"]["consumer"]["resultCsvOutput"])
        if any(Path(actual).resolve() != Path(recorded).resolve() for actual, recorded in
               zip((run_config, producer_events, consumer_results), expected)):
            raise AnalysisError("analysis paths differ from the finalized run inputs")
    phases = {}
    outcomes = {}
    for location, row in records(producer_events,
                                 {"runId", "eventId", "phase", "status", "recordedAt"}):
        key = identity(row, run_id, location)
        phase, status = row["phase"], row["status"]
        if phase not in {"WARM_UP", "MEASUREMENT"}:
            raise AnalysisError(f"{location}: unsupported originating phase {phase!r}")
        if status not in {"ATTEMPTED", "CONFIRMED", "FAILED"}:
            raise AnalysisError(f"{location}: unsupported publication status {status!r}")
        if key in phases and phases[key] != phase:
            raise AnalysisError(f"{location}: conflicting originating phases for {key}")
        phases[key] = phase
        observed = timestamp(row["recordedAt"], f"{location}:recordedAt")
        observation_key = key, status
        outcomes[observation_key] = min(outcomes.get(observation_key, observed), observed)

    for key in phases:
        if (key, "ATTEMPTED") not in outcomes:
            raise AnalysisError(f"{producer_events}: missing originating ATTEMPTED record for {key}")

    first_results = {}
    for location, row in records(consumer_results, {"runId", "eventId", "resultReceivedAt"}):
        key = identity(row, run_id, location)
        observed = timestamp(row["resultReceivedAt"], f"{location}:resultReceivedAt")
        if key not in phases:
            raise AnalysisError(f"{location}: no matching Producer event for {key}")
        first_results[key] = min(first_results.get(key, observed), observed)

    bins = [[0, 0, 0] for _ in range((end - start) // 1000)]
    for (key, status), observed in outcomes.items():
        if phases[key] == "MEASUREMENT" and start <= observed < end:
            if status in {"ATTEMPTED", "CONFIRMED"}:
                column = 0 if status == "ATTEMPTED" else 1
                bins[(observed - start) // 1000][column] += 1
    for key, observed in first_results.items():
        if phases[key] == "MEASUREMENT" and start <= observed < end:
            bins[(observed - start) // 1000][2] += 1

    # Exclusive creation also protects input files accidentally selected as output.
    with Path(output).open("x", encoding="utf-8", newline="") as destination:
        writer = csv.writer(destination)
        writer.writerow(["second", "attempted", "confirmed", "uniqueResults"])
        for second, counts in enumerate(bins):
            writer.writerow([second, *counts])
    return len(bins)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-config", required=True, type=Path)
    parser.add_argument("--producer-events", required=True, type=Path)
    parser.add_argument("--consumer-results", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        count = analyze(args.run_config, args.producer_events, args.consumer_results, args.output)
    except (OSError, ValueError, csv.Error) as error:
        print(f"Throughput analysis failed: {error}", file=sys.stderr)
        return 1
    print(f"Wrote {count} one-second bins to {args.output}. "
          "Finalized observation evidence revalidated.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
