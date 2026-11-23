#!/usr/bin/env python3
"""Generate B1-B4 scientific outputs after observation finalization.

Usage: python3 scripts/generate-results.py --run-config PATH
Reads the finalized run's recorded raw paths. summary.json is published last as
report completion evidence. Reports do not change observation finalization.
"""
import argparse
import csv
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile


def load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


THROUGHPUT = load_module("report_throughput", "analyze-throughput.py")
LATENCY = load_module("report_latency", "latency_metrics.py")
RELIABILITY = load_module("report_reliability", "reliability_metrics.py")


def generate(run_config):
    """Validate and stage both reports; publish exclusively, rolling back failures."""
    run_config = Path(run_config).resolve()
    directory = run_config.parent
    published = []
    try:
        coordinator = THROUGHPUT.coordinator_module()
        with run_config.open(encoding="utf-8") as source:
            data = json.load(source, object_pairs_hook=coordinator.strict_json_object,
                             parse_constant=coordinator.reject_json_constant)
        THROUGHPUT.require_complete_run(data)
        run_id, start, end = THROUGHPUT.metadata(run_config)
        cutoff = THROUGHPUT.timestamp(data["coordination"]["drain"]["scheduledCutoffEpochMs"], "drain cutoff")
        producer_path = Path(data["configuration"]["producer"]["eventCsvOutput"])
        consumer_path = Path(data["coordination"]["configurationEvidence"]["consumer"]["resultCsvOutput"])
        if any(os.path.lexists(directory / name) for name in ("throughput.csv", "summary.json")):
            raise FileExistsError("scientific output already exists; refusing to overwrite")
        producers = [row for _, row in THROUGHPUT.records(producer_path,
            {"runId", "eventId", "phase", "status", "recordedAt", "sentAt"})]
        results = [row for _, row in THROUGHPUT.records(consumer_path,
            {"runId", "eventId", "sentAt", "resultReceivedAt"})]
        phases = {(row["runId"], row["eventId"]): row["phase"] for row in producers}
        # Original phase and actual observation time remain separate dimensions.
        selected_producer = [row for row in producers if row["phase"] == "MEASUREMENT"
                             and int(row["recordedAt"]) <= cutoff]
        selected_results = [row for row in results if phases[(row["runId"], row["eventId"])] == "MEASUREMENT"
                            and int(row["resultReceivedAt"]) <= cutoff]
        latency = LATENCY.calculate_latency_metrics(selected_results)
        reliability = RELIABILITY.calculate_reliability_metrics(selected_producer, selected_results, run_id)
        # Preserve the calculator's provisional fields; coverage-qualified absence
        # is separately named and is never asserted to be proven Kafka data loss.
        reliability["confirmedEventsWithoutObservedResultAfterCutoff"] = reliability["provisionalMissingResultCount"]
        reliability["provenKafkaDataLossCount"] = None
        with tempfile.TemporaryDirectory(prefix=".analysis-", dir=directory) as temporary:
            stage = Path(temporary)
            THROUGHPUT.analyze(run_config, producer_path, consumer_path, stage / "throughput.csv")
            with (stage / "throughput.csv").open(newline="", encoding="utf-8") as source:
                bins = list(csv.DictReader(source))
            throughput = {}
            for column in ("attempted", "confirmed", "uniqueResults"):
                counts = [int(row[column]) for row in bins]
                throughput[column] = {"total": sum(counts), "meanEventsPerSecond": sum(counts) / len(counts),
                                      "minimumEventsPerSecond": min(counts), "maximumEventsPerSecond": max(counts)}
            summary = {"schemaVersion": 1, "reportStatus": "COMPLETE", "runId": run_id,
                       "scenario": data["scenario"], "experimentStatus": "FINALIZED",
                       "measurementInterval": {"startEpochMs": start, "deadlineEpochMs": end,
                                               "bounds": "[start, deadline)", "durationSeconds": len(bins)},
                       "observationCutoffEpochMs": cutoff, "cutoffInclusive": True,
                       "populations": {
                           "throughput": "Original MEASUREMENT events; each observation inside [measurementStart, measurementDeadline); earliest observation per identity/status; zero bins included.",
                           "latencyAndReliability": "Original MEASUREMENT events; observations at or before the drain cutoff; earliest result per (runId,eventId) for latency; all eligible deliveries for duplicate counts.",
                           "latencyPercentiles": "Type 7 over nonnegative unique latencies, resultReceivedAt - sentAt; negative observations preserved separately.",
                           "reliability": "Confirmed events without observed results after verified operational coverage are observed absences, not proven Kafka data loss."},
                       "units": {"throughput": "events/s", "latency": "ms", "timestamps": "Unix epoch ms"},
                       "operationalCoverage": data["finalization"]["operationalCoverage"],
                       "throughput": throughput, "latency": latency, "reliability": reliability}
            if data["scenario"] == "B4":
                summary["b4"] = THROUGHPUT.b4_analysis(data, producers, results)
            with (stage / "summary.json").open("x", encoding="utf-8") as destination:
                json.dump(summary, destination, indent=2, allow_nan=False)
                destination.write("\n")
            # Flush staged bytes before publication. Hard links provide atomic,
            # exclusive publication on this same filesystem (no replacing files).
            for name in ("throughput.csv", "summary.json"):
                with (stage / name).open("rb") as source:
                    os.fsync(source.fileno())
                target = directory / name
                inode = (stage / name).stat().st_ino
                os.link(stage / name, target)
                published.append((target, inode))
        return summary
    except BaseException as error:
        for path, inode in reversed(published):
            try:
                if path.stat().st_ino == inode:
                    path.unlink()
            except OSError as cleanup:
                error.add_note(f"report cleanup failed: {cleanup}")
        try:
            with (directory / "errors.log").open("a", encoding="utf-8") as diagnostics:
                diagnostics.write(f"[analysis] {type(error).__name__}: {error}\n")
                for note in getattr(error, "__notes__", []):
                    diagnostics.write(f"[analysis] {note}\n")
        except OSError as diagnostic:
            error.add_note(f"analysis diagnostics failed: {diagnostic}")
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-config", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        generate(args.run_config)
    except (OSError, ValueError, KeyError, csv.Error) as error:
        print(f"Scientific report generation failed: {error}", file=sys.stderr)
        return 1
    print(f"Scientific reports generated in {args.run_config.resolve().parent}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
