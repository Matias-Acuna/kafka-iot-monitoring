#!/usr/bin/env python3
"""Measurement throughput uses one-second intervals indexed from measurement start.

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
    """Revalidate coordinator evidence against the recorded run data and raw files."""
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
        coordinator = coordinator_module()
        # The coordinator records the expected processes separately from their actual launches.

        # Never infer a missing second instance away from incomplete launch evidence.
        try:
            roles = coordinator.command_roles({"scenario": data["scenario"],
                                               "commands": coordination["components"]})
        except ValueError as error:
            raise AnalysisError(f"invalid supervised process inventory: {error}") from error
        if not set(coordination["configurationEvidence"]).issubset(roles):
            raise AnalysisError("unsupported configuration evidence process role")
        commands = {}
        for record in coordination.get("evidence", []):
            if "component" in record and record["component"] not in roles:
                raise AnalysisError("unsupported supervised process role")
            if record.get("evidenceType") == "PROCESS_STARTED":
                role = record.get("component")
                if role not in roles or role in commands:
                    raise AnalysisError("ambiguous component launch evidence")
                command = record.get("command")
                if not isinstance(command, list) or any(not isinstance(arg, str) for arg in command):
                    raise AnalysisError("invalid component launch command evidence")
                commands[role] = command
        if set(commands) != set(roles):
            raise AnalysisError("missing component launch evidence")
        config = {"runId": data["runId"], "scenario": data["scenario"],
                  "outputDirectory": directory, "commands": commands}
        if data["scenario"] == "B4":
            config["b4"] = coordination["b4"]
            config["shutdownTimeoutSeconds"] = coordination["b4ShutdownTimeoutSeconds"]
        verified = coordinator.validate_finalization(data, coordination, config)
        if verified["status"] != "VERIFIED":
            raise AnalysisError("finalization revalidation failed: " + "; ".join(verified["blockingReasons"]))
        if any(finalization.get(key) != verified[key] for key in
               ("checks", "blockingReasons", "observationCoverage", "operationalCoverage")):
            raise AnalysisError("persisted finalization differs from revalidated coordinator evidence")
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
    if data.get("scenario") not in ("B1", "B2", "B3", "B4"):
        raise AnalysisError("scenario must explicitly identify B1, B2, B3 or B4")
    if data["scenario"] == "B4" and not isinstance(data.get("coordination", {}).get("b4"), dict):
        raise AnalysisError("scenario must explicitly include verified B4 control evidence")
    scheduled = data.get("scheduled")
    if not isinstance(scheduled, dict):
        raise AnalysisError("run-config.scheduled: measurement boundaries are required")
    start = timestamp(scheduled.get("measurementStartEpochMs"), "measurementStartEpochMs")
    end = timestamp(scheduled.get("measurementDeadlineEpochMs"), "measurementDeadlineEpochMs")
    if end <= start or (end - start) % 1000:
        raise AnalysisError("measurement interval must be positive and contain whole seconds")
    require_complete_run(data)
    if data["scenario"] == "B4":
        timeline = next(r for r in data["coordination"]["evidence"] if r.get("evidenceType") == "B4_TIMELINE")
        start, end = timeline["warmUpDeadlineEpochMs"], timeline["observationDeadlineEpochMs"]
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


def b4_analysis(data, producer_rows, result_rows):
    """Evaluate B4 recovery using recorded observations.

    The coordinator determines whether a run is finalized. Event rates
    use the earliest observations for each event and status during
    the measurement phase.

    Pending counts are reconstructed from Producer and Consumer records,
    including the warm-up backlog, rather than Kafka's internal queues.
    A 30-second recovery window must satisfy the rate thresholds and
    show no sustained growth in either pending count, based on net
    change and linear trend.

    ISR recovery is assessed from periodic metadata samples. States
    between samples cannot be verified.
    """
    require_complete_run(data)
    coordination = data["coordination"]
    settings = coordination["b4"]
    condition = settings["condition"]
    records = coordination["evidence"]
    timeline = next(r for r in records if r.get("evidenceType") == "B4_TIMELINE")
    start = timeline["warmUpDeadlineEpochMs"]
    end = timeline["observationDeadlineEpochMs"]
    events = [r["evidence"] for r in records if r.get("evidenceType") == "B4_CONTROL_EVIDENCE"]
    selection = next(e for e in events if e.get("evidenceType") == "B4_BROKER_SELECTED")
    samples = [e for e in events if e.get("evidenceType") == "B4_METADATA_SAMPLE"]
    gaps = [e for e in events if e.get("evidenceType") == "B4_METADATA_QUERY_FAILED"
            or (e.get("evidenceType") == "B4_METADATA_SAMPLE" and e.get("samplingGap"))]
    limitations = [
        "Minute eight means 480 seconds AFTER the 60-second warm-up in D.8.82; PENDING METHODOLOGICAL REVIEW.",
        "Recovery criteria and the pending-queue operationalization require pilot validation.",
        "Epoch timestamps are observations; one-second rates and sampled metadata do not establish exact service, election or synchronization times.",
        "Observed missing results are not proven permanent Kafka data loss.",
        "Coordinator finalization and report completion do not certify methodological approval or successful recovery.",
    ]
    answer = {"condition": condition,
              "requestedTargetEventsPerSecond": data["configuration"]["producer"]["targetEventsPerSecond"],
              "methodologicalStatus": "PENDING METHODOLOGICAL REVIEW",
              "recoveryCriteriaStatus": "PENDING_PILOT_VALIDATION",
              "observation": {"workloadStartEpochMs": timeline["workloadStartEpochMs"],
                              "startEpochMs": start, "deadlineEpochMs": end,
                              "generationDeadlineEpochMs": timeline["generationDeadlineEpochMs"],
                              "drainCutoffEpochMs": timeline["drainCutoffEpochMs"],
                              "bounds": "[start, deadline); drain excluded from recovery analysis"},
              "recordedTimeline": timeline,
              "selectedBroker": selection,
              "selectionPurpose": "REFERENCE_METADATA_ONLY" if condition == "NO_FAILURE" else "INPUT_LEADER_FAULT_TARGET",
              "actions": None, "interruption": None,
              "leadershipAndIsr": {"samples": samples, "observationGaps": gaps,
                                   "maximumPermittedGapMs": settings["metadataMaxGapMs"]},
              "serviceRecovery": {"status": "NOT_APPLICABLE"},
              "replicationRecovery": {"status": "NOT_APPLICABLE"},
              "missingData": [], "limitations": limitations}
    if condition == "NO_FAILURE":
        # Selection is a recorded reference query, not evidence of a fault.
        return answer
    actions = {}
    for action, name in (("stop", "stop"), ("start", "restart")):
        actions[name] = {
            "request": next(e for e in events if e.get("evidenceType") == "B4_BROKER_ACTION_REQUESTED" and e["action"] == action),
            "completion": next(e for e in events if e.get("evidenceType") == "B4_BROKER_ACTION_CONFIRMED" and e["action"] == action)}
    stop, restart = actions["stop"], actions["restart"]
    failure = stop["completion"]["requestedAtEpochMs"]
    stopped = stop["completion"]["observedAtEpochMs"]
    answer["actions"] = actions
    answer["interruption"] = {
        "faultMechanism": stop["request"]["faultMechanism"],
        "stopDispatchEpochMs": failure,
        "stopCompletionObservedAtEpochMs": stopped,
        "restartRequestObservedAtEpochMs": restart["request"]["observedAtEpochMs"],
        "restartDispatchEpochMs": restart["completion"]["requestedAtEpochMs"],
        "restartCompletionObservedAtEpochMs": restart["completion"]["observedAtEpochMs"],
        "observedStoppedIntervalMs": restart["completion"]["observedAtEpochMs"] - stopped,
        "kafkaAvailabilityOrRecoveryImplied": False}
    service = b4_service_recovery(producer_rows, result_rows, start, end, failure, stopped)
    answer["serviceRecovery"] = service
    if service["status"] == "NOT_DETERMINABLE":
        answer["missingData"].extend(service["reasons"])
    # Journal order separates the pre-stop leader query from post-request samples,
    # including observations with the same millisecond timestamp.
    post_fault_samples = [e for e in events[events.index(stop["request"]) + 1:]
                          if e.get("evidenceType") == "B4_METADATA_SAMPLE"]
    partitions = []
    for topic in ("sensor-events", "sensor-results", "sensor-alerts"):
        for partition in range(6):
            observations = [p for sample in post_fault_samples if sample["topic"] == topic
                            for p in sample["partitions"] if p["partition"] == partition
                            and failure <= p["observedAtEpochMs"] < end]
            recovery = b4_replication_recovery(observations, settings["metadataMaxGapMs"], failure)
            partitions.append(dict(topic=topic, partition=partition, **recovery))
    evaluated = next(p for p in partitions if p["topic"] == "sensor-events"
                     and p["partition"] == settings["targetPartition"])
    answer["replicationRecovery"] = dict(evaluated, scope="selected sensor-events partition", partitions=partitions)
    if evaluated["status"] == "NOT_DETERMINABLE":
        answer["missingData"].append(evaluated["reason"])
    return answer


def b4_pending_trend(boundaries):
    """Describe queue growth over 31 left-limit samples spanning 30 seconds.

    Require both nonpositive endpoint change and nonpositive OLS slope for each
    queue. Transient increases are allowed; endpoint-only cancellation and a
    positive overall trend are not. This is a descriptive finite-window rule,
    not a statistical stationarity test or proof of future queue stability.
    Centered integer weights give an exact slope-sign test without tolerance.
    """
    if len(boundaries) != 31 or any(
            row["epochMs"] != boundaries[0]["epochMs"] + i * 1000
            for i, row in enumerate(boundaries)):
        raise AnalysisError("B4 pending trend requires 31 consecutive one-second boundaries")
    trends = {}
    for key in ("publicationPending", "confirmedWithoutObservedResult"):
        values = [row[key] for row in boundaries]
        numerator = sum((i - 15) * value for i, value in enumerate(values))
        net = values[-1] - values[0]
        trends[key] = {"start": values[0], "end": values[-1], "netChange": net,
                       "olsSlopeEventsPerSecond": numerator / 2480,  # sum((i - 15)**2), i=0..30
                       "noSustainedGrowth": net <= 0 and numerator <= 0}
    return trends


def b4_service_recovery(producers, results, start, end, failure, stopped):
    """Exact 60 s pre-dispatch reference; 30 individual observed one-second bins.

    Recovery may occur before restart through surviving replicas. Search begins
    at observed stop completion, and cannot extend into drain. Rates are NOT
    rolling 30-second means: every one-second bin must pass both thresholds.
    """
    from bisect import bisect_left

    phases, outcomes, receipts = {}, {}, {}
    reasons = []
    for row in producers:
        key = row["runId"], row["eventId"]
        phases[key] = row["phase"]
        observed = timestamp(row["recordedAt"], "B4 Producer recordedAt")
        status = row["status"]
        outcomes.setdefault(key, {})[status] = min(outcomes.get(key, {}).get(status, observed), observed)
    for row in results:
        key = row["runId"], row["eventId"]
        observed = timestamp(row["resultReceivedAt"], "B4 resultReceivedAt")
        receipts[key] = min(receipts.get(key, observed), observed)
        if timestamp(row["sentAt"], "B4 sentAt") > observed:
            reasons.append("Negative result latency prevents reliable pending-queue chronology.")
    confirmed = sorted(s["CONFIRMED"] for k, s in outcomes.items()
                       if phases[k] == "MEASUREMENT" and "CONFIRMED" in s)
    received = sorted(at for k, at in receipts.items() if phases.get(k) == "MEASUREMENT")
    def count(times, lower, upper):
        return bisect_left(times, upper) - bisect_left(times, lower)
    reference = {"startEpochMs": failure - 60000, "deadlineEpochMs": failure,
                 "anchor": "actual stop command dispatch; [dispatch-60000, dispatch)",
                 "confirmedEventsPerSecond": count(confirmed, failure - 60000, failure) / 60,
                 "uniqueResultsPerSecond": count(received, failure - 60000, failure) / 60}
    if failure - 60000 < start or failure > end or stopped < failure or stopped >= end:
        reasons.append("Reference or post-stop observation interval is incomplete or contradictory.")
    if reference["confirmedEventsPerSecond"] <= 0 or reference["uniqueResultsPerSecond"] <= 0:
        reasons.append("A positive confirmed and result reference rate is required.")
    # Two observed queues: uncompleted publications and confirmed input identities
    # with no result yet. Include warm-up identities to retain carried backlog.
    changes = {}
    def change(at, column, delta):
        changes.setdefault(at, [0, 0])[column] += delta
    for key, statuses in outcomes.items():
        attempted = statuses.get("ATTEMPTED")
        terminal = [statuses[s] for s in ("CONFIRMED", "FAILED") if s in statuses]
        if attempted is None or len(terminal) != 1 or terminal[0] < attempted:
            reasons.append("Missing, conflicting or time-reversed publication outcomes prevent reliable pending reconstruction.")
            continue
        change(attempted, 0, 1)
        change(terminal[0], 0, -1)
        if "CONFIRMED" in statuses:
            confirmation = statuses["CONFIRMED"]
            change(confirmation, 1, 1)
            if key in receipts:
                change(max(confirmation, receipts[key]), 1, -1)
        if key in receipts and (receipts[key] < attempted or "CONFIRMED" not in statuses):
            reasons.append("A result precedes its attempt or has no confirmed publication; pending chronology is ambiguous.")
    if set(receipts) - set(outcomes):
        reasons.append("Results lack originating publication evidence.")
    response = {"status": "NOT_DETERMINABLE", "reference": reference,
                "criteria": {"rateFraction": 0.95, "consecutiveSeconds": 30,
                             "ratePopulation": "Original MEASUREMENT identities; earliest confirmation and result observations per identity.",
                             "pending": "Each reconstructed queue must have net change <= 0 and OLS slope <= 0 over the 31 one-second left-limit boundaries spanning the same 30-second rate window; transient increases allowed; pending pilot validation.",
                             "pendingPopulations": ["all attempted identities without terminal publication outcome", "all confirmed identities without observed unique result"],
                             "resolutionMs": 1000},
                "windowStartEpochMs": None, "verifiedThroughEpochMs": None,
                "elapsedFromStopDispatchMs": None, "reasons": sorted(set(reasons))}
    if reasons:
        return response
    times = sorted(changes)
    position, pending = 0, [0, 0]
    seconds = max(0, (end - stopped) // 1000)
    boundaries = []
    for i in range(seconds + 1):
        boundary = stopped + i * 1000
        # Left-limit at each boundary matches the half-open rate intervals.
        while position < len(times) and times[position] < boundary:
            delta = changes[times[position]]
            pending = [pending[c] + delta[c] for c in range(2)]
            position += 1
        boundaries.append({"epochMs": boundary, "publicationPending": pending[0],
                           "confirmedWithoutObservedResult": pending[1]})
    response["pendingObservations"] = boundaries
    if seconds < 30:
        response["reasons"] = ["Fewer than 30 complete post-stop seconds are observed before the active deadline."]
        return response
    streak = 0
    for i in range(seconds):
        lower = stopped + i * 1000
        rates_ok = (count(confirmed, lower, lower + 1000) >= 0.95 * reference["confirmedEventsPerSecond"]
                    and count(received, lower, lower + 1000) >= 0.95 * reference["uniqueResultsPerSecond"])
        streak = streak + 1 if rates_ok else 0
        if streak >= 30:
            trends = b4_pending_trend(boundaries[i - 29:i + 2])
            if not all(trend["noSustainedGrowth"] for trend in trends.values()):
                continue
            observed = lower + 1000
            response.update(status="OBSERVED_RECOVERY", windowStartEpochMs=observed - 30000,
                            verifiedThroughEpochMs=observed, elapsedFromStopDispatchMs=observed - failure,
                            pendingWindowTrends=trends)
            return response
    response.update(status="NOT_OBSERVED", reasons=["No 30-second window satisfies both per-second rate thresholds and nonpositive net change and OLS slope for both pending queues."])
    return response


def b4_replication_recovery(observations, max_gap_ms, failure):
    """Full ISR at sampled times spanning >=30 s after observed ISR degradation."""
    response = {"status": "NOT_DETERMINABLE", "reason": None,
                "fullIsrFirstObservedAtEpochMs": None, "qualifiedObservedAtEpochMs": None,
                "observedSpanMs": None, "elapsedFromStopDispatchMs": None,
                "continuousStateBetweenSamplesVerified": False}
    previous, previous_state, full_since, degraded = None, None, None, False
    for row in observations:
        at = timestamp(row["observedAtEpochMs"], "B4 metadata observation")
        replicas, isr = row["replicaBrokerIds"], row["isrBrokerIds"]
        state = (tuple(sorted(replicas)), tuple(sorted(isr)), row["leaderBrokerId"])
        if at == previous and state == previous_state:
            continue
        if (len(replicas) != 3 or set(replicas) != {4, 5, 6} or len(isr) != len(set(isr))
                or not set(isr).issubset(replicas) or row["leaderBrokerId"] not in isr
                or (previous is not None and not 0 < at - previous <= max_gap_ms)):
            response.update(status="NOT_DETERMINABLE", fullIsrFirstObservedAtEpochMs=None,
                            qualifiedObservedAtEpochMs=None, observedSpanMs=None, elapsedFromStopDispatchMs=None)
            response["reason"] = "Contradictory replica/ISR observations or a metadata sampling gap."
            return response
        previous, previous_state = at, state
        if set(isr) != set(replicas):
            degraded, full_since = True, None
        elif degraded:
            full_since = at if full_since is None else full_since
            if at - full_since >= 30000 and response["status"] != "OBSERVED_RECOVERY":
                response.update(status="OBSERVED_RECOVERY", fullIsrFirstObservedAtEpochMs=full_since,
                                qualifiedObservedAtEpochMs=at, observedSpanMs=at - full_since,
                                elapsedFromStopDispatchMs=at - failure)
    if response["status"] == "OBSERVED_RECOVERY":
        return response
    response["status"] = "NOT_OBSERVED" if degraded else "NOT_DETERMINABLE"
    response["reason"] = ("No sampled full-ISR sequence spans 30 seconds after degradation." if degraded else
                          "No post-dispatch ISR degradation was observed; restoration cannot be timed.")
    return response


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
