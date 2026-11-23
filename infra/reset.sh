#!/usr/bin/env bash
set -Eeuo pipefail

trap 'printf "ERROR: failure on line %s. Reset stopped; manual inspection is required.\n" "$LINENO" >&2' ERR

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

usage() {
    cat <<'USAGE'
Usage: bash infra/reset.sh [--help | --execute]

Without arguments or with --help, only display this description.
--execute deletes sensor-events, sensor-results and sensor-alerts, including
all records still stored in them, then recreates them using create-topics.sh.
Stop all Kafka clients using these topics before executing the reset.
The existing three-broker Compose cluster must already be running.
Containers, configurations, Docker volumes and other topics are preserved.
Deletion metadata is polled for up to 120 seconds before recreation.
USAGE
}

if (( $# == 0 )); then
    usage
    exit 0
fi
if (( $# != 1 )); then
    fail 'Expected exactly one argument: --help or --execute.'
fi
case "$1" in
    --help) usage; exit 0 ;;
    --execute) ;;
    *) fail "Unsupported argument: $1. Use --help or --execute." ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${script_dir}/compose.3-brokers.yml"
create_script="${script_dir}/create-topics.sh"
readonly bootstrap_server='broker-1:19092'
readonly -a topics=(sensor-events sensor-results sensor-alerts)
readonly -a compose=(docker compose -f "$compose_file")

kafka_topics() {
    local limit="$1"
    shift
    timeout --foreground --kill-after=2 "$limit" \
        "${compose[@]}" exec -T --interactive=false broker-1 \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$bootstrap_server" "$@"
}

topic_exists() {
    local expected="$1" entry
    while IFS= read -r entry; do
        [[ "$entry" != "$expected" ]] || return 0
    done <<< "$topic_list"
    return 1
}

printf 'Preflight: checking tools, files and the running broker.\n'
command -v docker >/dev/null 2>&1 || fail 'Docker was not found in PATH.'
command -v timeout >/dev/null 2>&1 || fail 'timeout was not found in PATH.'
command -v awk >/dev/null 2>&1 || fail 'awk (required by create-topics.sh) was not found in PATH.'
[[ -f "$compose_file" ]] || fail "${compose_file} does not exist."
[[ -f "$create_script" && -r "$create_script" ]] || fail "${create_script} is missing or unreadable."
docker compose version >/dev/null || fail 'Docker Compose is unavailable.'
# exec only uses an existing running service; no containers are started.
topic_list="$(kafka_topics 10 --list)" || fail 'Broker metadata is unavailable; no deletion was started.'
for topic in "${topics[@]}"; do
    topic_exists "$topic" || fail "Required topic $topic is missing; no deletion was started."
done

printf 'Preflight complete. Deleting the three experimental topics.\n'
for topic in "${topics[@]}"; do
    printf 'Deleting %s...\n' "$topic"
    kafka_topics 10 --delete --topic "$topic" \
        || fail "Deletion of $topic failed or timed out. Reset stopped; manual inspection is required."
done

printf 'Waiting up to 120 seconds for all three topics to disappear from metadata...\n'
deadline=$((SECONDS + 120))
deleted=false
while (( SECONDS < deadline )); do
    remaining=$((deadline - SECONDS))
    if (( remaining <= 0 )); then
        break
    fi
    limit=10
    if (( remaining < limit )); then
        limit="$remaining"
    fi
    topic_list="$(kafka_topics "$limit" --list)" \
        || fail 'Deletion metadata check failed or timed out. Manual inspection is required.'
    pending=false
    for topic in "${topics[@]}"; do
        if topic_exists "$topic"; then
            pending=true
        fi
    done
    if [[ "$pending" == false ]]; then
        deleted=true
        break
    fi
    remaining=$((deadline - SECONDS))
    if (( remaining > 0 )); then
        sleep 1
    fi
done
[[ "$deleted" == true ]] || fail 'Topic deletion did not complete within 120 seconds. Manual inspection is required.'

printf 'Deletion complete. Recreating and verifying topics with create-topics.sh...\n'
bash "$create_script" || fail 'Topic recreation or verification failed. Manual inspection is required.'
printf 'Reset completed: all three experimental topics were recreated and verified.\n'
