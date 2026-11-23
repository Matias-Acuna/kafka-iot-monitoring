#!/usr/bin/env bash
# Run from the repository root: bash infra/create-topics-1-broker.sh
# Requires the cluster in compose.1-broker.yml to be running already.
# Official CLI reference: https://kafka.apache.org/41/operations/basic-kafka-operations/
set -Eeuo pipefail

trap 'printf "ERROR: failure on line %s. Execution will stop.\n" "$LINENO" >&2' ERR

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
compose_file="${script_dir}/compose.1-broker.yml"
readonly bootstrap_server='broker-1:19092'
readonly -a topics=(sensor-events sensor-results sensor-alerts)
readonly -a compose=(docker compose -f "$compose_file")

command -v docker >/dev/null 2>&1 || fail 'Docker was not found in PATH.'
command -v awk >/dev/null 2>&1 || fail 'awk was not found in PATH.'
[[ -f "$compose_file" ]] || fail "${compose_file} does not exist."
docker compose version >/dev/null

# exec -T uses the existing Compose service without starting containers.
kafka_topics() {
    "${compose[@]}" exec -T --interactive=false broker-1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server "$bootstrap_server" "$@"
}

kafka_configs() {
    "${compose[@]}" exec -T --interactive=false broker-1 /opt/kafka/bin/kafka-configs.sh \
        --bootstrap-server "$bootstrap_server" "$@"
}

printf 'Using broker-1 and bootstrap-server %s.\n' "$bootstrap_server"
printf 'Missing topics will be created and the configuration of all topics will be verified.\n'

for topic in "${topics[@]}"; do
    printf '\nEnsuring topic %s: 6 partitions, RF=1, min.insync.replicas=1...\n' "$topic"
    # Existing topics are left unchanged; the checks below reject mismatches.
    kafka_topics --create --if-not-exists --topic "$topic" \
        --partitions 6 --replication-factor 1 --config min.insync.replicas=1

    printf 'Verifying partitions and replicas for %s...\n' "$topic"
    description="$(kafka_topics --describe --topic "$topic")"
    printf '%s\n' "$description"
    # Check both the summary and every partition, including mixed replica counts.
    if ! awk -v topic="$topic" '
        {
            delete fields
            for (i = 1; i < NF; i++) {
                if ($i ~ /:$/) fields[$i] = $(i + 1)
            }
            if (fields["Topic:"] != topic) next
            if ("PartitionCount:" in fields) {
                summaries++
                if (fields["PartitionCount:"] != "6" ||
                    fields["ReplicationFactor:"] != "1") bad = 1
            }
            if ("Partition:" in fields) {
                partitions++
                id = fields["Partition:"]
                if (id !~ /^[0-5]$/ || seen[id]++) bad = 1
                count = split(fields["Replicas:"], replicas, ",")
                if (count != 1 || replicas[1] != "4") bad = 1
            }
        }
        END { exit !(summaries == 1 && partitions == 6 && !bad) }
    ' <<< "$description"; then
        fail "$topic does not have exactly 6 partitions with exactly one replica per partition on broker 4. It will not be modified automatically."
    fi

    printf 'Verifying min.insync.replicas for %s...\n' "$topic"
    # Without --all, --describe returns explicit topic overrides, not defaults.
    configs="$(kafka_configs --describe --entity-type topics --entity-name "$topic")"
    printf '%s\n' "$configs"
    if [[ ! "$configs" =~ (^|[[:space:]])min\.insync\.replicas=1([[:space:]]|$) ]]; then
        fail "$topic must have min.insync.replicas=1 explicitly configured. It will not be modified automatically."
    fi
    printf 'OK: %s matches the required configuration.\n' "$topic"
done

printf '\nCompleted: sensor-events, sensor-results and sensor-alerts verified.\n'
