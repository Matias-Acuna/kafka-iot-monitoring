#!/usr/bin/env bash
set -Eeuo pipefail

trap 'printf "ERROR: B3 launcher failed on line %s.\n" "$LINENO" >&2' ERR

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

if [[ $# -eq 1 && "$1" == --help ]]; then
    cat <<'USAGE'
Usage: run-kafka-streams-b3.sh --cpu-quota 200% --memory-max 4G --state-dir DIRECTORY [Streams options]
Or, for each of two instances: --cpu-quota 100% --memory-max 2G
Pass the existing Streams --option value arguments, including bootstrap-servers,
application-id, rule and its parameters. All options require nonblank values.
MemorySwapMax=0 is required; unsupported resource controls fail startup.
CPU quotas are ceilings, not dedicated cores. MemoryMax limits total cgroup memory.

Prerequisite: prepare artifacts once, with all experimental Streams JVMs stopped:
  mvn -f services/kafka-streams/pom.xml compile dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory="$PWD/services/kafka-streams/target/dependency"
Use the existing module target/dependency directory; do not rebuild during runs.
This launcher never invokes Maven or deletes state. systemd allocates a unique
transient scope name. The coordinator must supervise the launcher process group.
USAGE
    exit 0
fi

declare -A seen=()
streams_args=()
cpu_quota=''
memory_max=''
while (( $# > 0 )); do
    option="$1"
    (( $# >= 2 )) || fail "Missing value for $option."
    [[ "$2" == *[![:space:]]* && "$2" != --* ]] || fail "Missing or blank value for $option."
    [[ -z "${seen[$option]+present}" ]] || fail "Duplicate option: $option."
    seen[$option]=true
    case "$option" in
        --cpu-quota) cpu_quota="$2" ;;
        --memory-max) memory_max="$2" ;;
        --bootstrap-servers|--application-id|--rule|--temperature-threshold|--window-duration-ms|--count-threshold|--max-time-difference-ms|--temperature-change-threshold|--lifecycle-stdout|--state-dir)
            streams_args+=("$option" "$2") ;;
        *) fail "Unsupported option: $option." ;;
    esac
    shift 2
done

case "$cpu_quota/$memory_max" in
    200%/4G|100%/2G) ;;
    *) fail 'Use exactly --cpu-quota 200% --memory-max 4G or --cpu-quota 100% --memory-max 2G.' ;;
esac
for option in --bootstrap-servers --application-id --rule --state-dir; do
    [[ -n "${seen[$option]+present}" ]] || fail "Required option: $option."
done

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
module_dir="$repo_root/services/kafka-streams"
command -v systemd-run >/dev/null 2>&1 || fail 'systemd-run was not found in PATH.'
command -v java >/dev/null 2>&1 || fail 'Java was not found in PATH.'
[[ -f "$module_dir/target/classes/com/kafkaiot/streams/SensorStreamsApplication.class" ]] \
    || fail 'Prebuilt Streams classes are missing. Prepare artifacts before the experiment; see --help.'
shopt -s nullglob
runtime_jars=("$module_dir/target/dependency/"*.jar)
(( ${#runtime_jars[@]} > 0 )) || fail 'Prebuilt runtime dependencies are missing; see --help.'
cd -- "$repo_root"

# Scope mode execs Java in this PID/session, preserving coordinator pipes, exit
# status and process-group signals (including SIGKILL). Do not detach or fork a
# supervisor: SIGKILL cannot run shell cleanup traps. Empty scopes are retired
# by systemd; startup failure exits nonzero without launching an unscoped JVM.
exec systemd-run --user --scope --expand-environment=no \
    --property="CPUQuota=$cpu_quota" --property="MemoryMax=$memory_max" \
    --property=MemorySwapMax=0 -- \
    java -cp "$module_dir/target/classes:$module_dir/target/dependency/*" \
    com.kafkaiot.streams.SensorStreamsApplication "${streams_args[@]}"
