#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
module_dir="$repo_root/services/measurement-consumer"

mvn -f "$module_dir/pom.xml" compile dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory="$module_dir/target/dependency"

exec java -cp "$module_dir/target/classes:$module_dir/target/dependency/*" \
    com.kafkaiot.measurement.MeasurementConsumerApplication "$@"
