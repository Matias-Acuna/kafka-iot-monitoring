#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
module_dir="$repo_root/services/sensor-producer"

if [[ $# -eq 1 && "$1" == "--help" ]]; then
    # Read the application's help text without requiring compiled classes or jars.
    awk '
        /System[.]out[.]println\("""/ { help = 1; next }
        help && /"""\);/ { print ""; exit }
        help { sub(/^                    /, ""); print }
    ' "$module_dir/src/main/java/com/kafkaiot/producer/SensorProducerApplication.java"
    exit 0
fi

mvn -f "$module_dir/pom.xml" compile dependency:build-classpath \
    -DincludeScope=runtime \
    -Dmdep.outputFile="$module_dir/target/classpath.txt"

dependency_classpath="$(cat -- "$module_dir/target/classpath.txt")"
exec java -cp "$module_dir/target/classes:$dependency_classpath" \
    com.kafkaiot.producer.SensorProducerApplication "$@"
