#!/usr/bin/env bash
set -euo pipefail
strategy="${1:-median}"
max_files="${2:-0}"
args=(compose run --rm --service-ports spark /opt/spark/bin/spark-submit --driver-memory 8g --master 'local[*]' /workspace/src/run_baseline.py --strategy "$strategy")
if [[ "$max_files" != "0" ]]; then args+=(--max-files "$max_files"); fi
docker "${args[@]}"
