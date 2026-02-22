#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: scripts/spark-submit.sh <script.py> [args...]"
  exit 1
fi

SCRIPT_PATH="$1"
shift

docker compose exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  "$SCRIPT_PATH" "$@"
