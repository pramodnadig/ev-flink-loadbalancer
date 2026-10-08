#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# submit_to_cluster.sh - run the Flink job on the standalone cluster (Web UI visible)
#
# Usage (after: source env.sh, cluster running):
#   bash scripts/submit_to_cluster.sh --stream baseline --run s3_cluster --watermark-s 120
#
# All arguments are passed through to flink_job/job.py.
# The job runs detached (-d): this command returns immediately and the job keeps
# running on the cluster. Watch it at http://localhost:8081
# -----------------------------------------------------------------------------
set -euo pipefail
: "${PROJECT_HOME:?Run 'source env.sh' first}"
PY="$EV_BASE/.venv/bin/python"

"${FLINK_HOME:?Run source env.sh first}/bin/flink" run -d \
  --python "$PROJECT_HOME/flink_job/job.py" \
  --pyExecutable "$PY" \
  --pyClientExecutable "$PY" \
  "$@"
