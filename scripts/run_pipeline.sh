#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run_pipeline.sh - one command for a complete run:
#   recreate Kafka topic -> start Flink job -> wait until it is running ->
#   send the events in (accelerated) real time -> wait for the job to finish -> summary
#
# Usage:
#   bash scripts/run_pipeline.sh <stream> <run> [speedup] [extra job arguments ...]
# Examples:
#   bash scripts/run_pipeline.sh s3 demo
#   bash scripts/run_pipeline.sh late10 late10_D120 900 --watermark-s 120
#   bash scripts/run_pipeline.sh s3 recovery 900 --fail-after 8000
#
# The stream must exist first:  python generator/event_generator.py --name <stream> ...
# The job log is written to results/logs/<run>.job.log
# -----------------------------------------------------------------------------
set -euo pipefail
: "${PROJECT_HOME:?Run 'source env.sh' first}"
STREAM="${1:?stream name}"; RUN="${2:?run name}"; SPEEDUP="${3:-900}"
shift $(( $# >= 3 ? 3 : $# ))
cd "$PROJECT_HOME"
LOG="results/logs/${RUN}.job.log"
mkdir -p results/logs

if [ ! -f "data/stream/$STREAM/events.jsonl" ]; then
  echo "Stream '$STREAM' not found. Generate it first:"
  echo "  python generator/event_generator.py --name $STREAM --clean"
  exit 1
fi

echo "[1/4] recreating Kafka topic ev-$STREAM"
python generator/kafka_producer.py --name "$STREAM" --create-topic

echo "[2/4] starting Flink job '$RUN' (log: $LOG)"
python flink_job/job.py --stream "$STREAM" --run "$RUN" --speedup "$SPEEDUP" "$@" > "$LOG" 2>&1 &
JOB_PID=$!
trap 'kill $JOB_PID 2>/dev/null || true' INT TERM

# the job is really running once it has written its first checkpoint
for i in $(seq 1 120); do
  if ls -d "checkpoints/$RUN"/*/chk-* >/dev/null 2>&1; then
    echo "      job is running (first checkpoint after ${i}s)"; break
  fi
  if ! kill -0 $JOB_PID 2>/dev/null; then
    echo "Flink job exited early - last lines of $LOG:"; tail -40 "$LOG"; exit 1
  fi
  sleep 1
  if [ "$i" = 120 ]; then echo "job did not start within 120 s - see $LOG"; tail -40 "$LOG"; exit 1; fi
done

echo "[3/4] sending events at ${SPEEDUP}x"
python generator/kafka_producer.py --name "$STREAM" --send --speedup "$SPEEDUP"
# keep this run's own copy of the send timing (used to measure pipeline latency)
cp "data/stream/$STREAM/.kafka_send.json" "results/outputs/$RUN/kafka_send.json"

echo "[4/4] waiting for the job to finish ..."
if wait $JOB_PID; then
  tail -2 "$LOG"
  python analysis/inspect_run.py "$RUN"
else
  echo "Flink job failed - last lines of $LOG:"; tail -60 "$LOG"; exit 1
fi
