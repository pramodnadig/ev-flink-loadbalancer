#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# run_experiments.sh - the controlled experiments for Parts C and D.
#
# Usage (after: source env.sh):
#   bash experiments/run_experiments.sh all          # everything (~60-90 min)
#   bash experiments/run_experiments.sh e2           # one experiment
#   SPEEDUP=600 bash experiments/run_experiments.sh e3
#
# All experiments replay the same 2 busiest days (22-23 Jan 2020) with the same seed,
# so the ONLY thing that changes between runs is the setting under test.
#
#   E1  event time vs processing time      exp_base, D=120s, + processing-time branch
#   E2  watermark sweep (latency vs completeness)
#                                           exp_late, D = 0 / 30 / 120 / 300 / 660 s, L = 0
#   E3  late plug/unplug: impact + repair   exp_base vs exp_late (L=0, uncorrected)
#                                           vs exp_late (L=15 min, corrected) + outage stream
#   E4  accuracy vs ground truth            computed for every run (analysis/evaluate_run.py)
#   E5  failure + recovery                  exp_base with ONE injected failure vs E1 run
#   E6  throughput / backpressure           exp_base replayed 4x and 16x faster (3600x,
#                                           14400x) - where does the pipeline saturate?
# -----------------------------------------------------------------------------
set -euo pipefail
: "${PROJECT_HOME:?Run 'source env.sh' first}"
cd "$PROJECT_HOME"
SPEEDUP="${SPEEDUP:-900}"
START="2020-01-22 00:00"; DAYS=2
mkdir -p results/logs

gen() {   # gen <name> [generator args...]
  local name=$1; shift
  if [ ! -f "data/stream/$name/events.jsonl" ]; then
    python generator/event_generator.py --name "$name" --start-local "$START" --days $DAYS --clean "$@"
  fi
}
run() {   # run <stream> <run name> [job args...]
  local stream=$1 name=$2; shift 2
  echo; echo "################ RUN $name ($stream, ${SPEEDUP}x) $*   [$(date +%H:%M:%S)]"
  bash scripts/run_pipeline.sh "$stream" "$name" "$SPEEDUP" "$@" > "results/logs/${name}.run.txt" 2>&1 \
    || { echo "RUN $name FAILED - see results/logs/${name}.run.txt"; tail -30 "results/logs/${name}.run.txt"; return 1; }
  grep -E "job finished|RUN |late \(ts|revisions|dropped" "results/logs/${name}.run.txt" | head -6
  python analysis/evaluate_run.py "$name" > "results/logs/${name}.eval.txt"
}

streams() {
  gen exp_base
  gen exp_late   --late-fraction 0.10 --late-delay-min 10
  gen exp_outage --outage CAMBRIDGE-3 "2020-01-23 08:00" 90 --outage HIGH-2 "2020-01-23 12:00" 60
}

e1() { streams; run exp_base E1_event_time --watermark-s 120 --proc-time-branch; }
e2() { streams; for d in 0 30 120 300 660; do run exp_late "E2_D${d}" --watermark-s $d --allowed-lateness-min 0; done; }
e3() { streams
       run exp_base   E3_base           --watermark-s 120 --allowed-lateness-min 0
       run exp_late   E3_late_L0        --watermark-s 120 --allowed-lateness-min 0
       run exp_late   E3_late_L15       --watermark-s 120 --allowed-lateness-min 15
       run exp_outage E3_outage_L0      --watermark-s 120 --allowed-lateness-min 0
       run exp_outage E3_outage_L120    --watermark-s 120 --allowed-lateness-min 120 --horizon-min 130; }
e5() { streams
       local n; n=$(python -c "import json;print(json.load(open('data/stream/exp_base/.manifest.json'))['events']//2)")
       run exp_base E5_recovery --watermark-s 120 --fail-after "$n"
       [ -d results/outputs/E1_event_time ] || e1
       python analysis/compare_runs.py E1_event_time E5_recovery | tee results/logs/E5_compare.txt; }

e6() { streams
       SPEEDUP=3600  run exp_base E6_speed3600  --watermark-s 120
       SPEEDUP=14400 run exp_base E6_speed14400 --watermark-s 120; }

case "${1:-all}" in
  e1) e1 ;; e2) e2 ;; e3) e3 ;; e5) e5 ;; e6) e6 ;;
  all) e1; e2; e3; e5; e6
       python analysis/evaluate_run.py $(ls results/outputs | grep "^E") > results/logs/all_runs.eval.txt
       echo "done. summary: results/logs/all_runs.eval.txt" ;;
  *) echo "usage: $0 [all|e1|e2|e3|e5|e6]"; exit 1 ;;
esac
