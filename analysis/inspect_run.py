"""
inspect_run.py - quick summary of everything a Flink run produced.

Usage:  python analysis/inspect_run.py <run name>
"""
from __future__ import annotations

import glob
import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402

pd.set_option("display.width", 160)
pd.set_option("display.max_columns", 20)


def load_sink(run: str, sink: str) -> pd.DataFrame:
    rows = []
    for fp in sorted(glob.glob(config.path("results", "outputs", run, sink, "*.jsonl"))):
        with open(fp) as f:
            rows.extend(json.loads(line) for line in f if line.strip())
    return pd.DataFrame(rows)


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: python analysis/inspect_run.py <run name>")
    run = sys.argv[1]
    base = config.path("results", "outputs", run)
    if not os.path.isdir(base):
        sys.exit(f"No outputs for run '{run}' in {base}")
    with open(os.path.join(base, "run_info.json")) as f:
        info = json.load(f)
    print("=" * 78)
    print(f" RUN {run}: stream={info['stream']}  D={info['watermark_s']}s  "
          f"L={info.get('allowed_lateness_min')}min  runtime={info.get('net_runtime_ms', 0) / 1000:.0f}s")
    print("=" * 78)
    sinks = sorted(d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d)))
    counts = {s: len(load_sink(run, s)) for s in sinks}
    hidden = sum(len(glob.glob(os.path.join(base, s, ".*"))) for s in sinks)
    print("rows per sink:", counts, f"| uncommitted files: {hidden}")
    if os.path.exists(os.path.join(base, "FAILURE_INJECTED.txt")):
        print("failure injected:", open(os.path.join(base, "FAILURE_INJECTED.txt")).read().strip())

    pr = load_sink(run, "probe")
    if not pr.empty:
        print(f"\n[probe] events={int(pr['events'].sum()):,}  late (ts <= watermark)={int(pr['late'].sum()):,}  "
              f"max lateness={pr['max_late_s'].max():.0f}s  hour-corrections={int((pr['emission'] > 1).sum())}")

    fl = load_sink(run, "feeder_load")
    if not fl.empty:
        last = fl.sort_values("firing").groupby(["feeder_id", "minute"]).tail(1)
        print(f"\n[feeder_load] minute results={len(fl):,}  revisions (late firings)={int(fl['revision'].sum()):,}")
        print(last.groupby("feeder_id").agg(minutes=("minute", "nunique"), capacity_kw=("capacity_kw", "first"),
                                            peak_kw=("load_kw", "max"), peak_pct=("pct", "max"),
                                            minutes_over_100=("pct", lambda p: int((p >= 100).sum())))
              .to_string())

    su = load_sink(run, "sustained")
    if not su.empty:
        last = su.sort_values("firing").groupby(["feeder_id", "window_end"]).tail(1)
        print(f"\n[sustained] windows={len(last):,}  >=80%: {int((last['pct'] >= 80).sum())}  "
              f"revisions={int(su['revision'].sum())}")

    al = load_sink(run, "alerts")
    if not al.empty:
        print("\n[alerts] by type and feeder:")
        print(al.pivot_table(index="feeder_id", columns="alert_type", values="event_time",
                             aggfunc="count", fill_value=0).to_string())
        print(al.sort_values("event_time")[["alert_type", "feeder_id", "event_time_local", "pct",
                                            "load_kw", "capacity_kw"]].head(10).to_string(index=False))

    se = load_sink(run, "sessions")
    if not se.empty:
        print(f"\n[sessions] {len(se):,} summaries  status={se['status'].value_counts().to_dict()}  "
              f"energy={se['energy_kwh_measured'].sum():,.1f} kWh  cost=${se['cost_usd'].sum():,.2f}  "
              f"with late events={int((se['late_events'] > 0).sum())}")

    sd = load_sink(run, "station_daily")
    if not sd.empty:
        print(f"\n[station_daily] {len(sd)} station-days; busiest:")
        print(sd.sort_values("energy_kwh", ascending=False)[["day_local", "station_id", "sessions",
                                                              "energy_kwh", "cost_usd", "avg_duration_min"]]
              .head(5).to_string(index=False))

    dl = load_sink(run, "dropped_late")
    print(f"\n[dropped_late] records too late for the windows (beyond allowed lateness): {len(dl):,}")
    if not dl.empty:
        print(dl["dropped_by"].value_counts().to_string())


if __name__ == "__main__":
    main()
