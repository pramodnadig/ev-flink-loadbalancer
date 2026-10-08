"""
evaluate_run.py - compare a Flink run with the ground truth (experiments E1-E5).

Ground truth
------------
Computed offline from the clean sessions with the SAME power model the generator used
(common/power_model.py): the exact load of every feeder at every minute, as if every
event had arrived instantly and in order. Any difference between Flink and the truth is
therefore caused purely by streaming effects (lateness, watermark choice, failures).

What is measured
----------------
  load accuracy      per feeder-minute: Flink's FIRST result (what the operator saw
                     first) and FINAL result (after late corrections) vs truth
                       - MAE (kW), % of minutes exactly right, minutes missing
  alert quality      overload EPISODES (consecutive minutes >= threshold) in truth vs
                     alerts raised by Flink:
                       precision = alerts that point at a real episode / all alerts
                       recall    = real episodes that got an alert / all real episodes
                     computed for first results and after retractions / late detections
  lateness           late events seen, records dropped beyond allowed lateness, revisions
  latency            pipeline latency in wall-clock ms: time from the moment the event
                     that closed a window was sent to Kafka until the result was written

Usage:  python analysis/evaluate_run.py <run> [<run> ...]
Writes: results/metrics/<run>.json  and prints a summary
"""
from __future__ import annotations

import bisect
import glob
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.power_model import load_profile  # noqa: E402

CFG = config.load()
MIN = 60_000


# --------------------------------------------------------------------------- loading
def load_sink(run: str, sink: str) -> pd.DataFrame:
    rows = []
    for fp in sorted(glob.glob(config.path("results", "outputs", run, sink, "*.jsonl"))):
        with open(fp) as f:
            rows.extend(json.loads(line) for line in f if line.strip())
    return pd.DataFrame(rows)


def run_info(run: str) -> dict:
    with open(config.path("results", "outputs", run, "run_info.json")) as f:
        return json.load(f)


def stream_meta(stream: str) -> tuple[dict, dict | None]:
    d = config.path("data", "stream", stream)
    with open(os.path.join(d, ".manifest.json")) as f:
        man = json.load(f)
    send = None
    p = os.path.join(d, ".kafka_send.json")
    if os.path.exists(p):
        with open(p) as f:
            send = json.load(f)
    return man, send


# --------------------------------------------------------------------------- ground truth
def ground_truth(stream: str, capacity: dict) -> dict:
    man, _ = stream_meta(stream)
    w0, w1 = man["window_utc_ms"]
    s = pd.read_csv(config.path("data", "processed", "sessions_replay.csv"))
    s = s[(s["start_ms"] >= w0) & (s["start_ms"] < w1)]
    end = int(s["unplug_ms"].max()) + MIN if len(s) else w1
    interval = CFG["power_model"]["sample_interval_s"] * 1000
    prof = load_profile(s, w0, end, interval)                    # index: minute, cols: feeder
    win = int(CFG["alerts"]["sustained_window_min"])
    out = {"minute_load": prof, "sustained_avg": prof.rolling(win, min_periods=1).sum() / win}
    out["episodes_spike"] = episodes(prof, capacity, CFG["alerts"]["spike_threshold_pct"])
    # sustained windows are labelled by their END (last minute + 1 min), like Flink
    sus = out["sustained_avg"].copy()
    sus.index = sus.index + MIN
    out["episodes_sustained"] = episodes(sus, capacity, CFG["alerts"]["sustained_threshold_pct"])
    out["sustained_by_end"] = sus
    return out


def episodes(series_df: pd.DataFrame, capacity: dict, pct: float) -> list[dict]:
    """Consecutive minutes at/above the threshold -> one episode."""
    eps = []
    for f in series_df.columns:
        over = (series_df[f] / capacity[f] * 100) >= pct
        start = None
        prev_t = None
        for t, o in over.items():
            if o and start is None:
                start = t
            if not o and start is not None:
                eps.append({"feeder_id": f, "start": start, "end": prev_t})
                start = None
            prev_t = t
        if start is not None:
            eps.append({"feeder_id": f, "start": start, "end": prev_t})
    return eps


# --------------------------------------------------------------------------- comparisons
def load_accuracy(fl: pd.DataFrame, truth: pd.DataFrame) -> dict:
    t = truth.stack().rename("truth_kw").reset_index()
    t.columns = ["minute", "feeder_id", "truth_kw"]
    t = t[t["truth_kw"] > 1e-9]
    res = {"truth_minutes_with_load": int(len(t))}
    if fl.empty:
        return res
    fl = fl.sort_values("firing")
    first = fl[~fl["revision"]].groupby(["feeder_id", "minute"])["load_kw"].first()
    final = fl.groupby(["feeder_id", "minute"])["load_kw"].last()
    for name, ser in (("first", first), ("final", final)):
        m = t.merge(ser.rename("flink_kw").reset_index(), on=["feeder_id", "minute"], how="outer")
        m["truth_kw"] = m["truth_kw"].fillna(0.0)
        m["flink_kw"] = m["flink_kw"].fillna(0.0)
        err = (m["flink_kw"] - m["truth_kw"]).abs()
        res[f"{name}_mae_kw"] = round(float(err.mean()), 4)
        res[f"{name}_max_abs_err_kw"] = round(float(err.max()), 3)
        res[f"{name}_exact_pct"] = round(float((err < 1e-3).mean() * 100), 3)
        res[f"{name}_minutes_overestimated"] = int((m["flink_kw"] - m["truth_kw"] > 1e-3).sum())
        res[f"{name}_minutes_underestimated"] = int((m["truth_kw"] - m["flink_kw"] > 1e-3).sum())
    return res


def alert_quality(al: pd.DataFrame, eps: list[dict], prefix: str, window_ms: int = 0) -> dict:
    """Episode-level precision/recall, first view and after corrections."""
    res = {f"{prefix}_true_episodes": len(eps)}
    if al.empty:
        a = pd.DataFrame(columns=["alert_type", "feeder_id", "event_time"])
    else:
        a = al[al["alert_type"].str.startswith(prefix)]

    def in_episode(f, t):
        for e in eps:
            if e["feeder_id"] == f and e["start"] <= t <= e["end"]:
                return e
        return None

    raised = a[a["alert_type"] == prefix]
    late = a[a["alert_type"] == prefix + "_LATE"]
    retracted = a[a["alert_type"] == prefix + "_RETRACTED"]
    retracted_keys = {(r.feeder_id, r.event_time) for r in retracted.itertuples()}

    def score(rows):
        hits, eps_hit = 0, set()
        for r in rows.itertuples():
            e = in_episode(r.feeder_id, r.event_time)
            if e is not None:
                hits += 1
                eps_hit.add((e["feeder_id"], e["start"]))
        n = len(rows)
        return {"alerts": n, "correct": hits,
                "precision": round(hits / n, 4) if n else None,
                "recall": round(len(eps_hit) / len(eps), 4) if eps else None}

    first = score(raised)
    kept = raised[[(r.feeder_id, r.event_time) not in retracted_keys for r in raised.itertuples()]]
    final = score(pd.concat([kept, late]))
    res.update({f"{prefix}_first_{k}": v for k, v in first.items()})
    res.update({f"{prefix}_final_{k}": v for k, v in final.items()})
    res[f"{prefix}_late_detected"] = int(len(late))
    res[f"{prefix}_retracted"] = int(len(retracted))
    return res


def pipeline_latency(fl: pd.DataFrame, stream: str, watermark_s: float, run: str = None) -> dict:
    """Wall-clock ms from 'event that closed the window was sent' to 'result written'."""
    man, send = stream_meta(stream)
    own = config.path("results", "outputs", run, "kafka_send.json") if run else None
    if own and os.path.exists(own):                  # this run's own send timing
        with open(own) as f:
            send = json.load(f)
    elif send is not None and run:                   # stream-level file: only valid if it belongs to this run
        info = run_info(run)
        if not (info["started_wall_ms"] <= send["wall_start_s"] * 1000 <= info.get("finished_wall_ms", 1e18)):
            return {"latency_note": "send timing of this run not available (stream replayed again later)"}
    if send is None or fl.empty:
        return {}
    with open(config.path("data", "stream", stream, "events.jsonl")) as f:
        ev = [json.loads(line) for line in f]
    arr = np.array([e["arrival_time"] for e in ev], dtype=np.int64)
    evt = np.array([e["event_time"] for e in ev], dtype=np.int64)
    run_max = np.maximum.accumulate(evt)                    # max event time seen, in arrival order
    t0, w0, sp = send["first_arrival_ms"], send["wall_start_s"], send["speedup"]
    first = fl[~fl["revision"]].sort_values("firing").groupby(["feeder_id", "minute"]).head(1)
    lat = []
    d_ms = int(watermark_s * 1000)
    for r in first.itertuples():
        close = r.minute + MIN - 1 + d_ms + 1            # watermark (max - D - 1) must reach window end - 1
        i = bisect.bisect_left(run_max, close)
        if i >= len(arr):
            continue                                     # closed by end-of-stream, not by an event
        sent_wall_ms = (w0 + (arr[i] - t0) / 1000.0 / sp) * 1000
        lat.append(r.emitted_wall_ms - sent_wall_ms)
    if not lat:
        return {}
    lat = np.array(lat)
    return {"latency_ms_p50": round(float(np.percentile(lat, 50)), 1),
            "latency_ms_p95": round(float(np.percentile(lat, 95)), 1),
            "latency_ms_max": round(float(lat.max()), 1),
            "latency_samples": int(len(lat)),
            "event_time_wait_s": watermark_s,
            "_note": "real-world time-to-result = window end + D (watermark wait) + pipeline latency"}


# --------------------------------------------------------------------------- main
def evaluate(run: str) -> dict:
    info = run_info(run)
    cap = info["capacity_kw"]
    gt = ground_truth(info["stream"], cap)
    fl = load_sink(run, "feeder_load")
    al = load_sink(run, "alerts")
    pr = load_sink(run, "probe")
    dl = load_sink(run, "dropped_late")
    se = load_sink(run, "sessions")
    man, send = stream_meta(info["stream"])
    m = {"run": run, "stream": info["stream"], "watermark_s": info["watermark_s"],
         "allowed_lateness_min": info.get("allowed_lateness_min"),
         "late_fraction": man["network_model"]["late_fraction"],
         "late_delay_min": man["network_model"]["late_delay_min"],
         "events": man["events"], "sessions": man["sessions"],
         "runtime_s": round(info.get("net_runtime_ms", 0) / 1000, 1),
         "send_s": round(send["wall_end_s"] - send["wall_start_s"], 1) if send else None,
         "throughput_events_per_s": round(man["events"] / (info["net_runtime_ms"] / 1000), 1)
         if info.get("net_runtime_ms") else None}
    m["late_events_seen"] = int(pr["late"].sum()) if not pr.empty else 0
    m["dropped_beyond_lateness"] = int(len(dl))
    m["feeder_minute_revisions"] = int(fl["revision"].sum()) if not fl.empty else 0
    m.update(load_accuracy(fl, gt["minute_load"]))
    m.update(alert_quality(al, gt["episodes_spike"], "SPIKE"))
    m.update(alert_quality(al, gt["episodes_sustained"], "SUSTAINED"))
    m.update(pipeline_latency(fl, info["stream"], info["watermark_s"], run))
    if not se.empty:
        m["sessions_summarised"] = int(len(se))
        m["sessions_stale"] = int((se["status"] == "STALE").sum())
        m["energy_kwh_flink"] = round(float(se["energy_kwh_measured"].sum()), 2)
        m["cost_usd_flink"] = round(float(se["cost_usd"].sum()), 2)
    os.makedirs(config.path("results", "metrics"), exist_ok=True)
    with open(config.path("results", "metrics", f"{run}.json"), "w") as f:
        json.dump(m, f, indent=2, default=str)
    return m


def main() -> None:
    if len(sys.argv) < 2:
        sys.exit("usage: python analysis/evaluate_run.py <run> [<run> ...]")
    rows = []
    for run in sys.argv[1:]:
        m = evaluate(run)
        rows.append(m)
        print(f"\n=== {run} ===")
        for k, v in m.items():
            if not k.startswith("_"):
                print(f"  {k:<34} {v}")
    if len(rows) > 1:
        keys = ["run", "watermark_s", "allowed_lateness_min", "late_events_seen", "dropped_beyond_lateness",
                "first_exact_pct", "final_exact_pct", "SPIKE_first_precision", "SPIKE_first_recall",
                "SPIKE_final_precision", "SPIKE_final_recall", "latency_ms_p50"]
        print("\n" + pd.DataFrame(rows)[[k for k in keys if k in rows[0]]].to_string(index=False))


if __name__ == "__main__":
    main()
