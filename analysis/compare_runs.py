"""
compare_runs.py - are two runs' results identical? (E5: recovery experiment)

A run with an injected failure is restarted by Flink from its last checkpoint. With
exactly-once checkpointing (Kafka offsets + operator state + transactional file sink)
its committed outputs must be IDENTICAL to a run without failure: nothing lost
(e.g. active sessions forgotten) and nothing duplicated (e.g. alerts raised twice).

Wall-clock and watermark fields legitimately differ and are ignored.

Usage:  python analysis/compare_runs.py <reference run> <recovered run>
"""
from __future__ import annotations

import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from analysis.evaluate_run import load_sink  # noqa: E402

IGNORE = {"emitted_wall_ms", "watermark", "dropped_wall_ms", "firing", "revision"}
KEYS = {"feeder_load": ["feeder_id", "minute"], "sustained": ["feeder_id", "window_end"],
        "alerts": ["alert_type", "feeder_id", "event_time"], "sessions": ["session_id"],
        "station_daily": ["station_id", "day_local"]}


def final_view(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    if "firing" in df.columns:
        df = df.sort_values("firing").groupby(keys, as_index=False).tail(1)
    cols = [c for c in df.columns if c not in IGNORE]
    out = df[cols].copy()
    for c in out.columns:
        if out[c].map(lambda v: isinstance(v, (dict, list))).any():
            out[c] = out[c].astype(str)
    return out.sort_values(keys).reset_index(drop=True)


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: python analysis/compare_runs.py <reference run> <recovered run>")
    ref, rec = sys.argv[1], sys.argv[2]
    marker = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                          "results", "outputs", rec, "FAILURE_INJECTED.txt")
    print(f"reference: {ref}   recovered: {rec}")
    if os.path.exists(marker):
        print("failure  :", open(marker).read().strip())
    else:
        print("WARNING: no failure marker found in the recovered run")
    all_ok = True
    for sink, keys in KEYS.items():
        a, b = final_view(load_sink(ref, sink), keys), final_view(load_sink(rec, sink), keys)
        if a.empty and b.empty:
            continue
        dup = int(load_sink(rec, sink).duplicated(subset=keys + (["firing"] if "firing" in
                                                                     load_sink(rec, sink).columns else [])).sum())
        same = len(a) == len(b) and a.round(6).equals(b.round(6))
        if not same:
            merged = a.merge(b, on=keys, how="outer", indicator=True, suffixes=("_ref", "_rec"))
            only = merged["_merge"].value_counts().to_dict()
        else:
            only = {}
        all_ok &= same and dup == 0
        print(f"  {sink:<14} ref={len(a):>6} rec={len(b):>6} identical={same}  duplicates_in_recovered={dup}"
              + (f"  diff={only}" if only else ""))
    print("RESULT:", "IDENTICAL - state and outputs fully recovered (exactly-once)" if all_ok
          else "DIFFERENCES FOUND - see above")


if __name__ == "__main__":
    main()
