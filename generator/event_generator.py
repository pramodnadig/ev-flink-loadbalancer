"""
event_generator.py - turn real charging sessions into a timestamped event stream.
(Stage 2)

What it produces
----------------
Four event types, one JSON object per line:

  PLUG_IN  a car is connected to a port                      (session starts)
  POWER    the charger reports its current power draw (kW)   (every 60 s while charging,
                                                              plus one 0 kW reading when charging ends)
  UNPLUG   the car is disconnected                           (session ends, total energy)
  TARIFF   the electricity price changes (time-of-use)       (global, not tied to a station)

Every event carries TWO times:

  event_time   when it HAPPENED at the charger          -> what Flink computes on (event time)
  arrival_time when it REACHES the platform              -> decides the ORDER of the stream

In a perfect world arrival_time == event_time. Real chargers sit on cellular links,
buffer readings when offline and retry, so events arrive late and out of order. The
generator models that explicitly, with every effect controlled by a seed so each
experiment is exactly repeatable:

  jitter       every event is delayed by 0..jitter_max_s seconds          (normal network noise)
  out-of-order a fraction of events is delayed by up to ooo_max_delay_s    (retries, congestion)
  late         a fraction of PLUG_IN / UNPLUG events is delayed by
               late_delay_min minutes                                      (Part C experiments)
  outage       every event of one station during a time span is held back
               and delivered in one burst when it comes back online        (Part C experiments)

The stream is sorted by arrival_time; that order is exactly what Flink will see.

Output modes
------------
  --mode file   one file  data/stream/<name>/events.jsonl   (bounded replay, fully
                deterministic, used for the experiments)
  --mode live   many small files written into data/stream/<name>/ in real time,
                `speedup` times faster than reality (default 3600x: 1 hour of charging
                per wall-clock second, the two weeks take ~6 minutes). Used for the
                live demo, monitoring screenshots and the failure/recovery experiment.

Each run also writes manifest.json (settings + statistics) and injected_delays.csv
(every event that was deliberately delayed), which the analysis uses later.

Examples
--------
  python generator/event_generator.py --name baseline
  python generator/event_generator.py --name late10 --late-fraction 0.10 --late-delay-min 10
  python generator/event_generator.py --name outage --outage CAMBRIDGE-3 "2020-01-15 08:00" 45
  python generator/event_generator.py --name live --mode live --clean
"""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.power_model import sample_times  # noqa: E402

CFG = config.load()
TYPE_ORDER = {"TARIFF": 0, "PLUG_IN": 1, "POWER": 2, "UNPLUG": 3}


# --------------------------------------------------------------------------- helpers
def local_to_utc_ms(text: str) -> int:
    ts = pd.Timestamp(text).tz_localize(CFG["dataset"]["local_timezone"])
    return int(ts.tz_convert("UTC").value // 1_000_000)


def fmt_local(ms: int) -> str:
    return (pd.Timestamp(ms, unit="ms", tz="UTC")
            .tz_convert(CFG["dataset"]["local_timezone"]).strftime("%Y-%m-%d %H:%M:%S"))


# --------------------------------------------------------------------------- 1. clean events
def session_events(s) -> list[dict]:
    """All events of one charging session, in the order they happened."""
    interval_ms = CFG["power_model"]["sample_interval_s"] * 1000
    base = {"session_id": s.session_id, "feeder_id": s.feeder_id, "site": s.site,
            "station_id": s.station_id, "port_id": s.port_id}
    ev = [dict(base, event_type="PLUG_IN", event_time=int(s.start_ms),
               power_kw=0.0, energy_kwh=0.0)]
    for t in sample_times(int(s.start_ms), int(s.charge_end_ms), interval_ms):
        elapsed_h = (t - s.start_ms) / 3_600_000
        ev.append(dict(base, event_type="POWER", event_time=int(t),
                       power_kw=round(float(s.power_kw), 4),
                       energy_kwh=round(float(s.power_kw) * elapsed_h, 4)))
    # Charging finished while the car stays plugged in: the charger reports 0 kW.
    # Only sent if the car really sits idle across at least one minute boundary; a car
    # unplugged while still charging sends no 0 kW reading - its UNPLUG ends the load.
    # (That is what makes a LATE UNPLUG leave "phantom" load on the feeder.)
    if -(-int(s.charge_end_ms) // interval_ms) * interval_ms < int(s.unplug_ms):
        ev.append(dict(base, event_type="POWER", event_time=int(s.charge_end_ms),
                       power_kw=0.0, energy_kwh=round(float(s.energy_kwh), 4)))
    ev.append(dict(base, event_type="UNPLUG", event_time=int(s.unplug_ms),
                   power_kw=0.0, energy_kwh=round(float(s.energy_kwh), 4)))
    return ev


def tariff_events(w0: int, w1: int) -> list[dict]:
    """A TARIFF event at every time-of-use boundary in the window (plus one at the start)."""
    tz = CFG["dataset"]["local_timezone"]
    periods = CFG["tariff"]["periods"]
    day = pd.Timestamp(w0, unit="ms", tz="UTC").tz_convert(tz).normalize()
    out = []
    while True:
        for p in periods:
            hh, mm = map(int, p["start"].split(":"))
            t = int((day + pd.Timedelta(hours=hh, minutes=mm)).tz_convert("UTC").value // 1_000_000)
            if t >= w1:
                return out
            if t >= w0 or not out:
                out.append({"event_type": "TARIFF", "event_time": max(t, w0),
                            "feeder_id": "ALL", "tariff_period": p["name"],
                            "usd_per_kwh": p["usd_per_kwh"]})
        day = day + pd.Timedelta(days=1)


# --------------------------------------------------------------------------- 2. network model
def apply_delays(events: list[dict], g: dict, outages: list[tuple[str, int, int]]) -> list[dict]:
    rng = random.Random(g["seed"])
    jitter_ms = int(g["jitter_max_s"] * 1000)
    ooo_ms = int(g["ooo_max_delay_s"] * 1000)
    late_ms = int(g["late_delay_min"] * 60_000)
    late_types = set(g["late_event_types"])
    injected = []
    for e in events:
        delay = rng.randint(0, jitter_ms)
        cause = None
        if rng.random() < g["ooo_fraction"]:
            delay += rng.randint(0, ooo_ms)
        if e["event_type"] in late_types and rng.random() < g["late_fraction"]:
            delay += late_ms
            cause = "late_injection"
        for station, o_start, o_end in outages:
            if e.get("station_id") == station and o_start <= e["event_time"] < o_end:
                delay = max(delay, (o_end - e["event_time"]) + rng.randint(0, jitter_ms))
                cause = "station_outage"
        e["delay_ms"] = delay
        e["arrival_time"] = e["event_time"] + delay
        if cause:
            injected.append({"event_type": e["event_type"], "session_id": e.get("session_id"),
                             "station_id": e.get("station_id"), "feeder_id": e["feeder_id"],
                             "event_time": e["event_time"], "arrival_time": e["arrival_time"],
                             "delay_ms": delay, "cause": cause})
    events.sort(key=lambda e: (e["arrival_time"], e["event_time"], TYPE_ORDER[e["event_type"]]))
    for i, e in enumerate(events, 1):
        e["seq"] = i
        e["event_id"] = f"E{i:07d}"
    return injected


def disorder_stats(events: list[dict]) -> dict:
    """How out-of-order is the stream, as Flink will see it?"""
    max_seen = -1
    ooo = 0
    lag = []
    for e in events:
        if e["event_time"] < max_seen:
            ooo += 1
            lag.append(max_seen - e["event_time"])
        max_seen = max(max_seen, e["event_time"])
    lag_s = pd.Series(lag, dtype="float64") / 1000
    delays_s = pd.Series([e["delay_ms"] for e in events], dtype="float64") / 1000
    return {
        "events_out_of_order": ooo,
        "pct_out_of_order": round(100 * ooo / max(1, len(events)), 2),
        "behind_max_event_time_s": {f"p{q}": round(float(lag_s.quantile(q / 100)), 1)
                                    for q in (50, 90, 99, 100)} if len(lag) else {},
        "delay_s": {f"p{q}": round(float(delays_s.quantile(q / 100)), 1)
                    for q in (50, 90, 99, 100)},
        "behind_more_than_s": {str(th): int((lag_s > th).sum()) for th in (5, 30, 60, 120, 300, 600)},
    }


# --------------------------------------------------------------------------- 3. writers
def to_json(e: dict) -> str:
    return json.dumps(e, separators=(",", ":"))


def write_file(events: list[dict], out_dir: str) -> str:
    path = os.path.join(out_dir, "events.jsonl")
    with open(path, "w") as f:
        for e in events:
            f.write(to_json(e) + "\n")
    return path


def write_live(events: list[dict], out_dir: str, speedup: float, batch_wall_s: float) -> None:
    """
    Release events in real time: an event is written once the replay clock passes its
    arrival_time. Files are written under a hidden name ('.part') and then renamed, so
    Flink never reads a half-written file (Flink ignores files starting with '.').
    """
    t0 = events[0]["arrival_time"]
    slice_ms = batch_wall_s * speedup * 1000
    wall0 = time.time()
    i, batch = 0, 0
    print(f"[live] writing into {out_dir}  (speedup {speedup:.0f}x, a file every {batch_wall_s}s)")
    print("[live] press Ctrl+C to stop early")
    try:
        while i < len(events):
            batch += 1
            limit = t0 + batch * slice_ms
            chunk = []
            while i < len(events) and events[i]["arrival_time"] < limit:
                chunk.append(events[i])
                i += 1
            if chunk:
                name = f"events-{batch:06d}.jsonl"
                tmp = os.path.join(out_dir, "." + name + ".part")
                with open(tmp, "w") as f:
                    f.write("\n".join(to_json(e) for e in chunk) + "\n")
                os.rename(tmp, os.path.join(out_dir, name))
            if batch % 60 == 0 or i >= len(events):
                print(f"[live] {i:>8,}/{len(events):,} events | replay clock {fmt_local(int(limit))}")
            sleep = wall0 + batch * batch_wall_s - time.time()
            if sleep > 0:
                time.sleep(sleep)
    except KeyboardInterrupt:
        print(f"\n[live] stopped by user after {i:,} events")


# --------------------------------------------------------------------------- main
def main() -> None:
    g = dict(CFG["generator"])
    ap = argparse.ArgumentParser(description="EV charging event stream generator")
    ap.add_argument("--name", default="baseline", help="output folder under data/stream/")
    ap.add_argument("--mode", choices=["file", "live"], default="file")
    ap.add_argument("--sessions", default=config.path("data", "processed", "sessions_replay.csv"))
    ap.add_argument("--seed", type=int, default=g["seed"])
    ap.add_argument("--jitter-max-s", type=float, default=g["jitter_max_s"])
    ap.add_argument("--ooo-fraction", type=float, default=g["ooo_fraction"])
    ap.add_argument("--ooo-max-delay-s", type=float, default=g["ooo_max_delay_s"])
    ap.add_argument("--late-fraction", type=float, default=g["late_fraction"])
    ap.add_argument("--late-delay-min", type=float, default=g["late_delay_min"])
    ap.add_argument("--late-types", nargs="+", default=g["late_event_types"],
                    choices=["PLUG_IN", "POWER", "UNPLUG"])
    ap.add_argument("--outage", nargs=3, action="append", default=[],
                    metavar=("STATION_ID", "START_LOCAL", "MINUTES"),
                    help='e.g. --outage CAMBRIDGE-3 "2020-01-15 08:00" 45')
    ap.add_argument("--speedup", type=float, default=g["speedup"])
    ap.add_argument("--batch-wall-s", type=float, default=g["batch_wall_s"])
    ap.add_argument("--start-local", default=None,
                    help='start the replay later than the window start, e.g. "2020-01-21 00:00"')
    ap.add_argument("--days", type=float, default=None, help="only replay N days")
    ap.add_argument("--clean", action="store_true", help="delete the output folder first")
    a = ap.parse_args()

    g.update(seed=a.seed, jitter_max_s=a.jitter_max_s, ooo_fraction=a.ooo_fraction,
             ooo_max_delay_s=a.ooo_max_delay_s, late_fraction=a.late_fraction,
             late_delay_min=a.late_delay_min, late_event_types=a.late_types)

    sessions = pd.read_csv(a.sessions)
    w0 = local_to_utc_ms(CFG["dataset"]["replay_start_local"])
    w1 = local_to_utc_ms(CFG["dataset"]["replay_end_local"])
    if a.start_local:
        w0 = local_to_utc_ms(a.start_local)
        sessions = sessions[sessions["start_ms"] >= w0]
    if a.days:
        w1 = min(w1, w0 + int(a.days * 86_400_000))
        sessions = sessions[sessions["start_ms"] < w1]

    outages = [(st, local_to_utc_ms(start), local_to_utc_ms(start) + int(float(mins) * 60_000))
               for st, start, mins in a.outage]

    # 1. clean, perfectly ordered events
    events = []
    for s in sessions.itertuples(index=False):
        events.extend(session_events(s))
    events.extend(tariff_events(w0, w1))

    # 2. realistic delivery: delays, disorder, late data
    injected = apply_delays(events, g, outages)
    stats = disorder_stats(events)

    # 3. write
    out_dir = config.path("data", "stream", a.name)
    if a.clean and os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    counts = pd.Series([e["event_type"] for e in events]).value_counts().to_dict()
    manifest = {
        "name": a.name, "mode": a.mode, "created": datetime.now().isoformat(timespec="seconds"),
        "sessions": int(len(sessions)), "events": len(events), "events_by_type": counts,
        "window_local": [fmt_local(w0), fmt_local(w1)], "window_utc_ms": [w0, w1],
        "network_model": g, "outages": [{"station": s, "start_ms": b, "end_ms": e} for s, b, e in outages],
        "injected_delays": len(injected), "disorder": stats,
        "speedup": a.speedup if a.mode == "live" else None,
    }
    with open(os.path.join(out_dir, ".manifest.json"), "w") as f:   # hidden: Flink skips it
        json.dump(manifest, f, indent=2)
    pd.DataFrame(injected).to_csv(os.path.join(out_dir, ".injected_delays.csv"), index=False)

    print("=" * 70)
    print(f" Stream '{a.name}'  |  mode={a.mode}  |  {fmt_local(w0)} -> {fmt_local(w1)}")
    print("=" * 70)
    print(f" sessions          : {len(sessions):,}")
    print(f" events            : {len(events):,}   {counts}")
    print(f" jitter / ooo      : 0-{g['jitter_max_s']}s on all, +0-{g['ooo_max_delay_s']}s on "
          f"{g['ooo_fraction']:.0%}")
    print(f" late injection    : {g['late_fraction']:.0%} of {g['late_event_types']} "
          f"delayed {g['late_delay_min']} min")
    print(f" outages           : {[(s, fmt_local(b), int((e - b) / 60000)) for s, b, e in outages]}")
    print(f" deliberately delayed events : {len(injected):,}")
    print(f" out-of-order as seen by Flink: {stats['events_out_of_order']:,} "
          f"({stats['pct_out_of_order']}%)")
    print(f" how far behind the newest event (s): {stats['behind_max_event_time_s']}")
    print(f" events more than N s behind   : {stats['behind_more_than_s']}")
    print("-" * 70)

    if a.mode == "file":
        path = write_file(events, out_dir)
        print(f" written: {path}")
    else:
        write_live(events, out_dir, a.speedup, a.batch_wall_s)
    print(f" manifest: {os.path.join(out_dir, '.manifest.json')}")


if __name__ == "__main__":
    main()
