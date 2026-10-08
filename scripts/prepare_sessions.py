"""
prepare_sessions.py - clean the raw Palo Alto sessions and build the station/feeder model.
(Stage 1, Part A: frame and model the stream)

Steps
-----
1. Parse every timestamp WITH its own time-zone label (PDT / PST / UTC) and convert
   to UTC epoch milliseconds. This is what Flink will use as event time: one
   unambiguous number, unaffected by daylight-saving changes.
2. Normalise names:  'PALO ALTO CA / BRYANT # 1'  ->  site BRYANT, station BRYANT-1,
   and each physical socket ->  port BRYANT-1-P2.
3. Apply explicit, counted cleaning rules (R0..R8) so the report can show exactly
   what was removed and why.
4. Map sites to feeders (config/project.json) and size each feeder's EV capacity
   from the number of charging ports x rated power x diversity factor.
5. Cut out the replay window (13-26 Jan 2020) that the event generator will stream.
6. Calibrate: show how often each feeder would exceed 80% / 100% of capacity for a
   range of diversity factors, so the chosen value is justified by data.

Usage
-----
    source ~/ev-flink/ev-flink-loadbalancer/env.sh
    python scripts/prepare_sessions.py

Outputs (data/processed/)
-------------------------
    sessions_clean.csv      all valid sessions 2011-2020
    sessions_replay.csv     sessions inside the replay window (input to the generator)
    stations.csv            one row per charging station
    feeders.csv             one row per feeder with capacity
    preparation_report.txt  cleaning counts + calibration table (also printed)
"""
from __future__ import annotations

import io
import os
import re
import sys
from contextlib import redirect_stdout

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.power_model import load_profile  # noqa: E402

CFG = config.load()
OUT_DIR = config.path("data", "processed")
TZ_OFFSET_H = {"PDT": -7, "PST": -8, "UTC": 0}   # local = UTC + offset
MS_PER_H = 3_600_000


# --------------------------------------------------------------------------- helpers
def hms_to_hours(s: pd.Series) -> pd.Series:
    parts = s.astype(str).str.split(":", expand=True)
    if parts.shape[1] < 3:
        return pd.Series(np.nan, index=s.index)
    h = pd.to_numeric(parts[0], errors="coerce")
    m = pd.to_numeric(parts[1], errors="coerce")
    sec = pd.to_numeric(parts[2], errors="coerce")
    return h + m / 60 + sec / 3600


def to_utc_ms(local_str: pd.Series, zone: pd.Series) -> pd.Series:
    """'1/13/2020 8:05' + 'PST'  ->  UTC epoch milliseconds (float, NaN if invalid)."""
    local = pd.to_datetime(local_str, format="%m/%d/%Y %H:%M", errors="coerce")
    offset_h = zone.astype(str).str.strip().map(TZ_OFFSET_H)
    local_ms = (local - pd.Timestamp("1970-01-01")) // pd.Timedelta(milliseconds=1)
    return local_ms.astype("float64") - offset_h * MS_PER_H


def split_name(station_name: str):
    """'PALO ALTO CA / BRYANT # 1' -> ('BRYANT', 'BRYANT-1')."""
    name = str(station_name).split("/", 1)[-1].upper()
    name = re.sub(r"\bLIB\b", " ", name)            # 'RINCONADA LIB 1' -> 'RINCONADA 1'
    name = re.sub(r"\s+", " ", name).strip()
    m = re.match(r"^(.*?)\s*#?\s*(\d+)$", name)
    if m:
        site, num = m.group(1).strip(), int(m.group(2))
    else:
        site, num = name, 1
    return site, f"{site.replace(' ', '-')}-{num}"


def local_to_utc_ms(text: str) -> int:
    ts = pd.Timestamp(text).tz_localize(CFG["dataset"]["local_timezone"])
    return int(ts.tz_convert("UTC").value // 1_000_000)


# --------------------------------------------------------------------------- main
def main() -> None:
    raw_path = config.path(CFG["dataset"]["raw_csv"])
    if not os.path.exists(raw_path):
        sys.exit(f"Dataset not found: {raw_path}\nRun: bash scripts/download_data.sh")
    c = CFG["cleaning"]

    df = pd.read_csv(raw_path, low_memory=False)
    n_raw = len(df)
    removed = []                                     # (rule, description, count)

    def drop(mask: pd.Series, rule: str, why: str) -> None:
        nonlocal df
        mask = mask.fillna(False).astype(bool)
        removed.append((rule, why, int(mask.sum())))
        df = df.loc[~mask].copy()

    # R0 exact duplicates
    drop(df.duplicated(), "R0", "exact duplicate rows")

    # --- derive fields ---------------------------------------------------------
    df["start_ms"] = to_utc_ms(df["Start Date"], df["Start Time Zone"])
    df["end_recorded_ms"] = to_utc_ms(df["End Date"], df["End Time Zone"])
    df["dur_h"] = hms_to_hours(df["Total Duration (hh:mm:ss)"])
    df["chg_h"] = hms_to_hours(df["Charging Time (hh:mm:ss)"])
    df["energy_kwh"] = pd.to_numeric(df["Energy (kWh)"], errors="coerce")
    names = df["Station Name"].map(split_name)
    df["site"] = names.map(lambda t: t[0])
    df["station_id"] = names.map(lambda t: t[1])
    df["port_no"] = pd.to_numeric(df["Port Number"], errors="coerce").fillna(1).astype(int)
    df["port_id"] = df["station_id"] + "-P" + df["port_no"].astype(str)
    df["port_type"] = df["Port Type"].fillna("Level 2").astype(str).str.strip()

    # R1..R6 validity rules
    drop(df["start_ms"].isna(), "R1", "start time unparseable or unknown time-zone label")
    drop(df["dur_h"].isna() | (df["dur_h"] <= 0) | (df["dur_h"] > c["max_session_hours"]),
         "R2", f"plugged-in duration missing, <= 0 or > {c['max_session_hours']} h")
    tol_h = c["charging_time_tolerance_min"] / 60
    drop(df["chg_h"].isna() | (df["chg_h"] <= 0) | (df["chg_h"] > df["dur_h"] + tol_h),
         "R3", "charging time missing, <= 0 or longer than plugged-in time")
    df["chg_h"] = np.minimum(df["chg_h"], df["dur_h"])
    drop(df["energy_kwh"].isna() | (df["energy_kwh"] < c["min_energy_kwh"]),
         "R4", f"energy below {c['min_energy_kwh']} kWh (no real charging)")
    drop(df["end_recorded_ms"].notna() & (df["end_recorded_ms"] < df["start_ms"]),
         "R5", "recorded end time before start time (corrupt timestamps)")

    df["power_kw"] = df["energy_kwh"] / df["chg_h"]
    rated = df["port_type"].map(c["rated_kw"]).fillna(c["rated_kw"]["Level 2"])
    drop(df["power_kw"] > rated * c["max_power_over_rated"],
         "R6", f"average power > {c['max_power_over_rated']}x the charger rating (physically impossible)")

    # R7 sites with no feeder in the config
    s2f = config.site_to_feeder()
    df["feeder_id"] = df["site"].map(s2f)
    unmapped = sorted(df.loc[df["feeder_id"].isna(), "site"].unique())
    drop(df["feeder_id"].isna(), "R7", f"site not mapped to any feeder {unmapped}")

    # timestamps of the session life-cycle
    df["start_ms"] = df["start_ms"].astype("int64")
    df["unplug_ms"] = df["start_ms"] + (df["dur_h"] * MS_PER_H).round().astype("int64")
    df["charge_end_ms"] = df["start_ms"] + (df["chg_h"] * MS_PER_H).round().astype("int64")

    # R8 overlapping sessions on the same physical port (two cars, one socket)
    df = df.sort_values(["port_id", "start_ms", "unplug_ms"]).copy()
    prev_unplug = df.groupby("port_id")["unplug_ms"].cummax().groupby(df["port_id"]).shift()
    drop(df["start_ms"] < prev_unplug, "R8", "overlaps an earlier session on the same port")

    # --- final table -------------------------------------------------------------
    df = df.sort_values(["start_ms", "port_id"]).reset_index(drop=True)
    df["session_id"] = [f"S{i:06d}" for i in range(1, len(df) + 1)]
    tz = CFG["dataset"]["local_timezone"]
    df["start_local"] = (pd.to_datetime(df["start_ms"], unit="ms", utc=True)
                         .dt.tz_convert(tz).dt.strftime("%Y-%m-%d %H:%M"))
    cols = ["session_id", "feeder_id", "site", "station_id", "port_id", "port_type",
            "start_ms", "charge_end_ms", "unplug_ms", "start_local",
            "energy_kwh", "power_kw", "Plug In Event Id"]
    clean = df[cols].rename(columns={"Plug In Event Id": "src_plug_in_event_id"})
    clean["energy_kwh"] = clean["energy_kwh"].round(4)
    clean["power_kw"] = clean["power_kw"].round(4)

    # --- replay window -------------------------------------------------------------
    w0 = local_to_utc_ms(CFG["dataset"]["replay_start_local"])
    w1 = local_to_utc_ms(CFG["dataset"]["replay_end_local"])
    replay = clean[(clean["start_ms"] >= w0) & (clean["start_ms"] < w1)].copy()

    # --- stations and feeders -------------------------------------------------------
    stations = (df[(df["start_ms"] >= w0) & (df["start_ms"] < w1)]
                .groupby(["station_id"])
                .agg(station_name=("Station Name", "first"), site=("site", "first"),
                     feeder_id=("feeder_id", "first"), ports=("port_id", "nunique"),
                     lat=("Latitude", "median"), lon=("Longitude", "median"))
                .reset_index())

    cap = CFG["capacity"]
    feeders = (stations.groupby("feeder_id")
               .agg(sites=("site", lambda s: " + ".join(sorted(set(s)))),
                    stations=("station_id", "nunique"), ports=("ports", "sum"))
               .reset_index())
    feeders["name"] = feeders["feeder_id"].map({k: v["name"] for k, v in CFG["feeders"].items()})
    feeders["installed_kw"] = feeders["ports"] * cap["port_rated_kw"]
    feeders["diversity_factor"] = cap["diversity_factor"]
    feeders["capacity_kw"] = (feeders["installed_kw"] * cap["diversity_factor"]).round(1)
    feeders = feeders[["feeder_id", "name", "sites", "stations", "ports",
                       "installed_kw", "diversity_factor", "capacity_kw"]]

    os.makedirs(OUT_DIR, exist_ok=True)
    clean.to_csv(os.path.join(OUT_DIR, "sessions_clean.csv"), index=False)
    replay.to_csv(os.path.join(OUT_DIR, "sessions_replay.csv"), index=False)
    stations.to_csv(os.path.join(OUT_DIR, "stations.csv"), index=False)
    feeders.to_csv(os.path.join(OUT_DIR, "feeders.csv"), index=False)

    # --- report ---------------------------------------------------------------------
    line = "=" * 78
    print(line + "\n1. CLEANING SUMMARY\n" + line)
    print(f"Raw rows                      : {n_raw:>9,}")
    for rule, why, n in removed:
        print(f"  {rule} removed {n:>7,}  {why}")
    print(f"Clean sessions (2011-2020)    : {len(clean):>9,}  "
          f"({len(clean) / n_raw:.1%} of raw)")
    print(f"Sites kept                    : {sorted(clean['site'].unique())}")

    print("\n" + line + "\n2. REPLAY WINDOW\n" + line)
    print(f"Local window : {CFG['dataset']['replay_start_local']}  ->  "
          f"{CFG['dataset']['replay_end_local']}  ({tz})")
    print(f"UTC ms       : {w0}  ->  {w1}")
    print(f"Sessions     : {len(replay):,}   energy {replay['energy_kwh'].sum():,.0f} kWh")
    print("Sessions per feeder:")
    print(replay.groupby("feeder_id").agg(sessions=("session_id", "size"),
                                         energy_kwh=("energy_kwh", "sum"),
                                         ports_used=("port_id", "nunique")).round(1).to_string())
    interval_ms = CFG["power_model"]["sample_interval_s"] * 1000
    n_samples = sum(max(0, -(-(r.charge_end_ms) // interval_ms) - (-(-r.start_ms // interval_ms)))
                    for r in replay.itertuples())
    print(f"Expected POWER events (1 per {interval_ms // 1000}s while charging): ~{n_samples:,}")

    print("\n" + line + "\n3. STATIONS AND FEEDERS (capacity at diversity factor "
          f"{cap['diversity_factor']})\n" + line)
    print(stations.to_string(index=False))
    print()
    print(feeders.to_string(index=False))

    print("\n" + line + "\n4. CAPACITY CALIBRATION (replay window, 1-minute load)\n" + line)
    prof = load_profile(replay, w0, w1, interval_ms)
    print("Peak load per feeder (kW):", prof.max().round(1).to_dict())
    win = CFG["alerts"]["sustained_window_min"] * 60_000 // interval_ms
    print(f"\n{'DF':>5} {'feeder':<15}{'cap kW':>8}{'peak %':>8}{'min >=100%':>12}"
          f"{'min >=80%':>11}{'15-min avg >=80%':>18}")
    for dfac in [0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.70]:
        for _, f in feeders.iterrows():
            capkw = f["installed_kw"] * dfac
            load = prof[f["feeder_id"]] if f["feeder_id"] in prof else pd.Series(0.0)
            pct = load / capkw * 100
            sustained = (load.rolling(win).mean() / capkw * 100 >= CFG["alerts"]["sustained_threshold_pct"]).sum()
            print(f"{dfac:>5.2f} {f['feeder_id']:<15}{capkw:>8.1f}{pct.max():>8.0f}"
                  f"{(pct >= 100).sum():>12}{(pct >= 80).sum():>11}{sustained:>18}")
        print()
    print("Columns: minutes in the 2-week window at or above the threshold.")
    print("'15-min avg >=80%' = sliding 15-minute windows whose average load is >= 80% (sustained excursions).")


if __name__ == "__main__":
    buf = io.StringIO()
    with redirect_stdout(buf):
        main()
    text = buf.getvalue()
    print(text)
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "preparation_report.txt"), "w") as f:
        f.write(text)
    print(f"[saved] {os.path.join(OUT_DIR, 'preparation_report.txt')}")
