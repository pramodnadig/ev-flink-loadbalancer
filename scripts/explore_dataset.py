"""
explore_dataset.py - profile the Palo Alto EV charging dataset (Stage 1, Part A).

Why this exists
---------------
Before designing an event model we need to *understand the data*: what entities
exist (stations, sites, ports), what time range it covers, how clean it is, and
how much power cars actually draw. These numbers drive three design decisions:
  1. how stations are grouped into feeders,
  2. which period of history is replayed as the live stream,
  3. which data-quality problems the pipeline has to tolerate.

Usage
-----
    source ~/ev-flink/ev-flink-loadbalancer/env.sh
    python scripts/explore_dataset.py

Output
------
Prints a report to the terminal and saves it to results/logs/dataset_profile.txt
"""
from __future__ import annotations

import io
import os
import re
import sys
from contextlib import redirect_stdout

import pandas as pd

PROJECT_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(PROJECT_HOME, "data", "raw", "palo_alto_ev_sessions.csv")
OUT = os.path.join(PROJECT_HOME, "results", "logs", "dataset_profile.txt")


def hms_to_hours(s: pd.Series) -> pd.Series:
    """'3:03:32' -> 3.0589 hours. Bad values become NaN."""
    parts = s.astype(str).str.split(":", expand=True)
    if parts.shape[1] < 3:
        return pd.Series(float("nan"), index=s.index)
    h = pd.to_numeric(parts[0], errors="coerce")
    m = pd.to_numeric(parts[1], errors="coerce")
    sec = pd.to_numeric(parts[2], errors="coerce")
    return h + m / 60 + sec / 3600


def site_of(station: str) -> str:
    """'PALO ALTO CA / HAMILTON #1' -> 'HAMILTON' (the physical site/car park)."""
    name = str(station).split("/", 1)[-1]
    name = re.sub(r"#\s*\d+\s*$", "", name)
    return name.strip().upper()


def section(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> None:
    if not os.path.exists(RAW):
        sys.exit(f"Dataset not found: {RAW}\nRun: bash scripts/download_data.sh")

    df = pd.read_csv(RAW, low_memory=False)

    section("1. SIZE AND COLUMNS")
    print(f"Rows (sessions): {len(df):,}")
    print(f"Columns ({df.shape[1]}): {list(df.columns)}")

    # ---- parse times -------------------------------------------------------
    df["start"] = pd.to_datetime(df["Start Date"], format="%m/%d/%Y %H:%M", errors="coerce")
    df["end"] = pd.to_datetime(df["End Date"], format="%m/%d/%Y %H:%M", errors="coerce")
    df["dur_h"] = hms_to_hours(df["Total Duration (hh:mm:ss)"])
    df["chg_h"] = hms_to_hours(df["Charging Time (hh:mm:ss)"])
    df["kwh"] = pd.to_numeric(df["Energy (kWh)"], errors="coerce")
    df["site"] = df["Station Name"].map(site_of)

    section("2. TIME COVERAGE")
    print(f"First session start : {df['start'].min()}")
    print(f"Last session start  : {df['start'].max()}")
    print(f"Unparseable start   : {df['start'].isna().sum():,}")
    print(f"Unparseable end     : {df['end'].isna().sum():,}")
    print("Start time zones    :", df["Start Time Zone"].value_counts(dropna=False).to_dict())
    print("\nSessions per year:")
    print(df["start"].dt.year.value_counts().sort_index().to_string())

    section("3. ENTITIES: STATIONS, SITES, PORTS")
    print(f"Distinct station names : {df['Station Name'].nunique()}")
    print(f"Distinct MAC addresses : {df['MAC Address'].nunique()}")
    print(f"Distinct sites         : {df['site'].nunique()}")
    print("Org names              :", df["Org Name"].value_counts().to_dict())
    print("Port types             :", df["Port Type"].value_counts(dropna=False).to_dict())
    print("Plug types             :", df["Plug Type"].value_counts(dropna=False).to_dict())
    print("Ports per station      :", df["Port Number"].value_counts(dropna=False).to_dict())

    print("\nSites (stations, sessions, first/last year, lat, lon):")
    site_tbl = df.groupby("site").agg(
        stations=("Station Name", "nunique"),
        sessions=("Station Name", "size"),
        first=("start", lambda s: s.min().year if s.notna().any() else None),
        last=("start", lambda s: s.max().year if s.notna().any() else None),
        lat=("Latitude", "median"),
        lon=("Longitude", "median"),
    ).sort_values("sessions", ascending=False)
    print(site_tbl.to_string())

    print("\nStation names per site:")
    for site, names in df.groupby("site")["Station Name"].unique().items():
        print(f"  {site}: {sorted(names)}")

    section("4. DATA QUALITY")
    checks = {
        "energy missing": df["kwh"].isna().sum(),
        "energy <= 0": (df["kwh"] <= 0).sum(),
        "energy > 100 kWh": (df["kwh"] > 100).sum(),
        "total duration missing": df["dur_h"].isna().sum(),
        "total duration <= 0": (df["dur_h"] <= 0).sum(),
        "total duration > 24 h": (df["dur_h"] > 24).sum(),
        "charging time > total duration": (df["chg_h"] > df["dur_h"] + 1e-6).sum(),
        "end before start": (df["end"] < df["start"]).sum(),
        "duplicate Plug In Event Id+station": df.duplicated(["Station Name", "Plug In Event Id"]).sum(),
        "fully duplicated rows": df.duplicated().sum(),
    }
    for k, v in checks.items():
        print(f"  {k:<38}: {int(v):>8,}  ({v / len(df):.2%})")

    section("5. CHARGING BEHAVIOUR")
    ok = df[(df["kwh"] > 0) & (df["chg_h"] > 0) & (df["dur_h"] > 0)].copy()
    ok["avg_kw"] = ok["kwh"] / ok["chg_h"]
    for col, label in [("kwh", "Energy per session (kWh)"),
                       ("dur_h", "Plugged-in duration (h)"),
                       ("chg_h", "Actively charging (h)"),
                       ("avg_kw", "Average charging power (kW)")]:
        q = ok[col].quantile([0.05, 0.25, 0.5, 0.75, 0.95, 0.99])
        print(f"  {label:<30} " + "  ".join(f"p{int(p*100)}={v:.2f}" for p, v in q.items()))
    print("\nAverage power by port type (median kW):")
    print(ok.groupby("Port Type")["avg_kw"].median().round(2).to_string())
    print(f"\nShare of plugged-in time spent idle (not charging): "
          f"{1 - ok['chg_h'].sum() / ok['dur_h'].sum():.1%}")
    print("\nSessions by hour of day (start):")
    print(ok["start"].dt.hour.value_counts().sort_index().to_string())

    section("6. BUSIEST PERIODS (candidate replay windows)")
    recent = ok[(ok["start"] >= "2019-01-01") & (ok["start"] < "2020-03-01")]  # pre-COVID
    weekly = recent.set_index("start").resample("W-MON")["kwh"].agg(["size", "sum"])
    weekly.columns = ["sessions", "kwh"]
    print("Top 8 weeks by sessions (Jan 2019 - Feb 2020):")
    print(weekly.sort_values("sessions", ascending=False).head(8).to_string())
    print("\nTop 10 days by sessions (Jan 2019 - Feb 2020):")
    print(recent["start"].dt.date.value_counts().head(10).to_string())

    section("7. CONCURRENCY (how many cars charge at once, per site)")
    # Rough peak concurrency per site in 2019, using plugged-in intervals.
    y = ok[(ok["start"] >= "2019-01-01") & (ok["start"] < "2020-01-01")]
    rows = []
    for site, g in y.groupby("site"):
        ev = pd.concat([
            pd.DataFrame({"t": g["start"], "d": 1}),
            pd.DataFrame({"t": g["start"] + pd.to_timedelta(g["chg_h"], unit="h"), "d": -1}),
        ]).sort_values(["t", "d"])
        conc = ev["d"].cumsum()
        rows.append((site, g["Station Name"].nunique(), int(conc.max()) if len(conc) else 0, len(g)))
    print(f"{'site':<28}{'stations':>9}{'peak cars charging':>20}{'sessions 2019':>15}")
    for site, st, peak, n in sorted(rows, key=lambda r: -r[3]):
        print(f"{site:<28}{st:>9}{peak:>20}{n:>15,}")


if __name__ == "__main__":
    buf = io.StringIO()
    with redirect_stdout(buf):
        main()
    text = buf.getvalue()
    print(text)
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        f.write(text)
    print(f"\n[saved] {OUT}")
