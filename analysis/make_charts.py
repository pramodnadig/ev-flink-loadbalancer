"""
make_charts.py - all figures for the report and presentation (Stage 8).

Reads the run outputs in results/outputs/ and metrics in results/metrics/ and writes
PNG charts to results/charts/. The reference ("true") load is the final output of the
on-time run E1_event_time, which evaluate_run.py verified to be 100% identical to the
offline ground truth.

Usage:  python analysis/make_charts.py [results_dir]
"""
from __future__ import annotations

import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use("Agg")
import matplotlib.dates as mdates          # noqa: E402
import matplotlib.pyplot as plt            # noqa: E402
import numpy as np                         # noqa: E402
import pandas as pd                        # noqa: E402

RES = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
OUT = os.path.join(RES, "charts")
TZ = ZoneInfo("America/Los_Angeles")
REF = "E1_event_time"

# ---- palette (validated categorical order; text in ink, never in series colour) ----
FEEDER = {"F1-DOWNTOWN-N": "#2a78d6", "F2-DOWNTOWN-S": "#eb6834",
          "F3-CAL-AVE": "#1baf7a", "F4-LIBRARIES": "#eda100"}
FEEDER_NAME = {"F1-DOWNTOWN-N": "F1 Downtown North", "F2-DOWNTOWN-S": "F2 Downtown South",
               "F3-CAL-AVE": "F3 California Ave", "F4-LIBRARIES": "F4 Libraries"}
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0"
TRUTH, FIRST, FINAL = "#52514e", "#eb6834", "#2a78d6"
CRIT, WARN = "#c0392b", "#b7791f"

plt.rcParams.update({
    "figure.dpi": 110, "savefig.dpi": 220, "font.size": 9.5, "axes.titlesize": 11,
    "axes.titleweight": "bold", "axes.titlelocation": "left", "axes.edgecolor": MUTED,
    "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True,
    "grid.color": GRID, "grid.linewidth": 0.6, "axes.spines.top": False,
    "axes.spines.right": False, "legend.frameon": False, "text.color": INK,
    "lines.linewidth": 1.4, "font.family": "DejaVu Sans", "axes.axisbelow": True})


# --------------------------------------------------------------------------- helpers
def sink(run: str, name: str) -> pd.DataFrame:
    rows = []
    for fp in sorted(glob.glob(os.path.join(RES, "outputs", run, name, "*.jsonl"))):
        with open(fp) as f:
            rows.extend(json.loads(l) for l in f if l.strip())
    return pd.DataFrame(rows)


def metrics(run: str) -> dict:
    with open(os.path.join(RES, "metrics", f"{run}.json")) as f:
        return json.load(f)


def local(ms):
    return pd.to_datetime(ms, unit="ms", utc=True).dt.tz_convert(TZ).dt.tz_localize(None) \
        if isinstance(ms, pd.Series) else \
        datetime.fromtimestamp(ms / 1000, TZ).replace(tzinfo=None)


def to_ms(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%d %H:%M").replace(tzinfo=TZ).timestamp() * 1000)


def minute_series(run: str, view: str = "final") -> pd.DataFrame:
    """feeder x minute load. view: 'final' (after corrections) or 'first' (on-time result)."""
    fl = sink(run, "feeder_load").sort_values("firing")
    if view == "first":
        fl = fl[~fl["revision"]].groupby(["feeder_id", "minute"]).head(1)
    else:
        fl = fl.groupby(["feeder_id", "minute"]).tail(1)
    return fl.pivot_table(index="minute", columns="feeder_id", values="load_kw", aggfunc="last")


def capacity(run: str = REF) -> dict:
    with open(os.path.join(RES, "outputs", run, "run_info.json")) as f:
        return json.load(f)["capacity_kw"]


def time_axis(ax, fmt="%H:%M"):
    ax.xaxis.set_major_formatter(mdates.DateFormatter(fmt))
    for lab in ax.get_xticklabels():
        lab.set_rotation(0)


def save(fig, name: str, caption: str):
    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, name)
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"[chart] {name:<36} {caption}")


def reindex_minutes(s: pd.Series, t0: int, t1: int) -> pd.Series:
    idx = np.arange(t0 - t0 % 60000, t1, 60000)
    return s.reindex(idx).fillna(0.0)


# --------------------------------------------------------------------------- charts
def fig_dataset_hours():
    path = os.path.join(RES, "logs", "dataset_profile.txt")
    if not os.path.exists(path):
        return
    txt = open(path).read()
    block = txt.split("Sessions by hour of day (start):")[1].split("=====")[0]
    pairs = [tuple(map(int, l.split())) for l in block.strip().splitlines()[1:] if re.match(r"^\d+\s+\d+$", l.strip())]
    hours, n = zip(*pairs)
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    ax.bar(hours, n, color=FEEDER["F1-DOWNTOWN-N"], width=0.8)
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlabel("Hour of day (local time)")
    ax.set_ylabel("Sessions started")
    ax.set_title("When drivers plug in: 259k Palo Alto sessions, 2011-2020")
    ax.grid(axis="x", visible=False)
    save(fig, "fig_dataset_sessions_by_hour.png", "dataset: two daily peaks (late morning, early evening)")


def fig_feeder_load():
    s = minute_series(REF)
    cap = capacity()
    al = sink(REF, "alerts")
    fig, axes = plt.subplots(4, 1, figsize=(8, 7.2), sharex=True)
    idx = np.arange(s.index.min(), s.index.max() + 60000, 60000)
    t = local(pd.Series(idx))
    for ax, f in zip(axes, FEEDER):
        pct = s[f].reindex(idx).fillna(0) / cap[f] * 100
        ax.fill_between(t, pct, color=FEEDER[f], alpha=0.18, linewidth=0)
        ax.plot(t, pct, color=FEEDER[f], linewidth=0.9)
        ax.axhline(100, color=CRIT, linewidth=0.9, linestyle="--")
        ax.axhline(80, color=WARN, linewidth=0.8, linestyle=":")
        a = al[al["feeder_id"] == f] if not al.empty else al
        for r in a.itertuples():
            marker = "v" if r.alert_type.startswith("SPIKE") else "o"
            ax.plot(local(r.event_time), 110, marker=marker, color=CRIT if marker == "v" else WARN,
                    markersize=6, markeredgecolor="white")
        ax.set_ylim(0, 118)
        ax.set_ylabel("% of capacity")
        ax.set_title(f"{FEEDER_NAME[f]}  (capacity {cap[f]:.1f} kW)", fontsize=9.5, pad=3)
    axes[-1].xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%a %H:%M"))
    fig.suptitle("Event-time rolling feeder load, 22-23 Jan 2020 (1-minute windows)",
                 x=0.01, ha="left", fontsize=11, fontweight="bold", y=1.0)
    fig.text(0.01, 0.965, "dashed = 100% spike threshold    dotted = 80% sustained threshold    "
             "▼ SPIKE alert    ● SUSTAINED alert (episode start)", fontsize=8, color=INK2)
    fig.tight_layout(rect=(0, 0, 1, 0.955))
    save(fig, "fig_feeder_load_2days.png", "Part A/D: rolling load per feeder with alerts")


def fig_event_vs_processing():
    proc = sink(REF, "proc_time_load")
    if proc.empty:
        return
    f = "F2-DOWNTOWN-S"
    t0, t1 = to_ms("2020-01-23 09:00"), to_ms("2020-01-23 13:00")
    truth = reindex_minutes(minute_series(REF)[f], t0, t1)
    p = proc[(proc["feeder_id"] == f)].copy()
    p["minute"] = p["max_event_time"] - p["max_event_time"] % 60000
    p = p[(p["minute"] >= t0) & (p["minute"] < t1)].sort_values("wall_start_ms")
    fig, ax = plt.subplots(figsize=(8, 3.3))
    ax.plot(local(pd.Series(truth.index)), truth.values, color=TRUTH, linewidth=1.6,
            label="Event time (correct load)")
    ax.plot(local(p["minute"]), p["load_kw"], color=FIRST, linewidth=0.9, alpha=0.9,
            label="Processing time (by arrival)")
    ax.axhline(capacity()[f], color=CRIT, linestyle="--", linewidth=0.9)
    ax.text(local(t0), capacity()[f] + 1, "capacity", color=CRIT, fontsize=8)
    ax.set_ylabel("Feeder load (kW)")
    time_axis(ax)
    ax.legend(loc="upper left", ncol=1, fontsize=8)
    # error statistics over the whole run
    allp = proc[proc["feeder_id"] == f].copy()
    allp["minute"] = allp["max_event_time"] - allp["max_event_time"] % 60000
    tr = minute_series(REF)[f]
    err = (allp["load_kw"] - allp["minute"].map(tr).fillna(0)).abs()
    ax.set_title("E1 - Same readings, two clocks: F2 Downtown South, 23 Jan 09:00-13:00")
    ax.set_ylim(0, max(160, float(p["load_kw"].max()) * 1.1))
    ax.set_xlabel(f"Whole run: the processing-time result is wrong in {100 * (err > 0.01).mean():.0f}% "
                  f"of windows (mean error {err.mean():.1f} kW); the event-time result is 100% correct",
                  fontsize=8, color=INK2)
    save(fig, "fig_E1_event_vs_processing_time.png", "E1: processing-time load is distorted")
    return {"proc_windows": int(len(allp)), "proc_wrong_pct": round(100 * float((err > 0.01).mean()), 1),
            "proc_mae_kw": round(float(err.mean()), 2)}


def fig_watermark_tradeoff():
    runs = [("E2_D0", 0), ("E2_D30", 30), ("E2_D120", 120), ("E2_D300", 300), ("E2_D660", 660)]
    rows = []
    for r, d in runs:
        m = metrics(r)
        false_alerts = (m["SPIKE_first_alerts"] - m["SPIKE_first_correct"]
                        + m["SUSTAINED_first_alerts"] - m["SUSTAINED_first_correct"])
        rows.append({"D": d, "exact": m["first_exact_pct"], "dropped": m["dropped_beyond_lateness"],
                     "late": m["late_events_seen"], "false_alerts": false_alerts,
                     "wrong_minutes": m["first_minutes_overestimated"] + m["first_minutes_underestimated"]})
    df = pd.DataFrame(rows)
    fig, axes = plt.subplots(1, 2, figsize=(9, 3.4))
    ax = axes[0]
    ax.plot(df["D"], df["exact"], color=FINAL, marker="o", markersize=7, markeredgecolor="white")
    for r in df.itertuples():
        ax.annotate(f"{r.exact:.1f}%", (r.D, r.exact), textcoords="offset points", xytext=(0, 8),
                    ha="center", fontsize=8, color=INK)
    ax.set_xlabel("Watermark delay D (s)  =  extra wait before a result")
    ax.set_ylabel("Feeder-minutes exactly right (%)")
    ax.set_title("Completeness rises with D")
    ax.set_ylim(93, 101)
    ax = axes[1]
    w = 0.38
    x = np.arange(len(df))
    ax.bar(x - w / 2, df["late"], w, color=FIRST, label="late events (arrived after watermark)")
    ax.bar(x + w / 2, df["dropped"], w, color=TRUTH, label="window records dropped (beyond lateness)")
    for i, r in df.iterrows():
        if r.late == 0 and r.dropped == 0:
            ax.text(i, 12, "0 / 0", ha="center", fontsize=8, color=INK)
    ax.set_xticks(x, [f"D={d}s" for d in df["D"]])
    ax.set_ylabel("Count")
    ax.set_ylim(0, df["dropped"].max() * 1.45)
    ax.set_title("Late and dropped data fall with D")
    ax.legend(loc="upper right", fontsize=7.5)
    ax.grid(axis="x", visible=False)
    fig.suptitle("E2 - Watermark trade-off (10% of plug/unplug events delayed 10 min, no allowed lateness)",
                 x=0.01, ha="left", fontsize=10.5, fontweight="bold", y=1.04)
    save(fig, "fig_E2_watermark_tradeoff.png", "E2: latency vs completeness")
    return df


def _window_plot(ax, truth, series, t0, t1, cap_kw, title):
    ax.plot(local(pd.Series(truth.index)), truth.values, color=TRUTH, linewidth=2.2, label="True load", zorder=1)
    for label, s, color, style in series:
        ax.plot(local(pd.Series(s.index)), s.values, color=color, linewidth=1.3, linestyle=style,
                label=label, zorder=2)
    ax.axhline(cap_kw, color=CRIT, linestyle="--", linewidth=0.9)
    ax.set_ylabel("Feeder load (kW)")
    ax.set_title(title)
    time_axis(ax)


def _shade(ax, truth, est):
    t = local(pd.Series(truth.index))
    ax.fill_between(t, truth.values, est.values, where=est.values > truth.values + 1e-6, step=None,
                    color=CRIT, alpha=0.18, linewidth=0, label="phantom load (over-estimate)")
    ax.fill_between(t, truth.values, est.values, where=est.values < truth.values - 1e-6, step=None,
                    color=FINAL, alpha=0.18, linewidth=0, label="missing load (under-estimate)")


def fig_late_example():
    cap = capacity()
    truth_all = minute_series(REF)
    l0 = minute_series("E3_late_L0", "final")
    idx = truth_all.index.union(l0.index)
    diff = (l0.reindex(idx).fillna(0) - truth_all.reindex(idx).fillna(0)).abs()
    area = diff.rolling(120, min_periods=1).sum()         # most distortion in a 2-hour window
    f = area.max().idxmax()
    t1 = int(area[f].idxmax()) + 5 * 60000
    t0 = t1 - 125 * 60000
    tr = reindex_minutes(truth_all[f], t0, t1)
    un = reindex_minutes(l0[f], t0, t1)
    fig, ax = plt.subplots(figsize=(8, 3.4))
    _shade(ax, tr, un)
    _window_plot(ax, tr, [("Uncorrected result (allowed lateness 0)", un, FIRST, "-")], t0, t1, cap[f],
                 f"E3 - Late plug/unplug events distort the load: {FEEDER_NAME[f]}")
    lo = min(tr.min(), un.min())
    ax.set_ylim(lo - (cap[f] - lo) * 0.45, cap[f] * 1.06)
    ax.legend(loc="lower left", fontsize=7.5, ncol=2)
    m = metrics("E3_late_L15")
    ax.set_xlabel(f"10% of plug/unplug events 10 min late, watermark D = 120 s.  With allowed lateness 15 min: "
                  f"{m['feeder_minute_revisions']} corrections and the final result equals the true load",
                  fontsize=7.5, color=INK2)
    save(fig, "fig_E3_late_events_effect.png", f"E3: phantom/missing load on {f}")


def fig_outage():
    cap = capacity()
    truth_all = minute_series(REF)
    l0 = minute_series("E3_outage_L0")
    fig, axes = plt.subplots(2, 1, figsize=(8, 6.2))
    # (a) lost PLUG_IN -> missing load
    f = "F3-CAL-AVE"
    t0, t1 = to_ms("2020-01-23 07:30"), to_ms("2020-01-23 12:00")
    tr, un = reindex_minutes(truth_all[f], t0, t1), reindex_minutes(l0[f], t0, t1)
    ax = axes[0]
    ax.axvspan(local(to_ms("2020-01-23 08:00")), local(to_ms("2020-01-23 09:30")), color=GRID, alpha=0.8, lw=0)
    _shade(ax, tr, un)
    _window_plot(ax, tr, [("Uncorrected (allowed lateness 0)", un, FIRST, "-")], t0, t1, cap[f],
                 "(a) F3: car plugged in during a 90-min outage of CAMBRIDGE-3 - its PLUG_IN was too late")
    ax.text(local(to_ms("2020-01-23 08:03")), cap[f] * 0.93, "charger offline", fontsize=8, color=INK2)
    ax.legend(loc="lower right", fontsize=7.5)
    # (b) lost UNPLUG -> phantom load until the stale-session timeout
    f = "F2-DOWNTOWN-S"
    t0, t1 = to_ms("2020-01-23 10:00"), to_ms("2020-01-24 13:00")
    tr, un = reindex_minutes(truth_all[f], t0, t1), reindex_minutes(l0[f], t0, t1)
    ax = axes[1]
    ax.axvspan(local(to_ms("2020-01-23 12:00")), local(to_ms("2020-01-23 13:00")), color=GRID, alpha=0.8, lw=0)
    _shade(ax, tr, un)
    _window_plot(ax, tr, [("Uncorrected (allowed lateness 0)", un, FIRST, "-")], t0, t1, cap[f],
                 "(b) F2: a car unplugged during a 60-min outage of HIGH-2 - its UNPLUG was too late")
    ax.xaxis.set_major_locator(mdates.HourLocator(byhour=[0, 6, 12, 18]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%a %H:%M"))
    ax.text(0.99, 0.9, "phantom car removed only by the 24 h stale-session timeout", transform=ax.transAxes,
            ha="right", fontsize=8, color=INK2)
    m = metrics("E3_outage_L120")
    fig.suptitle(f"E3 - Charger outages (no correction). With 120-min allowed lateness: "
                 f"{m['feeder_minute_revisions']} corrections, final result 100% exact",
                 x=0.01, ha="left", fontsize=10, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.96))
    save(fig, "fig_E3_outage.png", "E3: outage -> missing and phantom load")


def fig_accuracy_bars():
    runs = [("E3_base", "Baseline\n(no late data)"), ("E3_late_L0", "Late 10%\nL = 0"),
            ("E3_late_L15", "Late 10%\nL = 15 min"), ("E3_outage_L0", "Outages\nL = 0"),
            ("E3_outage_L120", "Outages\nL = 120 min")]
    first = [metrics(r)["first_exact_pct"] for r, _ in runs]
    final = [metrics(r)["final_exact_pct"] for r, _ in runs]
    x = np.arange(len(runs))
    w = 0.38
    fig, ax = plt.subplots(figsize=(8, 3.3))
    b1 = ax.bar(x - w / 2, first, w, color=FIRST, label="first result (what the operator sees first)")
    b2 = ax.bar(x + w / 2, final, w, color=FINAL, label="final result (after late corrections)")
    for bars in (b1, b2):
        for b in bars:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.3, f"{b.get_height():.1f}",
                    ha="center", fontsize=8, color=INK)
    ax.set_xticks(x, [l for _, l in runs])
    ax.set_ylim(75, 103)
    ax.set_ylabel("Feeder-minutes exactly right (%)")
    ax.set_title("E3/E4 - Accuracy against ground truth: damage from late data and its repair", pad=22)
    ax.legend(loc="lower left", bbox_to_anchor=(0.0, 1.0), ncol=2, fontsize=8)
    ax.grid(axis="x", visible=False)
    save(fig, "fig_E3_accuracy.png", "E3/E4: first vs final accuracy")


def fig_throughput():
    span_s = 228.7 * 900          # event-time span of the replayed stream (send time at 900x)
    rows = []
    for run, sp in [("E3_base", 900), ("E6_speed3600", 3600), ("E6_speed14400", 14400)]:
        m = metrics(run)
        rows.append({"speedup": sp, "offered": m["events"] * sp / span_s,
                     "processed": m["events"] / m["runtime_s"], "p50": m.get("latency_ms_p50")})
    df = pd.DataFrame(rows)
    x = np.arange(len(df))
    w = 0.38
    fig, ax = plt.subplots(figsize=(7.5, 3.2))
    ax.bar(x - w / 2, df["offered"], w, color=TRUTH, label="offered rate (events/s sent)")
    ax.bar(x + w / 2, df["processed"], w, color=FINAL, label="processed rate (events/s, incl. start-up)")
    for i, r in df.iterrows():
        ax.text(i + w / 2, r.processed + 40, f"{r.processed:.0f}", ha="center", fontsize=8)
        ax.text(i - w / 2, r.offered + 40, f"{r.offered:.0f}", ha="center", fontsize=8)
    ax.set_xticks(x, [f"{s}x replay" for s in df["speedup"]])
    ax.set_ylabel("Events per second")
    ax.set_title("E6 - Throughput: the single-slot PyFlink job saturates at ~1,100 events/s")
    ax.legend(loc="upper left", fontsize=8)
    ax.grid(axis="x", visible=False)
    save(fig, "fig_E6_throughput.png", "E6: throughput / backpressure")


def fig_station_summary():
    sd = sink(REF, "station_daily")
    if sd.empty:
        return
    g = sd.groupby("station_id").agg(energy=("energy_kwh", "sum"), cost=("cost_usd", "sum"),
                                     sessions=("sessions", "sum"), feeder=("feeder_id", "first"))
    g = g.sort_values("energy").tail(14)
    fig, ax = plt.subplots(figsize=(7.5, 4.2))
    ax.barh(g.index, g["energy"], color=[FEEDER[f] for f in g["feeder"]], height=0.7)
    for i, r in enumerate(g.itertuples()):
        ax.text(r.energy + 2, i, f"{r.energy:.0f} kWh · ${r.cost:.0f} · {r.sessions} sessions",
                va="center", fontsize=7.5, color=INK2)
    ax.set_xlabel("Energy delivered, 22-23 Jan 2020 (kWh)")
    ax.set_title("Station summaries (busiest 14 stations, colour = feeder)")
    ax.grid(axis="y", visible=False)
    ax.set_xlim(0, g["energy"].max() * 1.45)
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in FEEDER.values()]
    ax.legend(handles, FEEDER_NAME.values(), loc="lower right", fontsize=7.5)
    save(fig, "fig_station_summary.png", "Part D: station summaries with cost")


def main():
    print(f"results: {RES}")
    fig_dataset_hours()
    fig_feeder_load()
    e1 = fig_event_vs_processing()
    fig_watermark_tradeoff()
    fig_late_example()
    fig_outage()
    fig_accuracy_bars()
    fig_throughput()
    fig_station_summary()
    if e1:
        with open(os.path.join(OUT, "E1_processing_time_stats.json"), "w") as f:
            json.dump(e1, f, indent=2)
        print("E1 processing-time stats:", e1)


if __name__ == "__main__":
    main()
