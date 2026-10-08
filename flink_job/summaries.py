"""
summaries.py - station summaries (Part D) and a processing-time comparison branch (E1).

StationDailySummary
    TumblingEventTimeWindows(1 day, offset aligned to local midnight), keyed by station,
    over the session summaries emitted by StationSessions: sessions, energy, cost,
    average duration and how many sessions were affected by late data.

ProcessingTimeLoad   (only with --proc-time-branch)
    The SAME feeder load computed the naive way: POWER readings grouped by the wall-clock
    time they arrived, in processing-time windows equivalent to one replayed minute
    (60 s / speedup). Comparing it with the event-time result shows why the business
    computation must use event time.
"""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyflink.datastream.functions import ProcessWindowFunction


class StationDailySummary(ProcessWindowFunction):

    def __init__(self, local_tz: str):
        self.local_tz = local_tz

    def open(self, runtime_context):
        self.tz = ZoneInfo(self.local_tz)

    def process(self, key, context: "ProcessWindowFunction.Context", elements):
        rows = list(elements)
        w = context.window()
        n = len(rows)
        energy = sum(r["energy_kwh_measured"] for r in rows)
        yield {"kind": "station_daily", "station_id": key,
               "feeder_id": rows[0]["feeder_id"] if rows else None,
               "day_local": datetime.fromtimestamp(w.start / 1000, self.tz).strftime("%Y-%m-%d"),
               "sessions": n,
               "energy_kwh": round(energy, 3),
               "cost_usd": round(sum(r["cost_usd"] for r in rows), 2),
               "avg_price_usd_per_kwh": round(sum(r["cost_usd"] for r in rows) / energy, 4) if energy else None,
               "avg_duration_min": round(sum(r["duration_min"] for r in rows) / n, 1) if n else None,
               "stale_sessions": sum(1 for r in rows if r["status"] == "STALE"),
               "sessions_with_late_events": sum(1 for r in rows if r["late_events"] > 0),
               "emitted_wall_ms": int(time.time() * 1000)}


class ProcessingTimeLoad(ProcessWindowFunction):

    def process(self, key, context: "ProcessWindowFunction.Context", elements):
        rows = list(elements)
        w = context.window()
        yield {"kind": "proc_time_load", "feeder_id": key,
               "wall_start_ms": w.start, "wall_end_ms": w.end,
               "load_kw": round(sum(float(r.get("power_kw") or 0) for r in rows), 4),
               "readings": len(rows),
               "min_event_time": min(r["event_time"] for r in rows),
               "max_event_time": max(r["event_time"] for r in rows),
               "emitted_wall_ms": int(time.time() * 1000)}
