"""
feeder_load.py - event-time windows over feeder load (Part A rolling load, Part B
sustained excursions, rubric II).

1) FeederMinuteLoad  - TumblingEventTimeWindows(1 min), keyed by feeder
       load(m) = sum of station deltas for minute m          (event-time rolling load)
   Allowed lateness L: if a correction for minute m arrives after the window fired but
   within L, the window FIRES AGAIN with the corrected total (firing = 2, 3, ...).
   Corrections later than L go to a side output ("dropped late") and are counted.

2) SustainedLoad     - SlidingEventTimeWindows(15 min, slide 1 min), keyed by feeder
       average load over the last 15 minutes (minutes without load count as 0 kW)
   A "sustained excursion" is a 15-minute average >= 80% of feeder capacity: a feeder can
   absorb a short spike, but a long high load overheats cables and transformers.
   The input contains every firing of the minute windows; for each minute the LAST
   value received is used, so corrected minutes replace their earlier value.

Both functions count their own firings per window in per-window state, so every output
row says whether it is the first result (on time) or a revision caused by late data.
"""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyflink.common import Types
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor

FIRED = ValueStateDescriptor("firings", Types.INT())


def _firing(context, window_end_ms: int) -> int:
    """How many times this window has fired (1 = first, on-time result)."""
    try:
        st = context.window_state().get_state(FIRED)
        n = (st.value() or 0) + 1
        st.update(n)
        return n
    except Exception:                      # fallback if per-window state is unavailable
        return 1


def _clear(context) -> None:
    try:
        context.window_state().get_state(FIRED).clear()
    except Exception:
        pass


class FeederMinuteLoad(ProcessWindowFunction):

    def __init__(self, capacity_kw: dict, local_tz: str):
        self.capacity_kw = capacity_kw
        self.local_tz = local_tz

    def open(self, runtime_context):
        self.tz = ZoneInfo(self.local_tz)

    def process(self, key, context: "ProcessWindowFunction.Context", elements):
        w = context.window()
        rows = list(elements)
        load = round(sum(float(e["kw_delta"]) for e in rows), 4)
        cap = float(self.capacity_kw.get(key, 0.0)) or float("nan")
        n = _firing(context, w.end)
        late_window = n == 1 and all(e.get("revision") for e in rows)   # created only by corrections
        yield {"kind": "feeder_minute", "feeder_id": key, "minute": w.start,
               "minute_local": datetime.fromtimestamp(w.start / 1000, self.tz).strftime("%Y-%m-%d %H:%M"),
               "load_kw": load, "capacity_kw": cap, "pct": round(100 * load / cap, 2),
               "firing": n, "revision": n > 1 or late_window, "watermark": context.current_watermark(),
               "emitted_wall_ms": int(time.time() * 1000)}

    def clear(self, context):
        _clear(context)


class SustainedLoad(ProcessWindowFunction):

    def __init__(self, capacity_kw: dict, window_min: int, local_tz: str):
        self.capacity_kw = capacity_kw
        self.window_min = window_min
        self.local_tz = local_tz

    def open(self, runtime_context):
        self.tz = ZoneInfo(self.local_tz)

    def process(self, key, context: "ProcessWindowFunction.Context", elements):
        w = context.window()
        rows = list(elements)
        latest = {}
        for e in rows:                             # arrival order: later firings overwrite
            latest[e["minute"]] = float(e["load_kw"])
        cap = float(self.capacity_kw.get(key, 0.0)) or float("nan")
        avg = sum(latest.values()) / self.window_min
        n = _firing(context, w.end)
        late_window = n == 1 and all(e.get("revision") for e in rows)
        yield {"kind": "sustained", "feeder_id": key, "window_start": w.start, "window_end": w.end,
               "window_end_local": datetime.fromtimestamp(w.end / 1000, self.tz).strftime("%Y-%m-%d %H:%M"),
               "avg_kw": round(avg, 4), "max_kw": round(max(latest.values(), default=0.0), 4),
               "minutes_with_load": len(latest),
               "minutes_over_capacity": sum(1 for v in latest.values() if v >= cap),
               "capacity_kw": cap, "pct": round(100 * avg / cap, 2),
               "firing": n, "revision": n > 1 or late_window, "watermark": context.current_watermark(),
               "emitted_wall_ms": int(time.time() * 1000)}

    def clear(self, context):
        _clear(context)
