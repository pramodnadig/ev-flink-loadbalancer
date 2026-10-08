"""
alerts.py - turn feeder load results into operator alerts (Part D).

Input (keyed by feeder): every firing of the 1-minute load windows ("feeder_minute")
and of the 15-minute sustained windows ("sustained").

Alert types
-----------
  SPIKE                 1-minute load >= spike threshold (default 100% of capacity)
                        - raised once at the START of an overload episode, not every minute
  SUSTAINED             15-minute average >= sustained threshold (default 80%)
                        - raised once at the start of a sustained episode
  SPIKE_LATE / SUSTAINED_LATE
                        a REVISION caused by late data pushed the value over the threshold:
                        an overload that the first (on-time) result missed
  SPIKE_RETRACTED / SUSTAINED_RETRACTED
                        a revision pulled the value back under the threshold: an alert that
                        was raised on incomplete data was a FALSE alarm

Each alert carries the event time it refers to and the wall-clock time it was produced,
so the analysis can measure alert latency and accuracy.
"""
from __future__ import annotations

import time

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

MINUTE_MS = 60_000


class AlertManager(KeyedProcessFunction):

    def __init__(self, spike_pct: float, sustained_pct: float, horizon_min: float):
        self.spike_pct = spike_pct
        self.sustained_pct = sustained_pct
        self.horizon_ms = int(horizon_min * 60_000)

    def open(self, runtime_context: RuntimeContext):
        # one ValueState per feeder: {"pct": {"SPIKE:<t>": pct, ...}, "last": {"SPIKE": t, "SUSTAINED": t}}
        self.state = runtime_context.get_state(
            ValueStateDescriptor("alert_state", Types.PICKLED_BYTE_ARRAY()))
        mg = runtime_context.get_metrics_group().add_group("ev")
        self.m_alerts = mg.counter("alerts_raised")
        self.m_retracted = mg.counter("alerts_retracted")
        self.m_late_detected = mg.counter("alerts_late_detected")

    def _alert(self, kind, r, t, extra=None):
        a = {"alert_type": kind, "feeder_id": r["feeder_id"], "event_time": t,
             "event_time_local": r.get("minute_local") or r.get("window_end_local"),
             "pct": r["pct"], "load_kw": r.get("load_kw", r.get("avg_kw")),
             "capacity_kw": r["capacity_kw"], "firing": r["firing"],
             "watermark": r["watermark"], "emitted_wall_ms": int(time.time() * 1000)}
        if extra:
            a.update(extra)
        return a

    def process_element(self, r, ctx: "KeyedProcessFunction.Context"):
        if r["kind"] == "feeder_minute":
            t, thr, prefix = r["minute"], self.spike_pct, "SPIKE"
        else:
            t, thr, prefix = r["window_end"], self.sustained_pct, "SUSTAINED"
        st = self.state.value() or {"pct": {}, "last": {}}
        key = f"{prefix}:{t}"
        prev = st["pct"].get(key)
        st["pct"][key] = float(r["pct"])
        over = r["pct"] >= thr

        if prev is None and r.get("revision"):             # window first created by late data
            if over:
                self.m_late_detected.inc()
                yield self._alert(prefix + "_LATE", r, t, {"previous_pct": None})
        elif prev is None:                                 # first result for this minute/window
            if over:
                last = st["last"].get(prefix)
                if last is None or t - last > MINUTE_MS:   # a new episode starts
                    self.m_alerts.inc()
                    yield self._alert(prefix, r, t)
                st["last"][prefix] = t
        else:                                              # revision caused by late data
            was_over = prev >= thr
            if over and not was_over:
                self.m_late_detected.inc()
                yield self._alert(prefix + "_LATE", r, t, {"previous_pct": prev})
            elif was_over and not over:
                self.m_retracted.inc()
                yield self._alert(prefix + "_RETRACTED", r, t, {"previous_pct": prev})

        # bound state: forget minutes/windows older than the correction horizon
        if r["firing"] == 1 and len(st["pct"]) > 200:
            st["pct"] = {k: v for k, v in st["pct"].items() if int(k.split(":")[1]) >= t - self.horizon_ms}
        self.state.update(st)
