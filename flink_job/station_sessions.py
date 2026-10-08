"""
station_sessions.py - keyed state for active charging sessions (Part B) + tariff
broadcast state (operator state) + event-time timers.

Operator type: KeyedBroadcastProcessFunction
  * keyed input     : PLUG_IN / POWER / UNPLUG events, keyed by station_id
  * broadcast input : TARIFF events, copied to every parallel instance

State
-----
  station   ValueState (one per station = KEYED state) holding:
              sessions    {session_id -> session dict}   the ACTIVE charging sessions
              emitted     {minute -> kW already reported}
              corrections {minutes to re-check because late data arrived}
              next_tick   the next per-minute timer already registered
  tariffs   broadcast MapState  effective_from -> (price, period)   BROADCAST (operator) state

(Keeping a station's state in ONE value object is a deliberate performance choice for
PyFlink: every state access is a call between the Python worker and the Java runtime,
so one read + one write per event is far cheaper than many small map reads/writes.)

Event-time timers (one per station per minute while it has sessions)
---------------------------------------------------------------------
At minute m (when the watermark passes m) the station reports its total power:
      kW(m) = sum over its sessions of session_logic.kw_at(session, m)
as a DELTA against what it reported for m before. Downstream feeder windows add the
deltas up. Using deltas means a late event can CORRECT an already-reported minute:
a late UNPLUG produces a negative delta, a late PLUG_IN a positive one.

Why corrections are emitted from timers: an output record inherits the timestamp of
whatever triggered it. To give a correction the timestamp of the minute it corrects,
the operator registers an event-time timer AT that (past) minute; it fires at the next
watermark and the record carries the right timestamp, so downstream windows treat it
as late data for that minute (allowed lateness -> window re-fires with corrected load).

State lifecycle / cleanup
-------------------------
  * a finished session (PLUG_IN and UNPLUG known) is summarised and removed once the
    watermark is `horizon` past its unplug time (late events may still arrive until then)
  * a session with no events for `stale_hours` is summarised as STALE and removed
    (e.g. a lost UNPLUG) - this bounds state size
  * reported minutes older than `horizon` are forgotten
  * events older than (watermark - horizon) are too late to correct anything: counted
    and dropped
"""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyflink.common import Types
from pyflink.datastream.functions import KeyedBroadcastProcessFunction, RuntimeContext
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from flink_job import session_logic as sl

NO_WATERMARK = -(2 ** 63)
TARIFF_STATE = MapStateDescriptor("tariffs", Types.LONG(), Types.PICKLED_BYTE_ARRAY())


def _empty() -> dict:
    return {"sessions": {}, "emitted": {}, "corrections": set(), "next_tick": None}


class StationSessions(KeyedBroadcastProcessFunction):

    def __init__(self, horizon_min: float, stale_hours: float, local_tz: str):
        self.horizon_ms = int(horizon_min * 60_000)
        self.stale_ms = int(stale_hours * 3_600_000)
        self.local_tz = local_tz

    # ------------------------------------------------------------------ setup
    def open(self, runtime_context: RuntimeContext):
        self.tz = ZoneInfo(self.local_tz)
        self.state = runtime_context.get_state(
            ValueStateDescriptor("station", Types.PICKLED_BYTE_ARRAY()))
        self.tariff_cache = {}
        self.active_local = 0
        mg = runtime_context.get_metrics_group().add_group("ev")
        self.m_events = mg.counter("events_in")
        self.m_late = mg.counter("late_events")
        self.m_too_late = mg.counter("events_beyond_horizon")
        self.m_opened = mg.counter("sessions_opened")
        self.m_closed = mg.counter("sessions_closed")
        self.m_stale = mg.counter("sessions_stale")
        self.m_corrections = mg.counter("load_corrections")
        mg.gauge("active_sessions", lambda: int(self.active_local))

    # ------------------------------------------------------------------ helpers
    def _tariffs(self, ctx) -> dict:
        try:
            d = {int(k): v for k, v in ctx.get_broadcast_state(TARIFF_STATE).items()}
            if d:
                self.tariff_cache = d
                return d
        except Exception:
            pass
        return self.tariff_cache

    def _local(self, ts: int) -> str:
        return datetime.fromtimestamp(ts / 1000, self.tz).strftime("%Y-%m-%d %H:%M")

    def _schedule(self, st: dict, ts_service, t: int) -> None:
        """Register the per-minute tick only if an earlier one is not already pending."""
        if st["next_tick"] is None or t < st["next_tick"]:
            ts_service.register_event_time_timer(t)
            st["next_tick"] = t

    # ------------------------------------------------------------------ inputs
    def process_broadcast_element(self, e, ctx: "KeyedBroadcastProcessFunction.Context"):
        value = (float(e["usd_per_kwh"]), e.get("tariff_period", "?"))
        ctx.get_broadcast_state(TARIFF_STATE).put(int(e["event_time"]), value)
        self.tariff_cache[int(e["event_time"])] = value

    def process_element(self, e, ctx: "KeyedBroadcastProcessFunction.ReadOnlyContext"):
        ts = ctx.timestamp()
        ts_service = ctx.timer_service()
        wm = ts_service.current_watermark()
        has_wm = wm != NO_WATERMARK
        late = has_wm and ts <= wm
        self.m_events.inc()
        if late:
            self.m_late.inc()
        if has_wm and ts < wm - self.horizon_ms:
            self.m_too_late.inc()        # cannot correct results this old any more
            return

        st = self.state.value() or _empty()
        sid = e.get("session_id")
        s = st["sessions"].get(sid)
        if s is None:
            s = sl.new_session(e)
            st["sessions"][sid] = s
            self.m_opened.inc()
            self.active_local += 1
        sl.apply_event(s, e, late)

        if late:
            # minutes already reported that this event may change -> timers in the past
            m, last = sl.ceil_minute(ts), sl.floor_minute(wm)
            while m <= last:
                if m not in st["corrections"]:
                    st["corrections"].add(m)
                    ts_service.register_event_time_timer(m)
                m += sl.MINUTE_MS
            self._schedule(st, ts_service, last + sl.MINUTE_MS)
        else:
            nxt = sl.ceil_minute(ts)
            if has_wm:
                nxt = max(nxt, sl.floor_minute(wm) + sl.MINUTE_MS)
            self._schedule(st, ts_service, nxt)
        self.state.update(st)

    # ------------------------------------------------------------------ timers
    def on_timer(self, m: int, ctx: "KeyedBroadcastProcessFunction.OnTimerContext"):
        if m % sl.MINUTE_MS:
            return
        st = self.state.value()
        if st is None:
            return
        wm = ctx.timer_service().current_watermark()
        sessions = list(st["sessions"].values())
        is_correction = m in st["corrections"]
        st["corrections"].discard(m)
        is_tick = st["next_tick"] == m
        if is_tick:
            st["next_tick"] = None

        # 1) station power at minute m, reported as a delta
        kw = sl.station_kw_at(sessions, m)
        old = st["emitted"].get(m)
        prev = 0.0 if old is None else old
        if abs(kw - prev) > 1e-9 and sessions:
            revision = old is not None or is_correction
            if revision:
                self.m_corrections.inc()
            yield {"kind": "station_load", "station_id": ctx.get_current_key(),
                   "feeder_id": sessions[0]["feeder_id"], "minute": m,
                   "minute_local": self._local(m), "kw": kw, "kw_delta": round(kw - prev, 4),
                   "revision": revision, "watermark": wm,
                   "emitted_wall_ms": int(time.time() * 1000)}
        st["emitted"][m] = kw

        # 2) state lifecycle: summarise and remove finished / stale sessions
        if is_tick:
            tariffs = None
            for s in sessions:
                done = sl.is_finished(s) and s["unplug_ts"] + self.horizon_ms <= m
                stale = (not sl.is_finished(s)) and s["last_event_ts"] + self.stale_ms <= m
                if done or stale:
                    if tariffs is None:
                        tariffs = self._tariffs(ctx)
                    summary = sl.summarise(s, tariffs, "COMPLETE" if done else "STALE")
                    summary.update(kind="session_summary",
                                   plug_in_local=self._local(s["plug_in_ts"]) if s["plug_in_ts"] else None,
                                   closed_at_event_time=m)
                    yield summary
                    del st["sessions"][s["session_id"]]
                    self.active_local = max(0, self.active_local - 1)
                    self.m_closed.inc()
                    if stale:
                        self.m_stale.inc()
            # 3) forget reported minutes older than the horizon
            cutoff = m - self.horizon_ms
            st["emitted"] = {k: v for k, v in st["emitted"].items() if k >= cutoff}
            # 4) keep ticking while the station still has sessions
            if st["sessions"]:
                self._schedule(st, ctx.timer_service(), m + sl.MINUTE_MS)

        if st["sessions"] or st["corrections"] or st["next_tick"] is not None:
            self.state.update(st)
        else:
            self.state.clear()
