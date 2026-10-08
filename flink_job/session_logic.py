"""
session_logic.py - pure-Python business logic for active charging sessions.

Kept free of any Flink API so it can be unit-tested on its own; the Flink operator in
station_sessions.py only stores these dictionaries in keyed state and calls these
functions.

A session is a plain dict:
    session_id, feeder_id, station_id, site, port_id
    plug_in_ts   : event time of PLUG_IN   (None until the PLUG_IN event has arrived)
    unplug_ts    : event time of UNPLUG    (None until the UNPLUG event has arrived)
    readings     : {event_time: kW}        (POWER readings, any arrival order)
    reported_kwh : energy reported by the charger (cumulative, from POWER/UNPLUG)
    first_seen_ts, last_event_ts, events, late_events

Operational rule for the power a session draws at minute m (the "gating" rule):
    * 0 kW unless the PLUG_IN is known and m >= plug_in_ts
    * 0 kW once the UNPLUG is known and m >= unplug_ts
    * otherwise the most recent POWER reading at or before m (sample-and-hold)

The rule is what a real operator system must do: it only trusts sessions it knows
have started, and it assumes a car keeps drawing power until it is told otherwise.
That is precisely why LATE plug/unplug events change the feeder load:
    late PLUG_IN  -> the car's power is missing  -> load UNDER-estimated (missed alert)
    late UNPLUG   -> a departed car still counts -> load OVER-estimated  (false alert)
"""
from __future__ import annotations

from bisect import bisect_right

MINUTE_MS = 60_000


def floor_minute(ts: int) -> int:
    return ts - ts % MINUTE_MS


def ceil_minute(ts: int) -> int:
    return -(-ts // MINUTE_MS) * MINUTE_MS


def new_session(e: dict) -> dict:
    return {"session_id": e.get("session_id"), "feeder_id": e.get("feeder_id"),
            "station_id": e.get("station_id"), "site": e.get("site"),
            "port_id": e.get("port_id"), "plug_in_ts": None, "unplug_ts": None,
            "readings": {}, "reported_kwh": 0.0, "first_seen_ts": e["event_time"],
            "last_event_ts": e["event_time"], "events": 0, "late_events": 0,
            "plug_in_was_late": False, "unplug_was_late": False}


def apply_event(s: dict, e: dict, is_late: bool) -> None:
    """Update a session with one PLUG_IN / POWER / UNPLUG event (any arrival order)."""
    ts = e["event_time"]
    s["events"] += 1
    s["first_seen_ts"] = min(s["first_seen_ts"], ts)
    s["last_event_ts"] = max(s["last_event_ts"], ts)
    if is_late:
        s["late_events"] += 1
    t = e["event_type"]
    if t == "PLUG_IN":
        s["plug_in_ts"] = ts
        s["plug_in_was_late"] = is_late
    elif t == "POWER":
        s["readings"][ts] = float(e.get("power_kw") or 0.0)
        s["reported_kwh"] = max(s["reported_kwh"], float(e.get("energy_kwh") or 0.0))
    elif t == "UNPLUG":
        s["unplug_ts"] = ts
        s["unplug_was_late"] = is_late
        s["reported_kwh"] = max(s["reported_kwh"], float(e.get("energy_kwh") or 0.0))


def kw_at(s: dict, m: int) -> float:
    """Power the operator attributes to this session at minute m (gating rule above)."""
    if s["plug_in_ts"] is None or m < s["plug_in_ts"]:
        return 0.0
    if s["unplug_ts"] is not None and m >= s["unplug_ts"]:
        return 0.0
    times = sorted(s["readings"])
    i = bisect_right(times, m)
    return s["readings"][times[i - 1]] if i else 0.0


def station_kw_at(sessions, m: int) -> float:
    return round(sum(kw_at(s, m) for s in sessions), 4)


def is_finished(s: dict) -> bool:
    return s["plug_in_ts"] is not None and s["unplug_ts"] is not None


def price_at(tariffs: dict, ts: int):
    """Price in force at ts: the tariff with the latest effective time <= ts."""
    if not tariffs:
        return None, None
    times = sorted(tariffs)
    i = bisect_right(times, ts)
    if i == 0:
        return None, None
    return tariffs[times[i - 1]]          # (price, period name)


def summarise(s: dict, tariffs: dict, status: str) -> dict:
    """Session summary with energy split by tariff period and cost."""
    times = sorted(s["readings"])
    end_ts = s["unplug_ts"] if s["unplug_ts"] is not None else s["last_event_ts"]
    energy = 0.0
    cost = 0.0
    unpriced = 0.0
    by_period: dict = {}
    for i, t in enumerate(times):
        nxt = times[i + 1] if i + 1 < len(times) else end_ts
        seg_kwh = s["readings"][t] * max(0, nxt - t) / 3_600_000
        if seg_kwh <= 0:
            continue
        energy += seg_kwh
        price, period = price_at(tariffs, t)
        if price is None:
            unpriced += seg_kwh
        else:
            cost += seg_kwh * price
            by_period[period] = by_period.get(period, 0.0) + seg_kwh
    start = s["plug_in_ts"] if s["plug_in_ts"] is not None else s["first_seen_ts"]
    return {
        "session_id": s["session_id"], "station_id": s["station_id"], "site": s["site"],
        "feeder_id": s["feeder_id"], "port_id": s["port_id"], "status": status,
        "plug_in_ts": s["plug_in_ts"], "unplug_ts": s["unplug_ts"],
        "duration_min": round((end_ts - start) / 60_000, 1),
        "energy_kwh_measured": round(energy, 4),
        "energy_kwh_reported": round(s["reported_kwh"], 4),
        "energy_kwh_by_period": {k: round(v, 4) for k, v in by_period.items()},
        "unpriced_kwh": round(unpriced, 4),
        "cost_usd": round(cost, 4),
        "events": s["events"], "late_events": s["late_events"],
        "plug_in_was_late": s["plug_in_was_late"], "unplug_was_late": s["unplug_was_late"],
    }
