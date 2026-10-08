"""
power_model.py - the ONE definition of "how much power a session draws, and when".

Both the event generator (which produces the stream Flink reads) and the ground
truth (which we compare Flink's answers against) import this module. Because they
share one definition, any difference between Flink's output and the ground truth
can only come from *streaming effects* (lateness, out-of-order data, watermarks),
never from two pieces of code disagreeing about the physics.

Model (stated as an assumption in the report)
---------------------------------------------
* The dataset gives, per session: plug-in time, unplug time, energy (kWh) and
  how long the car was actually charging.
* A car starts charging when it plugs in and draws a constant power
      P = energy_kwh / charging_hours
  until   charge_end = plug_in + charging_time.   After that it sits idle
  (still plugged in, 0 kW) until it unplugs.
* The charger reports its power at every sample boundary (default every 60 s)
  that falls inside [plug_in, charge_end), and reports 0 kW once at charge_end if
  the car then stays plugged in idle (across a sample boundary). A car unplugged
  while still charging sends no 0 kW reading: its UNPLUG ends the load.
* Feeder load at time t = sum of P over sessions on that feeder with
      plug_in <= t < charge_end,   evaluated at sample boundaries.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def first_sample(start_ms: int, interval_ms: int) -> int:
    """First sample boundary at or after start (ceil to the interval grid)."""
    return -(-start_ms // interval_ms) * interval_ms


def sample_times(start_ms: int, charge_end_ms: int, interval_ms: int):
    """Sample boundaries t with start <= t < charge_end."""
    t = first_sample(start_ms, interval_ms)
    while t < charge_end_ms:
        yield t
        t += interval_ms


def load_profile(sessions: pd.DataFrame, window_start_ms: int, window_end_ms: int,
                 interval_ms: int, group_col: str = "feeder_id") -> pd.DataFrame:
    """
    Exact load (kW) per group at every sample boundary in [window_start, window_end).

    Returns a DataFrame indexed by sample time (epoch ms), one column per group.
    Uses a difference array, so it is fast even for many sessions.
    """
    w0 = first_sample(window_start_ms, interval_ms)
    n = int((window_end_ms - w0) // interval_ms)
    groups = sorted(sessions[group_col].unique())
    out = {}
    for g in groups:
        s = sessions[sessions[group_col] == g]
        diff = np.zeros(n + 1)
        i0 = np.ceil((s["start_ms"].to_numpy() - w0) / interval_ms).astype(np.int64)
        i1 = np.ceil((s["charge_end_ms"].to_numpy() - w0) / interval_ms).astype(np.int64)
        kw = s["power_kw"].to_numpy()
        i0 = np.clip(i0, 0, n)
        i1 = np.clip(i1, 0, n)
        keep = i1 > i0
        np.add.at(diff, i0[keep], kw[keep])
        np.add.at(diff, i1[keep], -kw[keep])
        out[g] = np.cumsum(diff)[:n]
    idx = w0 + np.arange(n, dtype=np.int64) * interval_ms
    return pd.DataFrame(out, index=pd.Index(idx, name="t_ms"))
