"""
stream_probe.py - a diagnostic operator that makes watermarks and lateness VISIBLE.
(Stage 3; its counts are also monitoring evidence for Part D)

Keyed by feeder. For every event it compares the event's timestamp with the current
watermark:
      event_time <= watermark   ->   the event is LATE
(Flink has already promised results up to the watermark, so this event arrived after
its time window was declared complete.)

Every event-time bucket (default 1 hour) is summarised when the watermark passes the
end of the bucket - using an EVENT-TIME TIMER. If a late event later lands in a bucket
that was already reported, the timer fires again and a second "correction" row
(emission = 2, 3, ...) is produced. That is exactly how late data changes results that
were already published.

State used (keyed by feeder), one ValueState holding:
  buckets : bucket_start -> counters         (open buckets)
  emitted : bucket_start -> times reported   (to recognise corrections)
"""
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

NO_WATERMARK = -(2 ** 63)          # Flink's initial watermark (Long.MIN_VALUE)


class WatermarkProbe(KeyedProcessFunction):

    def __init__(self, bucket_minutes: int = 60, local_tz: str = "America/Los_Angeles"):
        self.bucket_ms = int(bucket_minutes * 60_000)
        self.local_tz = local_tz

    def open(self, runtime_context: RuntimeContext):
        self.tz = ZoneInfo(self.local_tz)
        self.state = runtime_context.get_state(
            ValueStateDescriptor("probe", Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, e, ctx: "KeyedProcessFunction.Context"):
        ts = ctx.timestamp()
        wm = ctx.timer_service().current_watermark()
        start = ts - ts % self.bucket_ms
        st = self.state.value() or {"buckets": {}, "emitted": {}}
        new_bucket = start not in st["buckets"]
        b = st["buckets"].get(start) or {"events": 0, "late": 0, "max_late_s": 0.0,
                                         "PLUG_IN": 0, "POWER": 0, "UNPLUG": 0, "TARIFF": 0}
        b["events"] += 1
        b[e["event_type"]] += 1
        if wm != NO_WATERMARK and ts <= wm:
            b["late"] += 1
            b["max_late_s"] = max(b["max_late_s"], (wm - ts) / 1000.0)
        st["buckets"][start] = b
        self.state.update(st)
        if new_bucket:
            ctx.timer_service().register_event_time_timer(start + self.bucket_ms - 1)

    def on_timer(self, timestamp: int, ctx: "KeyedProcessFunction.OnTimerContext"):
        start = timestamp - self.bucket_ms + 1
        st = self.state.value()
        if st is None or start not in st["buckets"]:
            return
        b = st["buckets"].pop(start)
        n = st["emitted"].get(start, 0) + 1
        st["emitted"][start] = n
        st["emitted"] = {k: v for k, v in st["emitted"].items() if k >= start - 48 * 3_600_000}
        self.state.update(st)
        yield {
            "feeder_id": ctx.get_current_key(),
            "bucket_start": start,
            "bucket_start_local": datetime.fromtimestamp(start / 1000, self.tz).strftime("%Y-%m-%d %H:%M"),
            "emission": n,                      # 1 = on time, >1 = correction caused by late data
            **b,
            "watermark": ctx.timer_service().current_watermark(),
            "emitted_wall_ms": int(time.time() * 1000),
        }
