"""
chaos.py - controlled failure injection for the fault-tolerance experiment (Part D).

FailOnce throws an exception when it has processed `after_events` events, but only the
FIRST time: before failing it writes a marker file. After Flink restarts the job from the
last checkpoint, the marker exists and processing simply continues. Comparing the outputs
of a run with this failure against a run without it shows whether state (active
sessions, window contents, Kafka offsets) was recovered correctly: with exactly-once
checkpointing the results must be identical.
"""
from __future__ import annotations

import os
import time

from pyflink.datastream.functions import MapFunction, RuntimeContext


class FailOnce(MapFunction):

    def __init__(self, after_events: int, marker_path: str):
        self.after_events = after_events
        self.marker_path = marker_path

    def open(self, runtime_context: RuntimeContext):
        self.seen = 0

    def map(self, value):
        self.seen += 1
        if self.seen >= self.after_events and not os.path.exists(self.marker_path):
            with open(self.marker_path, "w") as f:
                f.write(f"failure injected at wall time {time.time():.3f}, "
                        f"after {self.seen} events since the last (re)start, "
                        f"event_time={value.get('event_time')}\n")
            raise RuntimeError(f"CHAOS: injected failure after {self.seen} events "
                               f"(event_time={value.get('event_time')})")
        return value
