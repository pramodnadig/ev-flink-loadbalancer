"""
time_semantics.py - event time, timestamps and the watermark strategy (Part B / rubric II).

Event time vs processing time
-----------------------------
* event_time       : when the charger measured / did something. Feeder load is a
                     physical quantity at a moment in time, so the business logic uses
                     THIS clock.
* processing time  : the wall clock of the machine running Flink. It depends on network
                     delay and replay speed, so a load computed on it would change every
                     time the stream is replayed - not acceptable for overload decisions.

Watermark
---------
A watermark W(t) is Flink's declaration "I do not expect any more events with
event_time <= t". Event-time windows and timers fire only when the watermark passes
them, so the watermark decides WHEN results are produced.

BoundedOutOfOrderness(D):   W = (largest event_time seen so far) - D - 1 ms
  * small D  -> results come out quickly, but events delayed by more than D are LATE
  * large D  -> fewer late events (more complete), but every result is D later
  -> this latency / completeness trade-off is measured in the experiments.

Idleness: if an input stops sending (e.g. a quiet partition at night) it is marked idle
after `idleness_s`, so it does not hold back the watermark of the whole job.

Where timestamps come from
--------------------------
The producer writes each event's event_time into the Kafka record's timestamp. The
strategy is therefore applied directly at the KafkaSource ("source-level watermarks"):
Flink's Java runtime reads the record timestamp and generates watermarks per Kafka
partition, with no Python code in the time path. (EventTimeAssigner below is the
alternative when timestamps can only be taken from the JSON payload.)
"""
from __future__ import annotations

from pyflink.common import Duration, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner


class EventTimeAssigner(TimestampAssigner):
    """Tell Flink which field holds the event time (epoch milliseconds, UTC)."""

    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value["event_time"])


def build_watermark_strategy(out_of_orderness_s: float, idleness_s: float = 0,
                             use_record_timestamp: bool = True):
    if out_of_orderness_s <= 0:
        strategy = WatermarkStrategy.for_monotonous_timestamps()
    else:
        strategy = WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_millis(int(out_of_orderness_s * 1000)))
    if not use_record_timestamp:
        strategy = strategy.with_timestamp_assigner(EventTimeAssigner())
    if idleness_s and idleness_s > 0:
        strategy = strategy.with_idleness(Duration.of_millis(int(idleness_s * 1000)))
    return strategy
