"""
events.py - parsing and validating raw events coming from Kafka.

Each Kafka message is one JSON object produced by generator/event_generator.py.
A malformed message must never crash the pipeline, so parse_event returns None for
anything it cannot use and the job filters those out (and counts them).
"""
from __future__ import annotations

import json

EVENT_TYPES = ("PLUG_IN", "POWER", "UNPLUG", "TARIFF")
REQUIRED = ("event_type", "event_time", "feeder_id")


def parse_event(line: str):
    """JSON text -> dict, or None if the message is unusable."""
    try:
        e = json.loads(line)
    except (TypeError, ValueError):
        return None
    if not isinstance(e, dict) or any(k not in e for k in REQUIRED):
        return None
    if e["event_type"] not in EVENT_TYPES:
        return None
    try:
        e["event_time"] = int(e["event_time"])
    except (TypeError, ValueError):
        return None
    return e
