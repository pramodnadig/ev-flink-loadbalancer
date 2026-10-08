"""
kafka_producer.py - deliver a generated event stream into Kafka in (accelerated) real time.

The event generator decides WHAT arrives and in WHICH ORDER (events.jsonl is sorted by
arrival_time). This script decides WHEN: it sends each event once the replay clock
reaches its arrival_time, `speedup` times faster than reality. Flink therefore sees a
smooth, continuous stream - just like data from real chargers - so watermarks advance
in small, regular steps.

Kafka basics used here
----------------------
  topic     a named queue of messages; one topic per stream:  ev-<name>
  producer  this script (writes messages)
  consumer  the Flink job (reads messages)
  offset    position of a message in the topic (0, 1, 2, ...). Flink stores the offset
            in every checkpoint, so after a failure it resumes at exactly the right message.

The topic has ONE partition, so Kafka keeps the exact arrival order the generator
decided. Each record's Kafka TIMESTAMP is set to the event's event_time, so Flink can
take event time straight from the record (source-level watermarks). Because these are
2020 timestamps, the topic is created with time-based retention switched off -
otherwise Kafka's 7-day retention would treat the records as expired.

Usage
-----
  python generator/kafka_producer.py --name baseline --create-topic     # (re)create empty topic
  python generator/kafka_producer.py --name baseline --send --speedup 900
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def topic_for(name: str) -> str:
    return f"ev-{name}"


def recreate_topic(topic: str) -> None:
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="ev-admin")
    try:
        admin.delete_topics([topic])
        print(f"[kafka] deleted old topic {topic}")
        time.sleep(2)
    except UnknownTopicOrPartitionError:
        pass
    for attempt in range(20):
        try:
            admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1,
                                          topic_configs={"retention.ms": "-1",
                                                         "message.timestamp.type": "CreateTime"})])
            print(f"[kafka] created topic {topic} (1 partition)")
            return
        except TopicAlreadyExistsError:          # deletion still in progress
            time.sleep(1)
    sys.exit(f"[kafka] could not recreate topic {topic}")


def send(name: str, speedup: float, report_every: int = 20_000) -> None:
    from kafka import KafkaProducer

    stream_dir = config.path("data", "stream", name)
    path = os.path.join(stream_dir, "events.jsonl")
    if not os.path.exists(path):
        sys.exit(f"No events file: {path}\nRun the generator first:  "
                 f"python generator/event_generator.py --name {name}")
    with open(path) as f:
        lines = f.readlines()
    topic = topic_for(name)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, linger_ms=2, acks=1)

    first = json.loads(lines[0])
    t0 = first["arrival_time"]
    wall0 = time.time()
    print(f"[kafka] sending {len(lines):,} events to {topic} at {speedup:.0f}x "
          f"(~{(json.loads(lines[-1])['arrival_time'] - t0) / 1000 / speedup:.0f} s)")
    for i, line in enumerate(lines, 1):
        e = json.loads(line)
        due = wall0 + (e["arrival_time"] - t0) / 1000.0 / speedup
        delay = due - time.time()
        if delay > 0:
            time.sleep(delay)
        producer.send(topic, value=line.rstrip("\n").encode(),
                      key=(e.get("station_id") or "ALL").encode(),
                      timestamp_ms=int(e["event_time"]))
        if i % report_every == 0:
            print(f"[kafka] {i:>8,}/{len(lines):,} sent | behind schedule "
                  f"{max(0.0, time.time() - due):.2f}s")
    producer.flush()
    wall1 = time.time()
    info = {"topic": topic, "events": len(lines), "speedup": speedup,
            "first_arrival_ms": t0, "wall_start_s": wall0, "wall_end_s": wall1,
            "_how_to_use": "send wall time of an event = wall_start_s + (arrival_time - first_arrival_ms)/1000/speedup"}
    with open(os.path.join(stream_dir, ".kafka_send.json"), "w") as f:
        json.dump(info, f, indent=2)
    print(f"[kafka] done: {len(lines):,} events in {wall1 - wall0:.1f} s")


def main() -> None:
    ap = argparse.ArgumentParser(description="Send a generated stream into Kafka")
    ap.add_argument("--name", required=True, help="stream name under data/stream/")
    ap.add_argument("--create-topic", action="store_true", help="delete + recreate the topic")
    ap.add_argument("--send", action="store_true", help="send the events")
    ap.add_argument("--speedup", type=float, default=config.load()["generator"]["speedup"])
    a = ap.parse_args()
    if not (a.create_topic or a.send):
        ap.error("choose --create-topic and/or --send")
    if a.create_topic:
        recreate_topic(topic_for(a.name))
    if a.send:
        send(a.name, a.speedup)


if __name__ == "__main__":
    main()
