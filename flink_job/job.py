"""
job.py - EV Charging Load Balancer: the Apache Flink streaming job (entry point).

Dataflow
--------
  Kafka topic ev-<stream>
     | KafkaSource (exactly-once: offsets are part of every checkpoint)
     | + event-time timestamps & watermarks generated AT THE SOURCE from the Kafka
     |   record timestamp (= event_time) - BoundedOutOfOrderness D, optional idleness
     v
  parse JSON -> drop malformed  [-> optional FailOnce for the recovery experiment]
     |
     +--> key by feeder ----------------> WatermarkProbe (lateness per feeder-hour)  -> probe
     |
     +--> TARIFF events --broadcast--+
     |                               v
     +--> PLUG_IN/POWER/UNPLUG --key by station--> StationSessions
                                     (keyed MapState of ACTIVE SESSIONS + broadcast tariffs
                                      + event-time timers: per-minute station power deltas)
                                     |                         |
                          station_load deltas            session summaries -> sessions
                                     v                         v
               key by feeder: Tumbling 1-min event-time   key by station: Tumbling 1-day
               window, allowed lateness L                 window -> station_daily
                     -> feeder_load (rolling load)
                     |   (late beyond L -> side output -> dropped_late)
                     v
               key by feeder: Sliding 15-min / 1-min window -> sustained
                     |
                     +--> AlertManager (keyed state, episodes, retractions) -> alerts

  Optional (--proc-time-branch): POWER readings in PROCESSING-time windows -> proc_time_load

Run locally (embedded mini-cluster):
  python flink_job/job.py --stream s3 --run demo
Run on the standalone cluster (Web UI, metrics, checkpoints at http://localhost:8081):
  bash scripts/submit_to_cluster.sh --stream s3 --run demo
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyflink.common import Configuration, Types                                  # noqa: E402
from pyflink.common.serialization import SimpleStringSchema                      # noqa: E402
from pyflink.common.time import Time                                             # noqa: E402
from pyflink.datastream import (CheckpointingMode, OutputTag,                     # noqa: E402
                                RuntimeExecutionMode, StreamExecutionEnvironment)
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer,         # noqa: E402
                                                 KafkaSource, KafkaTopicPartition)
from pyflink.datastream.window import (SlidingEventTimeWindows,                   # noqa: E402
                                       TumblingEventTimeWindows,
                                       TumblingProcessingTimeWindows)

from common import config                                                        # noqa: E402
from flink_job.alerts import AlertManager                                        # noqa: E402
from flink_job.chaos import FailOnce                                             # noqa: E402
from flink_job.events import parse_event                                         # noqa: E402
from flink_job.feeder_load import FeederMinuteLoad, SustainedLoad                # noqa: E402
from flink_job.sinks import json_file_sink                                       # noqa: E402
from flink_job.station_sessions import TARIFF_STATE, StationSessions             # noqa: E402
from flink_job.stream_probe import WatermarkProbe                                # noqa: E402
from flink_job.summaries import ProcessingTimeLoad, StationDailySummary          # noqa: E402
from flink_job.time_semantics import build_watermark_strategy                    # noqa: E402

CFG = config.load()
P = CFG["pipeline"]


# --------------------------------------------------------------------------- arguments
def parse_args():
    ap = argparse.ArgumentParser(description="EV Charging Load Balancer - Flink job")
    ap.add_argument("--stream", required=True, help="stream name (Kafka topic ev-<stream>)")
    ap.add_argument("--run", default=None, help="output folder under results/outputs/ (default: stream)")
    ap.add_argument("--watermark-s", type=float, default=P["watermark_s"],
                    help="bounded out-of-orderness D (s)")
    ap.add_argument("--idleness-s", type=float, default=P["idleness_s"], help="source idleness (0 = off)")
    ap.add_argument("--allowed-lateness-min", type=float, default=P["allowed_lateness_min"],
                    help="window allowed lateness L (min)")
    ap.add_argument("--horizon-min", type=float, default=P["correction_horizon_min"],
                    help="how far back late events may correct results (min)")
    ap.add_argument("--parallelism", type=int, default=1)
    ap.add_argument("--timestamps", choices=["source", "payload"], default="source",
                    help="source: Kafka record timestamp (default); payload: event_time field via Python")
    ap.add_argument("--watermark-interval-ms", type=int, default=10,
                    help="how often (wall clock) the source emits a new watermark")
    ap.add_argument("--bundle-size", type=int, default=1000, help="Python bundle size")
    ap.add_argument("--bundle-time-ms", type=int, default=50, help="Python bundle time")
    ap.add_argument("--checkpoint-s", type=float, default=P["checkpoint_interval_s"])
    ap.add_argument("--state-backend", choices=["hashmap", "rocksdb"], default=P["state_backend"])
    ap.add_argument("--fail-after", type=int, default=0,
                    help="inject ONE failure after N events (recovery experiment)")
    ap.add_argument("--proc-time-branch", action="store_true",
                    help="also compute load in processing-time windows (experiment E1)")
    ap.add_argument("--speedup", type=float, default=CFG["generator"]["speedup"],
                    help="replay speed of the producer (sizes the processing-time windows)")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--unbounded", action="store_true", help="do not stop after the last event")
    ap.add_argument("--keep-output", action="store_true", help="keep old results of this run")
    return ap.parse_args()


# --------------------------------------------------------------------------- helpers
def feeder_capacities() -> dict:
    """capacity_kw = installed charger capacity x diversity factor (config)."""
    path = config.path("data", "processed", "feeders.csv")
    if not os.path.exists(path):
        sys.exit(f"{path} not found. Run: python scripts/prepare_sessions.py")
    df = CFG["capacity"]["diversity_factor"]
    with open(path) as f:
        return {r["feeder_id"]: round(float(r["installed_kw"]) * df, 2) for r in csv.DictReader(f)}


def package_project_code() -> str:
    """Zip flink_job/ and common/ so Flink's Python workers can import them."""
    import zipfile
    build_dir = config.path("build")
    os.makedirs(build_dir, exist_ok=True)
    zpath = os.path.join(build_dir, "ev_job_code.zip")
    with zipfile.ZipFile(zpath, "w") as z:
        for pkg in ("flink_job", "common"):
            for root, _, files in os.walk(config.path(pkg)):
                for fn in files:
                    if fn.endswith(".py"):
                        full = os.path.join(root, fn)
                        z.write(full, os.path.relpath(full, config.PROJECT_HOME))
    return zpath


def build_env(a, run: str) -> StreamExecutionEnvironment:
    conf = Configuration()
    conf.set_string("pipeline.name", f"ev-load-balancer [{run}]")
    # watermarks are emitted on a wall-clock timer: short interval = closely tracking watermark
    conf.set_string("pipeline.auto-watermark-interval", f"{a.watermark_interval_ms} ms")
    # Python operators process records in bundles. A watermark always flushes the current
    # bundle first, so bundling changes throughput/latency but never event-time results.
    conf.set_string("python.fn-execution.bundle.size", str(a.bundle_size))
    conf.set_string("python.fn-execution.bundle.time", str(a.bundle_time_ms))
    # ---- fault tolerance -------------------------------------------------------------
    conf.set_string("restart-strategy.type", "fixed-delay")
    conf.set_string("restart-strategy.fixed-delay.attempts", str(P["restart_attempts"]))
    conf.set_string("restart-strategy.fixed-delay.delay", f"{P['restart_delay_s']} s")
    conf.set_string("state.backend.type", a.state_backend)
    if a.state_backend == "rocksdb":
        conf.set_string("state.backend.incremental", "true")
    conf.set_string("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION")
    conf.set_string("state.checkpoints.num-retained", "3")
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(a.parallelism)

    jar = os.environ.get("KAFKA_CONNECTOR_JAR")
    if not jar or not os.path.exists(jar):
        sys.exit("Kafka connector jar not found. Run: bash scripts/setup_kafka.sh (and source env.sh)")
    env.add_jars("file://" + jar)
    env.add_python_file(package_project_code())

    # Checkpoints: consistent snapshots of Kafka offsets + ALL operator state
    env.enable_checkpointing(int(a.checkpoint_s * 1000), CheckpointingMode.EXACTLY_ONCE)
    ckpt_dir = config.path("checkpoints", run)
    if os.path.isdir(ckpt_dir) and not a.keep_output:
        shutil.rmtree(ckpt_dir)
    os.makedirs(ckpt_dir, exist_ok=True)
    env.get_checkpoint_config().set_checkpoint_storage_dir("file://" + ckpt_dir)
    return env


def kafka_source(a, stream_dir: str):
    topic = f"ev-{a.stream}"
    builder = (KafkaSource.builder()
               .set_bootstrap_servers(a.bootstrap)
               .set_topics(topic)
               .set_group_id(f"ev-load-balancer-{a.run}")
               .set_starting_offsets(KafkaOffsetsInitializer.earliest())
               .set_value_only_deserializer(SimpleStringSchema()))
    n_events = None
    if not a.unbounded:
        manifest = os.path.join(stream_dir, ".manifest.json")
        if not os.path.exists(manifest):
            sys.exit(f"Stream manifest not found: {manifest}\nGenerate the stream first.")
        with open(manifest) as f:
            n_events = json.load(f)["events"]
        # stop after exactly n_events messages (single-partition topic, offsets 0..n-1)
        builder = builder.set_bounded(
            KafkaOffsetsInitializer.offsets({KafkaTopicPartition(topic, 0): n_events}))
    return builder.build(), topic, n_events


def to_late_record(tag_name: str):
    def f(r):
        r = dict(r)
        r["dropped_by"] = tag_name
        r["dropped_wall_ms"] = int(time.time() * 1000)
        return r
    return f


# --------------------------------------------------------------------------- pipeline
def main() -> None:
    a = parse_args()
    run = a.run or a.stream
    a.run = run
    tz = CFG["dataset"]["local_timezone"]
    al = CFG["alerts"]
    capacity = feeder_capacities()
    lateness_ms = int(a.allowed_lateness_min * 60_000)
    horizon_min = max(a.horizon_min, a.allowed_lateness_min + 5)

    out_dir = config.path("results", "outputs", run)
    if os.path.isdir(out_dir) and not a.keep_output:
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)
    stream_dir = config.path("data", "stream", a.stream)

    env = build_env(a, run)
    source, topic, n_events = kafka_source(a, stream_dir)

    # 1+2. source with event-time timestamps + watermarks
    wm_strategy = build_watermark_strategy(a.watermark_s, a.idleness_s,
                                           use_record_timestamp=(a.timestamps == "source"))
    if a.timestamps == "source":      # Kafka record timestamp = event_time (Java, at the source)
        raw = env.from_source(source, wm_strategy,
                              f"kafka:{topic} [event-time, watermark D={a.watermark_s:g}s]")
        timed = (raw.map(parse_event).name("parse-json")
                    .filter(lambda e: e is not None).name("drop-malformed"))
    else:                             # fallback: read event_time from the JSON in Python
        from pyflink.common.watermark_strategy import WatermarkStrategy
        raw = env.from_source(source, WatermarkStrategy.no_watermarks(), f"kafka:{topic}")
        timed = (raw.map(parse_event).name("parse-json")
                    .filter(lambda e: e is not None).name("drop-malformed")
                    .assign_timestamps_and_watermarks(wm_strategy)
                    .name(f"event-time+watermark(D={a.watermark_s:g}s)"))
    if a.fail_after > 0:
        marker = os.path.join(out_dir, "FAILURE_INJECTED.txt")
        timed = timed.map(FailOnce(a.fail_after, marker)).name(f"chaos(fail-after={a.fail_after})")

    # 3. diagnostics: lateness per feeder-hour
    probe = (timed.key_by(lambda e: e["feeder_id"], key_type=Types.STRING())
             .process(WatermarkProbe(60, tz)).name("watermark-probe"))
    json_file_sink(probe, out_dir, "probe")

    # 4. active sessions per station (keyed state) + tariffs (broadcast state)
    tariffs = timed.filter(lambda e: e["event_type"] == "TARIFF").name("tariff-events")
    charger = timed.filter(lambda e: e["event_type"] != "TARIFF").name("charger-events")
    stations = (charger.key_by(lambda e: e["station_id"], key_type=Types.STRING())
                .connect(tariffs.broadcast(TARIFF_STATE))
                .process(StationSessions(horizon_min, P["stale_session_hours"], tz))
                .name("station-sessions(keyed+broadcast state)"))
    station_load = stations.filter(lambda r: r["kind"] == "station_load").name("station-load")
    session_summaries = stations.filter(lambda r: r["kind"] == "session_summary").name("session-summaries")
    json_file_sink(session_summaries, out_dir, "sessions")

    # 5. event-time rolling feeder load: 1-minute tumbling windows + allowed lateness
    late_minute_tag = OutputTag("late-station-load", Types.PICKLED_BYTE_ARRAY())
    feeder_minute = (station_load
                     .key_by(lambda r: r["feeder_id"], key_type=Types.STRING())
                     .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                     .allowed_lateness(lateness_ms)
                     .side_output_late_data(late_minute_tag)
                     .process(FeederMinuteLoad(capacity, tz))
                     .name("feeder-load(1-min tumbling)"))
    json_file_sink(feeder_minute, out_dir, "feeder_load")

    # 6. sustained excursions: 15-minute sliding windows
    win_min = int(al["sustained_window_min"])
    late_sust_tag = OutputTag("late-feeder-minute", Types.PICKLED_BYTE_ARRAY())
    sustained = (feeder_minute
                 .key_by(lambda r: r["feeder_id"], key_type=Types.STRING())
                 .window(SlidingEventTimeWindows.of(Time.minutes(win_min),
                                                    Time.minutes(int(al["sustained_slide_min"]))))
                 .allowed_lateness(lateness_ms)
                 .side_output_late_data(late_sust_tag)
                 .process(SustainedLoad(capacity, win_min, tz))
                 .name(f"sustained-load({win_min}-min sliding)"))
    json_file_sink(sustained, out_dir, "sustained")

    # 7. alerts
    alerts = (feeder_minute.union(sustained)
              .key_by(lambda r: r["feeder_id"], key_type=Types.STRING())
              .process(AlertManager(al["spike_threshold_pct"], al["sustained_threshold_pct"], horizon_min))
              .name("alert-manager"))
    json_file_sink(alerts, out_dir, "alerts")

    # 8. late data that was too late for the windows (beyond allowed lateness)
    dropped = (feeder_minute.get_side_output(late_minute_tag).map(to_late_record("feeder-minute-window"))
               .union(sustained.get_side_output(late_sust_tag).map(to_late_record("sustained-window"))))
    json_file_sink(dropped, out_dir, "dropped_late")

    # 9. station summaries per local day (offset = UTC offset of local midnight)
    offset_h = int(CFG["dataset"].get("utc_offset_hours_for_daily_windows", 8))
    station_daily = (session_summaries
                     .key_by(lambda r: r["station_id"], key_type=Types.STRING())
                     .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(offset_h)))
                     .process(StationDailySummary(tz))
                     .name("station-daily-summary"))
    json_file_sink(station_daily, out_dir, "station_daily")

    # 10. optional: the naive processing-time version of feeder load (experiment E1)
    if a.proc_time_branch:
        win_ms = max(1, int(60_000 / a.speedup))
        proc = (timed.filter(lambda e: e["event_type"] == "POWER")
                .key_by(lambda e: e["feeder_id"], key_type=Types.STRING())
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(win_ms)))
                .process(ProcessingTimeLoad())
                .name(f"processing-time-load({win_ms} ms)"))
        json_file_sink(proc, out_dir, "proc_time_load")

    run_info = {"run": run, "stream": a.stream, "topic": topic, "events_expected": n_events,
                "watermark_s": a.watermark_s, "idleness_s": a.idleness_s,
                "allowed_lateness_min": a.allowed_lateness_min, "horizon_min": horizon_min,
                "parallelism": a.parallelism, "checkpoint_s": a.checkpoint_s, "timestamps": a.timestamps,
                "watermark_interval_ms": a.watermark_interval_ms, "bundle_size": a.bundle_size,
                "bundle_time_ms": a.bundle_time_ms,
                "state_backend": a.state_backend, "fail_after": a.fail_after,
                "proc_time_branch": a.proc_time_branch, "speedup": a.speedup,
                "capacity_kw": capacity, "started_wall_ms": int(time.time() * 1000)}
    with open(os.path.join(out_dir, "run_info.json"), "w") as f:
        json.dump(run_info, f, indent=2)
    print("=" * 74)
    print(f" Flink job '{run}' | topic {topic} | {n_events} events | D={a.watermark_s:g}s "
          f"| L={a.allowed_lateness_min:g}min | backend={a.state_backend}")
    print(f" feeder capacity (kW): {capacity}")
    print(f" outputs -> {out_dir}")
    print("=" * 74)

    result = env.execute(f"ev-load-balancer [{run}]")

    try:
        runtime = result.get_net_runtime()
    except Exception:
        runtime = 0
    if not runtime:          # detached cluster submission: the job keeps running on the cluster
        print(" job submitted to the cluster - follow it at http://localhost:8081")
        return
    run_info["net_runtime_ms"] = runtime
    run_info["finished_wall_ms"] = int(time.time() * 1000)
    with open(os.path.join(out_dir, "run_info.json"), "w") as f:
        json.dump(run_info, f, indent=2)
    print(f" job finished in {run_info.get('net_runtime_ms', 0) / 1000:.1f} s")


if __name__ == "__main__":
    main()
