"""
sinks.py - write result streams to JSON-lines files.

Flink's FileSink is exactly-once: files are written as hidden "in-progress" files and
only become visible (committed) when a checkpoint completes. So after a failure no
half-written or duplicated results appear - a property used in the fault-tolerance
experiment. (This is why checkpointing is always enabled in this job.)

Output layout:  results/outputs/<run>/<sink name>/<sink name>-<id>-<n>.jsonl
"""
from __future__ import annotations

import json
import os

from pyflink.common import Types
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors.file_system import (BucketAssigner, FileSink,
                                                       OutputFileConfig, RollingPolicy)


def to_json(record) -> str:
    return json.dumps(record, separators=(",", ":"), default=str)


def json_file_sink(stream, out_dir: str, name: str):
    path = os.path.join(out_dir, name)
    os.makedirs(path, exist_ok=True)
    sink = (FileSink
            .for_row_format("file://" + path, Encoder.simple_string_encoder())
            .with_output_file_config(OutputFileConfig.builder()
                                     .with_part_prefix(name)
                                     .with_part_suffix(".jsonl")
                                     .build())
            .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())
            .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy())
            .build())
    return (stream
            .map(to_json, output_type=Types.STRING()).name(f"to-json:{name}")
            .sink_to(sink).name(f"sink:{name}"))
