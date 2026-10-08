"""Load the project configuration (config/project.json) from anywhere in the project."""
from __future__ import annotations

import json
import os
from functools import lru_cache

PROJECT_HOME = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def path(*parts: str) -> str:
    """Absolute path inside the project, e.g. path('data', 'processed')."""
    return os.path.join(PROJECT_HOME, *parts)


@lru_cache(maxsize=1)
def load() -> dict:
    with open(path("config", "project.json")) as f:
        cfg = json.load(f)
    # drop documentation keys ("_comment", "_note", ...) at every level
    def strip(d):
        if isinstance(d, dict):
            return {k: strip(v) for k, v in d.items() if not k.startswith("_")}
        return d
    return strip(cfg)


def site_to_feeder() -> dict:
    """{'BRYANT': 'F1-DOWNTOWN-N', ...}"""
    out = {}
    for fid, f in load()["feeders"].items():
        for s in f["sites"]:
            out[s] = fid
    return out
