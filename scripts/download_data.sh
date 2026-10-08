#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# download_data.sh - fetch the public EV charging dataset used by this project
#
# Dataset : City of Palo Alto - Electric Vehicle Charging Station Usage
#           (July 2011 - Dec 2020), ChargePoint session records
# Source  : https://data.paloalto.gov/datasets/194693/electric-vehicle-charging-station-usage-july-2011-dec-2020/
# Licence : Open Data Commons Public Domain Dedication and License (PDDL)
#
# Usage:  bash scripts/download_data.sh
# -----------------------------------------------------------------------------
set -euo pipefail

PROJECT_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$PROJECT_HOME/data/raw/palo_alto_ev_sessions.csv"

URLS=(
  "https://data.paloalto.gov/datasets/194693-electric-vehicle-charging-station-usage-july-2011-dec-2020.download/"
  "https://data.cityofpaloalto.org/datasets/194693-electric-vehicle-charging-station-usage-july-2011-dec-2020.download/"
)

if [ -s "$OUT" ]; then
  echo "Dataset already present: $OUT"
else
  for url in "${URLS[@]}"; do
    echo "Downloading from: $url"
    if curl -fL --retry 3 -o "$OUT" "$url"; then
      break
    fi
    echo "  ...failed, trying next mirror"
  done
fi

if [ ! -s "$OUT" ]; then
  echo "ERROR: download failed. Open the Source link above in a browser,"
  echo "download the CSV manually and save it as: $OUT"
  exit 1
fi

echo
echo "Saved to : $OUT"
echo "Size     : $(du -h "$OUT" | cut -f1)"
echo "Rows     : $(wc -l < "$OUT" | tr -d ' ') (including header)"
echo "File type: $(file -b "$OUT")"
echo
echo "First 3 lines:"
head -3 "$OUT"
