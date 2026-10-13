#!/usr/bin/env bash
# Download NYC TLC yellow-taxi monthly Parquet files + the zone lookup table.
#
#   scripts/download_data.sh 2024            # one year
#   scripts/download_data.sh 2009 2026       # range of years (inclusive)
#
# Source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Files that do not exist yet (e.g. future 2026 months) are skipped.
set -euo pipefail

BASE_URL="https://d37ci6vzurychx.cloudfront.net"
DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/data"
FIRST_YEAR="${1:?usage: $0 FIRST_YEAR [LAST_YEAR]}"
LAST_YEAR="${2:-$FIRST_YEAR}"

mkdir -p "$DATA_DIR"

fetch() {  # url, destination
  if [[ -s "$2" ]]; then
    echo "exists   $(basename "$2")"
  elif curl -fsSL --retry 3 -o "$2.part" "$1"; then
    mv "$2.part" "$2"; echo "ok       $(basename "$2")"
  else
    rm -f "$2.part"; echo "missing  $(basename "$2")"
  fi
}

fetch "$BASE_URL/misc/taxi_zone_lookup.csv" "$DATA_DIR/taxi_zone_lookup.csv"

for year in $(seq "$FIRST_YEAR" "$LAST_YEAR"); do
  for month in $(seq -w 1 12); do
    f="yellow_tripdata_${year}-${month}.parquet"
    fetch "$BASE_URL/trip-data/$f" "$DATA_DIR/$f"
  done
done

# Record what was downloaded and when (needed for the README / report).
(cd "$DATA_DIR" && ls -l *.parquet *.csv > MANIFEST.txt && date -u +"downloaded %Y-%m-%dT%H:%MZ from $BASE_URL" >> MANIFEST.txt)
