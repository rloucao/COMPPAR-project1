#!/usr/bin/env bash
# Run once on EVERY machine of the cluster, from the repository root:
#   cluster/setup.sh
# Checks Java and PySpark, links the shared data path to this repository's data/
# folder, and verifies the data files (sizes from cluster/expected-files.txt).
set -uo pipefail
source "$(dirname "$0")/common.sh"

FAILED=0
ok()   { echo "  ok    $*"; }
fail() { echo "  FAIL  $*"; FAILED=1; }

if [ "$(uname)" = "Darwin" ]; then
  CORES="$(sysctl -n hw.ncpu)"; MEM_GB=$(( $(sysctl -n hw.memsize) / 1024 / 1024 / 1024 ))
else
  CORES="$(nproc)"; MEM_GB=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 / 1024 ))
fi
echo "Machine $(hostname): LAN IP $(lan_ip), $CORES cores, ${MEM_GB} GB RAM"

if use_java; then ok "Java: $JAVA_HOME"; else fail "Java 17 or 21"; fi

if [ -x "$SPARK_CLASS" ]; then
  V="$("$PYTHON" -c 'import pyspark; print(pyspark.__version__)')"
  if [ "$V" = "4.0.1" ]; then ok "PySpark $V"; else fail "PySpark is $V, needs 4.0.1"; fi
else
  fail "no .venv: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt"
fi

# Shared data path -> this repository's data/ folder
if [ -L "$CLUSTER_DATA_DIR" ] || [ -d "$CLUSTER_DATA_DIR" ]; then
  ok "data path $CLUSTER_DATA_DIR exists"
else
  ln -s "$ROOT/data" "$CLUSTER_DATA_DIR" && ok "linked $CLUSTER_DATA_DIR -> $ROOT/data"
fi

# Verify the files the experiments use (YEARS=2024 by default; pass 2025 to check it too)
YEARS="${*:-2024}"
MISSING=0
while read -r size name; do
  case "$name" in taxi_zone*) ;; *) echo "$YEARS" | grep -q "${name:16:4}" || continue ;; esac
  f="$CLUSTER_DATA_DIR/$name"
  if [ ! -f "$f" ]; then
    echo "        missing     $name"; MISSING=$((MISSING + 1))
  elif [ "$(wc -c < "$f" | tr -d ' ')" != "$size" ]; then
    echo "        wrong size  $name"; MISSING=$((MISSING + 1))
  fi
done < "$ROOT/scripts/cluster/expected-files.txt"
if [ "$MISSING" = 0 ]; then ok "data files for $YEARS complete"; else
  fail "$MISSING data file(s) missing or incomplete: run scripts/download_data.sh 2024"; fi

if [ "$FAILED" = 0 ]; then echo "Ready."; else echo "Fix the FAIL lines above, then run this again."; exit 1; fi
