#!/usr/bin/env bash
# Machine 1 (the driver): run the baseline (control) and the optimised pipeline (all
# five switches), interleaved, on whatever workers are alive right now.
#   cluster/run_experiments.sh 1node     # only machine 1's worker is running
#   cluster/run_experiments.sh 2node     # after machine 2's worker joined
# Optional: number of rounds (default 3). Results: results/lan-<label>-*.json
set -uo pipefail
source "$(dirname "$0")/common.sh"
LABEL="${1:?usage: $0 <label, e.g. 1node or 2node> [rounds]}"
ROUNDS="${2:-3}"
OPT=(-p CACHE_INPUT True -p CACHE_INTERMEDIATE True -p READ_SPLIT_MB 16 -p ALIGN_KEYS True -p BROADCAST_HINTS True)
cd "$ROOT"
MASTER_IP="${MASTER_IP:-$(lan_ip)}"
export MASTER_IP

WORKERS="$(alive_workers_json "$MASTER_IP:$MASTER_UI_PORT")" || { echo "No master at $MASTER_IP"; exit 1; }
echo "Alive workers: $(echo "$WORKERS" | "$PYTHON" -c '
import json, sys
print(", ".join("%s (%d cores, %d MB)" % (w["host"], w["cores"], w["memory"]) for w in json.load(sys.stdin)))')"

run() {
  local name="$1"; shift
  echo "=== $name start $(date +%T)"
  scripts/run_pipeline.sh lan -p YEARS 2024 -p RUN_NAME "$name" "$@" > "$LOGS/$name.log" 2>&1
  local code=$?
  echo "=== $name exit $code end $(date +%T)"
  [ "$code" = 0 ] || echo "    see $LOGS/$name.log"
}

run "_warmup-lan-$LABEL"
mkdir -p results/archive/warmup && mv results/_warmup-lan-* results/archive/warmup/ 2>/dev/null
for r in $(seq 1 "$ROUNDS"); do
  run "lan-$LABEL-control-r$r"
  run "lan-$LABEL-all-opts-r$r" "${OPT[@]}"
done

"$PYTHON" - "$ROOT/results" "$LABEL" <<'EOF'
import glob, json, statistics, sys
results, label = sys.argv[1], sys.argv[2]
for config in ("control", "all-opts"):
    runs = [json.load(open(p)) for p in sorted(glob.glob(f"{results}/lan-{label}-{config}-r*.json"))]
    if runs:
        t = [r["metadata"]["total_wall_time_s"] for r in runs]
        remote = statistics.median(sum(x.get("shuffle_remote_read_bytes", 0) for x in r["runs"].values()) for r in runs)
        print(f"{label} {config:9} median {statistics.median(t):6.1f}s  (range {min(t):.1f}-{max(t):.1f}, "
              f"{len(t)} runs, {runs[0]['metadata']['total_cores']} cores, remote shuffle {remote / 2**20:.0f} MB)")
EOF
