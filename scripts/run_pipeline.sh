#!/usr/bin/env bash
# Run notebooks/pipeline.ipynb from the command line with papermill, which
# overrides the notebook's `parameters` cell. The executed copy (with every
# output) is saved to results/notebooks/.
#
#   scripts/run_pipeline.sh local   -p YEARS 2024 -p FRACTION 0.1 -p RUN_NAME local-smoke
#   scripts/run_pipeline.sh cluster -p YEARS 2024 -p RUN_NAME cluster-3w-baseline
#   scripts/run_pipeline.sh local   -p RUN_NAME opt-shuffle64 \
#       -y 'SPARK_CONF: {spark.sql.shuffle.partitions: "64"}'
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:?usage: $0 local|cluster [papermill args, e.g. -p YEARS 2024]}"
shift
STAMP="$(date +%Y%m%d-%H%M%S)"
mkdir -p "$ROOT/results/notebooks"

case "$MODE" in
  local)
    papermill "$ROOT/notebooks/pipeline.ipynb" "$ROOT/results/notebooks/pipeline-$STAMP.ipynb" \
      --cwd "$ROOT/notebooks" -p MASTER "local[*]" "$@"
    ;;
  cluster)
    docker compose -f "$ROOT/docker/docker-compose.yml" exec client \
      papermill /app/notebooks/pipeline.ipynb "/results/notebooks/pipeline-$STAMP.ipynb" \
      --cwd /app/notebooks -p MASTER spark://spark-master:7077 "$@"
    ;;
  *)
    echo "unknown mode '$MODE' (use local or cluster)" >&2; exit 1
    ;;
esac
