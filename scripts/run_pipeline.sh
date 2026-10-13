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
    COMPOSE=(docker compose -f "$ROOT/docker/docker-compose.yml")
    # Total cores of the ALIVE workers. With spark.cores.max set to it, the
    # image's spark.scheduler.minRegisteredResourcesRatio=1.0 makes the driver
    # wait for every executor before the first job (otherwise standalone mode
    # starts as soon as one executor has registered and skews the timings).
    CORES="$("${COMPOSE[@]}" exec -T client python3 -c '
import json, urllib.request
workers = json.load(urllib.request.urlopen("http://spark-master:8080/json/"))["workers"]
print(sum(w["cores"] for w in workers if w["state"] == "ALIVE"))')"
    echo "cluster: $CORES cores on alive workers"
    # Memory defaults sized for the Docker workers (--memory 2G); later -p
    # arguments override them, e.g. -p EXECUTOR_MEMORY 2g with WORKER_MEMORY=3G.
    "${COMPOSE[@]}" exec -e PYSPARK_SUBMIT_ARGS="--conf spark.cores.max=$CORES pyspark-shell" client \
      papermill /app/notebooks/pipeline.ipynb "/results/notebooks/pipeline-$STAMP.ipynb" \
      --cwd /app/notebooks -p MASTER spark://spark-master:7077 \
      -p DRIVER_MEMORY 2g -p EXECUTOR_MEMORY 1536m "$@"
    ;;
  *)
    echo "unknown mode '$MODE' (use local or cluster)" >&2; exit 1
    ;;
esac
