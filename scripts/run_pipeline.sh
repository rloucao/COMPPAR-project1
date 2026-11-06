#!/usr/bin/env bash
# Run notebooks/pipeline.ipynb from the command line with papermill, which
# overrides the notebook's `parameters` cell. The executed copy (with every
# output) is saved to results/notebooks/.
#
#   scripts/run_pipeline.sh local   -p YEARS 2024 -p FRACTION 0.1 -p RUN_NAME local-smoke
#   scripts/run_pipeline.sh cluster -p YEARS 2024 -p RUN_NAME cluster-3w-baseline
#   scripts/run_pipeline.sh lan     -p YEARS 2024 -p RUN_NAME lan-2node-control-r1   (see cluster/README.md)
#   scripts/run_pipeline.sh local   -p RUN_NAME opt-shuffle64 \
#       -y 'SPARK_CONF: {spark.sql.shuffle.partitions: "64"}'
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:?usage: $0 local|cluster|lan [papermill args, e.g. -p YEARS 2024]}"
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
  lan)
    # Multi-machine standalone cluster (cluster/README.md); the driver runs on this
    # machine. MASTER_IP defaults to this machine. Outputs are computed but not saved
    # (no shared storage), and the job waits until every alive worker's cores registered.
    source "$ROOT/scripts/cluster/common.sh"
    use_java   # the driver needs Java 17/21 too (on Linux the notebook does not pick one)
    MASTER_IP="${MASTER_IP:-$(lan_ip)}"
    CORES="$(alive_workers_json "$MASTER_IP:$MASTER_UI_PORT" | "$PYTHON" -c 'import json,sys; print(sum(w["cores"] for w in json.load(sys.stdin)))')"
    echo "cluster: $CORES cores on alive workers (master $MASTER_IP)"
    PYSPARK_SUBMIT_ARGS="--conf spark.driver.host=$(lan_ip) --conf spark.driver.port=$DRIVER_PORT \
--conf spark.blockManager.port=$BLOCK_MANAGER_PORT --conf spark.cores.max=$CORES \
--conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
--conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s pyspark-shell" \
    "$ROOT/.venv/bin/papermill" "$ROOT/notebooks/pipeline.ipynb" "$ROOT/results/notebooks/pipeline-$STAMP.ipynb" \
      --cwd "$ROOT/notebooks" -p MASTER "spark://$MASTER_IP:$MASTER_PORT" \
      -p DATA_DIR "$CLUSTER_DATA_DIR" -p OUTPUT_DIR "$ROOT/output/_lan" -p RESULTS_DIR "$ROOT/results" \
      -p WRITE_OUTPUT False -p DRIVER_MEMORY 4g -p EXECUTOR_MEMORY "${EXECUTOR_MEMORY:-4g}" "$@"
    ;;
  *)
    echo "unknown mode '$MODE' (use local, cluster or lan)" >&2; exit 1
    ;;
esac
