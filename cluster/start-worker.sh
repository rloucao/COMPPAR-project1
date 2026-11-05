#!/usr/bin/env bash
# Every machine that contributes cores: start a worker that joins the master.
#   cluster/start-worker.sh <master-ip> [cores] [memory]
#   cluster/start-worker.sh 192.168.1.84 4 6g
# Use the same cores/memory on every machine, so that 1 vs 2 machines is a fair comparison.
set -euo pipefail
source "$(dirname "$0")/common.sh"
MASTER_IP="${1:?usage: $0 <master-ip> [cores] [memory]}"
CORES="${2:-4}"
MEMORY="${3:-6g}"
use_java
IP="$(lan_ip)"

if [ ! -f "$CLUSTER_DATA_DIR/taxi_zone_lookup.csv" ]; then
  echo "No data at $CLUSTER_DATA_DIR: run cluster/setup.sh first"; exit 1
fi
if ! curl -sf --max-time 5 "http://$MASTER_IP:$MASTER_UI_PORT/json/" >/dev/null; then
  echo "Cannot reach the master at http://$MASTER_IP:$MASTER_UI_PORT"
  echo "(same network? master started? firewall on the master machine?)"; exit 1
fi

nohup "$SPARK_CLASS" org.apache.spark.deploy.worker.Worker "spark://$MASTER_IP:$MASTER_PORT" \
  --host "$IP" --port "$WORKER_PORT" --webui-port "$WORKER_UI_PORT" \
  --cores "$CORES" --memory "$MEMORY" > "$LOGS/worker-$HOST.log" 2>&1 &
echo $! > "$LOGS/worker-$HOST.pid"

for _ in $(seq 1 30); do
  if alive_workers_json "$MASTER_IP:$MASTER_UI_PORT" | grep -q "\"host\": \"$IP\""; then
    echo "Worker $IP joined spark://$MASTER_IP:$MASTER_PORT with $CORES cores and $MEMORY"
    exit 
  fi
  sleep 1
done
echo "The worker did not register with the master; see $LOGS/worker-$HOST.log"
echo "(firewall on either machine? both on the same network?)"; exit 1
