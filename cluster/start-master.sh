#!/usr/bin/env bash
# Machine 1 only: start the Spark master on this machine's LAN address.
#   cluster/start-master.sh
set -euo pipefail
source "$(dirname "$0")/common.sh"
use_java
IP="$(lan_ip)"

if curl -sf "http://$IP:$MASTER_UI_PORT/json/" >/dev/null; then
  echo "A master is already running at http://$IP:$MASTER_UI_PORT"; exit 0
fi
nohup "$SPARK_CLASS" org.apache.spark.deploy.master.Master \
  --host "$IP" --port "$MASTER_PORT" --webui-port "$MASTER_UI_PORT" > "$LOGS/master-$HOST.log" 2>&1 &
echo $! > "$LOGS/master-$HOST.pid"

for _ in $(seq 1 30); do
  curl -sf "http://$IP:$MASTER_UI_PORT/json/" >/dev/null && break
  sleep 1
done
if ! curl -sf "http://$IP:$MASTER_UI_PORT/json/" >/dev/null; then
  echo "The master did not start; see $LOGS/master-$HOST.log"; exit 
fi
echo "Master running: spark://$IP:$MASTER_PORT (UI http://$IP:$MASTER_UI_PORT)"
echo "Next, on every machine that contributes cores (this one included):"
echo "  cluster/start-worker.sh $IP <cores> <memory>     e.g. cluster/start-worker.sh $IP 4 6g"
