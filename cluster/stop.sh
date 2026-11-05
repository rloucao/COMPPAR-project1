#!/usr/bin/env bash
# Stop the Spark worker and/or master running on THIS machine.
#   cluster/stop.sh            # both
#   cluster/stop.sh worker     # only the worker (e.g. to restart it with other cores/memory)
#   cluster/stop.sh master     # only the master
# Processes are found by class name among this user's processes on this machine
# (the home folder may be shared between machines, so pid files are not trusted).
source "$(dirname "$0")/common.sh"
ROLES="${1:-worker master}"
for role in $ROLES; do
  case "$role" in
    worker) CLASS=org.apache.spark.deploy.worker.Worker ;;
    master) CLASS=org.apache.spark.deploy.master.Master ;;
    *) echo "usage: $0 [worker|master]"; exit 1 ;;
  esac
  if pkill -u "$(id -u)" -f "$CLASS"; then
    echo "stopped $role on $HOST"
  else
    echo "no $role running on $HOST"
  fi
  rm -f "$LOGS/$role-$HOST.pid" "$LOGS/$role.pid"
done
