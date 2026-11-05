#!/usr/bin/env bash
# Shared settings for the multi-machine Spark standalone cluster.
# Sourced by the other scripts in this folder and by scripts/run_pipeline.sh (lan mode).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$ROOT/.venv/bin/python"
# The pip-installed PySpark is a full Spark distribution. Point SPARK_HOME at it
# explicitly: otherwise spark-class looks for it with the system python3, which
# does not have PySpark installed.
if [ -x "$PYTHON" ]; then
  export SPARK_HOME="$("$PYTHON" -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')"
fi
SPARK_CLASS="${SPARK_HOME:-$ROOT/.venv}/bin/spark-class"

# Java 17+ blocks the internal access Spark needs unless these modules are opened.
# The driver and executors get them from Spark itself; the master and worker daemons
# started with spark-class did not on the university cluster, so pass them explicitly
# (same list as Spark 4.0.1's launcher, JavaModuleOptions). Repeating them is harmless.
export SPARK_DAEMON_JAVA_OPTS="${SPARK_DAEMON_JAVA_OPTS:-} -XX:+IgnoreUnrecognizedVMOptions \
--add-modules=jdk.incubator.vector \
--add-opens=java.base/java.lang=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
--add-opens=java.base/java.io=ALL-UNNAMED \
--add-opens=java.base/java.net=ALL-UNNAMED \
--add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
--add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
--add-opens=java.base/sun.security.action=ALL-UNNAMED \
--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
-Djdk.reflect.useDirectMethodHandle=false \
-Dio.netty.tryReflectionSetAccessible=true"
LOGS="$ROOT/cluster/logs"
HOST="$(hostname -s)"   # the home folder (and so cluster/logs) may be shared between machines:
                        # log and pid files carry the machine's name
export SPARK_WORKER_DIR="$ROOT/cluster/work"   # executor stdout/stderr per application
mkdir -p "$LOGS" "$SPARK_WORKER_DIR"

# Executors read the input from their own disk, so it must be at the same absolute
# path on every machine. /tmp exists everywhere without admin rights; setup.sh links
# it to this repository's data/ folder.
CLUSTER_DATA_DIR="${CLUSTER_DATA_DIR:-/tmp/compar-taxi-data}"

# Fixed ports, so they are easy to allow in a firewall.
MASTER_PORT=7077
MASTER_UI_PORT=8080
WORKER_PORT=7091
WORKER_UI_PORT=8081
DRIVER_PORT=7078
BLOCK_MANAGER_PORT=7079

lan_ip() {  # this machine's address on the local network (override with MY_IP=...)
  if [ -n "${MY_IP:-}" ]; then echo "$MY_IP"; return; fi
  if [ "$(uname)" = "Darwin" ]; then
    local i ip
    for i in en0 en1 en2 en3 en4; do
      ip="$(ipconfig getifaddr "$i" 2>/dev/null || true)"
      if [ -n "$ip" ]; then echo "$ip"; return; fi
    done
  else
    hostname -I 2>/dev/null | awk '{print $1}'
  fi
}

java_major() {  # major version of the JDK in $1, or nothing
  [ -x "$1/bin/java" ] && "$1/bin/java" -version 2>&1 | head -1 | sed -E 's/.*version "([0-9]+).*/\1/'
}

use_java() {  # Spark 4.0 runs on Java 17 or 21 only; finds one and exports JAVA_HOME
  local candidates=() c
  if [ -n "${CLUSTER_JAVA_HOME:-}" ]; then candidates+=("$CLUSTER_JAVA_HOME"); fi
  if [ "$(uname)" = "Darwin" ]; then
    candidates+=("$(/usr/libexec/java_home -v 21 2>/dev/null || true)" "$(/usr/libexec/java_home -v 17 2>/dev/null || true)")
  else
    # JAVA_HOME may point to an older JDK while 17/21 is installed next to it,
    # or a JDK may have been unpacked in the (shared) home folder.
    candidates+=("${JAVA_HOME:-}")
    for c in "$HOME"/jdk-21* "$HOME"/jdk-17* /usr/lib/jvm/*21* /usr/lib/jvm/*17* /opt/*jdk*21* /opt/*jdk*17*; do
      [ -d "$c" ] && candidates+=("$c")
    done
    if command -v java >/dev/null; then
      candidates+=("$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")")
    fi
  fi
  for c in "${candidates[@]}"; do
    case "$(java_major "$c")" in
      17|21) export JAVA_HOME="$c"; return 0 ;;
    esac
  done
  echo "No Java 17 or 21 found on $(hostname) (JAVA_HOME=${JAVA_HOME:-unset})." >&2
  echo "Unpack one in your home folder (see cluster/README.md), or set CLUSTER_JAVA_HOME." >&2
  return 1
}

alive_workers_json() {  # the master's view of its workers: alive ones only
  "$PYTHON" - "$1" <<'EOF'
import json, sys, urllib.request
workers = json.load(urllib.request.urlopen(f"http://{sys.argv[1]}/json/", timeout=5))["workers"]
print(json.dumps([w for w in workers if w["state"] == "ALIVE"]))
EOF
}
