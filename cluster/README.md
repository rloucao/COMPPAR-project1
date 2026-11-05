# Two-machine Spark cluster

A real Spark standalone cluster on two computers on the same network: Spark runs
natively from the project's `.venv` (PySpark 4.0.1 is a full Spark distribution),
without Docker.

| | Machine 1 (Rodrigo) | Machine 2 (Guilherme) |
|---|---|---|
| Runs | master + worker + driver (the notebook) | worker |
| Starts | `cluster/start-master.sh`, `cluster/start-worker.sh` | `cluster/start-worker.sh` |

How it works:
- Every worker reads the input from its **own disk**, so the data must be at the same
  path on both machines: `/tmp/compar-taxi-data`, a link that `setup.sh` creates to the
  repository's `data/` folder.
- There is no shared storage, so operations run completely but **do not save their outputs**
  (`WRITE_OUTPUT=False`, Spark's `noop` sink). Correctness was already checked locally and
  on the Docker cluster. The M1 statistics and the profiling results are saved on machine 1.
- Ports are fixed so they can be allowed in a firewall: master 7077 (UI 8080),
  worker 7091 (UI 8081), driver 7078, block managers 7079 and up.

## 1. Both machines: prepare (once)

Requirements: macOS or Linux (Windows: use WSL2), Python 3.10+, Java 17 or 21, and both
machines on the same network (same Wi-Fi, or better, the same cable router).

```bash
git clone https://github.com/rloucao/COMPPAR-project1.git   # or: git pull
cd COMPPAR-project1
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
scripts/download_data.sh 2024        # 12 files, ~690 MB
cluster/setup.sh                     # must end with "Ready."
```

**Firewall**: allow incoming connections for **Java** on both machines. On macOS, either
click *Allow* when macOS asks the first time Spark starts, or add `java` under
System Settings > Network > Firewall > Options. On Linux with ufw:
`sudo ufw allow 7077:7099/tcp && sudo ufw allow 8080:8081/tcp`.

## 2. Machine 1: master and its own worker

```bash
cluster/start-master.sh                       # prints the master address, e.g. 192.168.1.84
cluster/start-worker.sh 192.168.1.84 4 6g     # 4 cores, 6 GB for executors
```

Check http://192.168.1.84:8080: one worker, ALIVE.

## 3. Machine 1: experiments with ONE machine

```bash
cluster/run_experiments.sh 1node      # warm-up + 3 rounds of baseline and optimised, ~15 min
```

## 4. Machine 2: join the cluster

Use **the same cores and memory** as machine 1's worker, so that 1 vs 2 machines is a
fair comparison:

```bash
cluster/start-worker.sh 192.168.1.84 4 6g     # the master's address from step 2
```

Check http://192.168.1.84:8080: two workers, both ALIVE.

## 5. Machine 1: experiments with TWO machines

```bash
cluster/run_experiments.sh 2node
```

During the runs, don't use either machine for anything heavy. Results:
`results/lan-1node-*.json` and `results/lan-2node-*.json`. Each run's log is in
`cluster/logs/`. Both scripts print a median summary at the end, including the shuffle data
that crossed the network ("remote shuffle").

## 6. Both machines: stop

```bash
cluster/stop.sh
```

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `start-worker.sh`: cannot reach the master | different networks, master not started, or machine 1's firewall |
| worker starts but never registers | firewall on machine 2 (the master connects back to the worker) |
| run hangs at the start, then fails after 60 s | machine 2 cannot reach the driver on machine 1 (port 7078, firewall) |
| tasks fail with `FileNotFoundException` | data missing on one machine: run `cluster/setup.sh` there |
| `/tmp/compar-taxi-data` missing after a reboot | macOS empties `/tmp`: run `cluster/setup.sh` again |
| wrong network address detected | set it by hand: `MY_IP=192.168.1.x cluster/start-worker.sh ...` |
| "No Java 17 or 21 found" (e.g. the node only has Java 11) | unpack a JDK in your home folder (below); the scripts find `~/jdk-17*` / `~/jdk-21*` automatically |

### A JDK without admin rights

A JDK is just a folder: unpacking it in your home folder installs nothing system-wide, and
on a cluster with a shared home one copy serves every node. Remove it with `rm -rf ~/jdk-17*`.

```
cd ~
curl -L -o jdk17.tar.gz "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse"
tar -xzf jdk17.tar.gz && rm jdk17.tar.gz
ls -d ~/jdk-17*
```

To force a specific JDK: `export CLUSTER_JAVA_HOME=~/jdk-17.0.x+y` before the scripts.
