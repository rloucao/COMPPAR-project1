# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark

PySpark pipeline over the NYC TLC Yellow Taxi trip records (2024), with a
measurement-driven performance study: profile, optimise, measure.

## What we did

| Operation | Description |
|---|---|
| **M1** | Data cleaning: seven validity criteria, per-criterion statistics |
| **M2** | Temporal demand per borough, weekday and hour |
| **M3** | Monthly zone statistics with window functions (rank, rolling share, anomaly flag) |
| **O2** | Origin–destination flows: top-20 routes per borough, Q1/Q3 share changes |
| **O3** | Demand features per zone and hour, with a 4-week lag |

We profiled the baseline (per-stage times, shuffle, skew, GC) and applied five
optimisations, each a switch in the notebook. Caching the input (`CACHE_INPUT`)
and the O3 hourly table (`CACHE_INTERMEDIATE`) gave the main gains. Read
splits, aligned keys and broadcast hints did not help. Scalability was measured
on a Docker cluster and on the university DI cluster. Details are in the report.

## Layout

```
notebooks/pipeline.ipynb    the pipeline (parameters, ingestion, M1-O3, profiling, results JSON)
notebooks/analysis.ipynb    reads results/*.json and produces tables and charts
notebooks/spark_profiler.py profiler (per-stage breakdown, skew, JSON export)
docker/                     Spark standalone cluster in Docker
scripts/cluster/            scripts for a native Spark cluster on several machines (see scripts/cluster/README.md)
scripts/download_data.sh    dataset download
scripts/run_pipeline.sh     runs the pipeline from the command line (papermill)
results/                    run profiles (JSON) and saved plans
report/                     LaTeX report
data/, output/              dataset and outputs (git-ignored)
```

## Requirements

- Spark / PySpark 4.0.1
- Java 17 or 21 (Spark 4.0 does not support Java 25)
- Python 3.10+
- Docker (only for the Docker cluster)

## Data

```bash
scripts/download_data.sh 2024
```

Files come from the TLC (`yellow_tripdata_YYYY-MM.parquet` and
`taxi_zone_lookup.csv`) and are stored in `data/`.

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run locally

```bash
scripts/run_pipeline.sh local -p YEARS 2024 -p RUN_NAME local-baseline
```

Or open `notebooks/pipeline.ipynb` in JupyterLab, set the *Parameters* cell and
run all cells (restart the kernel between runs).

Optimisation switches (all off = baseline):

```bash
scripts/run_pipeline.sh local -p YEARS 2024 -p RUN_NAME local-opt1 -p CACHE_INPUT True
scripts/run_pipeline.sh local -p YEARS 2024 -p RUN_NAME local-all \
    -p CACHE_INPUT True -p CACHE_INTERMEDIATE True -p READ_SPLIT_MB 16 \
    -p ALIGN_KEYS True -p BROADCAST_HINTS True
```

| Parameter | Meaning |
|---|---|
| `YEARS`, `MONTHS` | data scale, e.g. `2024`, `2024-2025`, `1-3` |
| `FRACTION` | sample fraction for quick tests |
| `OPS` | operations to run, default `m2,m3,o2,o3` (M1 always runs) |
| `RUN_NAME` | name of the result file `results/<RUN_NAME>.json` |
| `EXPLAIN` | save the physical plan of each operation |
| `CACHE_INPUT`, `CACHE_INTERMEDIATE`, `READ_SPLIT_MB`, `ALIGN_KEYS`, `BROADCAST_HINTS` | optimisation switches |

## Run on the Docker cluster

```bash
cd docker
docker compose up -d --build --scale spark-worker=3
cd ..
scripts/run_pipeline.sh cluster -p YEARS 2024 -p RUN_NAME cluster-3w-baseline
```

Master UI: <http://localhost:8080>. Application UI: <http://localhost:4140>.

## Run on the DI cluster (several machines)

Full instructions are in [scripts/cluster/README.md](scripts/cluster/README.md). In short, one
worker per node, then the experiments on the master node:

```bash
scripts/cluster/start-master.sh                                # on the master node
scripts/cluster/start-worker.sh <master-ip> 16 48g             # on every node
EXECUTOR_MEMORY=40g scripts/cluster/run_experiments.sh 3node   # on the master node
```

`run_experiments.sh` runs the baseline and the optimised pipeline (warm-up plus
3 runs each) and writes `results/lan-3node-*.json`. Use `1node` or `2node` for
fewer nodes, and `scripts/cluster/stop.sh` to shut everything down.

## Results

Open `notebooks/analysis.ipynb` to build the tables and the figures in
`report/figures/`.

Main results (2024, 41.2 M trips):

| | Baseline | Optimised |
|---|---|---|
| Local (10 cores) | 69.1 s | 25.1 s |
| DI cluster, 1 / 2 / 3 nodes | 109.7 / 82.4 / 63.9 s | 45.3 / 43.1 / 34.8 s |

## Acknowledgements

AI tools (Claude) were used to support the implementation, the experiments and
the writing of this project; see the report for details.
