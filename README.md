# ComPar 26/27 — Project 1: Large-Scale Analytics with Spark

PySpark pipeline over the NYC TLC Yellow Taxi trip records (2009–2026), built
for a measurement-driven performance study (profile → hypothesise → optimise →
measure → repeat).

Operations: **M1** cleaning, **M2** temporal demand, **M3** zone statistics,
plus optional operations _TODO: pick ≥ 2 of O1–O4 and justify in the report_.

## Layout

```
notebooks/pipeline.ipynb    the pipeline: parameters, ingestion, M1, M2, M3, O1-O4, profiling, results JSON
notebooks/analysis.ipynb    reads results/*.json -> run overview, per-stage/skew tables,
                            before/after comparisons, optimisation table, scale & speedup charts
notebooks/spark_profiler.py Lab 3 profiler + per-stage breakdown, skew indicator, JSON export
docker/                     Spark standalone cluster (master + N workers + JupyterLab client/driver)
scripts/download_data.sh    dataset download
scripts/run_pipeline.sh     runs pipeline.ipynb from the command line (papermill)
results/                    <run>.json profiles + saved plans (input for the report)
report/                     LaTeX report (figures/ is written by analysis.ipynb)
data/, output/              dataset and pipeline outputs (git-ignored)
```

## Software versions

| Component | Version |
|---|---|
| Spark (Docker image) | `spark:4.0.1-python3` |
| PySpark (local) | 4.0.1 (`requirements.txt`) |
| Java (local) | 17 or 21 — Spark 4.0 does not support Java 25 |
| Python | 3.10+ |

## Dataset

- Trip records: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet`
- Zone lookup: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Landing page: <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

```bash
scripts/download_data.sh 2024            # one year (development)
scripts/download_data.sh 2009 2026       # full range
```

`data/MANIFEST.txt` records file sizes and the download date (TLC files are
occasionally re-published, so the date identifies the version used).

**Schema caveat:** 2009–2010 files use different column names and have
lat/lon instead of `PULocationID`/`DOLocationID`; see the Ingestion section of `notebooks/pipeline.ipynb`.

## Running

### Interactively in Jupyter (local)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
jupyter lab notebooks/
```

Spark 4.0 needs **Java 17 or 21** (Java 23+ fails with `getSubject is not
supported`). On macOS the notebook selects an installed JDK 21/17 by itself,
because IDE/Jupyter kernels don't inherit a `JAVA_HOME` exported in a
terminal. On other systems, set `JAVA_HOME` before starting Jupyter.

Open `pipeline.ipynb`, set the *Parameters* cell (e.g. `FRACTION = 0.01` for a
first smoke test), then **Kernel -> Restart Kernel and Run All Cells**. Always
restart between runs: `getOrCreate()` reuses the existing Spark session and
ignores new settings.

### Interactively on the Docker cluster

```bash
cd docker
docker compose up -d --build --scale spark-worker=3     # 3 workers x 2 cores x 2 GB
```

**Memory:** every container shares Docker Desktop's memory limit (Settings →
Resources; 8 GB here). The defaults fit that: workers `--memory 2G`, executors
`1536m`, driver `2g`. For bigger workers, raise Docker's limit first, then e.g.
`WORKER_MEMORY=3G docker compose up -d --scale spark-worker=3` and pass
`-p EXECUTOR_MEMORY 2g`.

**Fair measurements:** the Docker "cluster" runs on the same CPU cores as
your Mac, so shut down any local-mode notebook kernel before timing cluster
runs.

Open JupyterLab at <http://localhost:8888> (token `compar`), open
`notebooks/pipeline.ipynb` and set `MASTER = "spark://spark-master:7077"`,
`DRIVER_MEMORY = "2g"`, `EXECUTOR_MEMORY = "1536m"`. The data paths switch to
`/data`, `/output` and `/results` automatically.

- Spark master UI: <http://localhost:8080>
- Application UI (while the session is alive): <http://localhost:4140>.
  The container's port 4040 is published as 4140 on the host, because a
  local-mode notebook session already uses 4040.

Non-default Spark settings baked into the image (`docker/Dockerfile`):
`spark.scheduler.minRegisteredResourcesRatio=1.0` (plus a 60 s wait limit).
`scripts/run_pipeline.sh cluster` also sets `spark.cores.max` to the cores of
the alive workers, so the first job only starts once every executor has
registered.

### From the command line (papermill)

The brief requires command-line parameters. `scripts/run_pipeline.sh` runs
the notebook with papermill, which overrides the tagged *Parameters* cell:

```bash
scripts/run_pipeline.sh local   -p YEARS 2024 -p FRACTION 0.1 -p RUN_NAME local-smoke
scripts/run_pipeline.sh cluster -p YEARS 2024 -p RUN_NAME cluster-3w-baseline
scripts/run_pipeline.sh local   -p RUN_NAME opt-shuffle64 -y 'SPARK_CONF: {spark.sql.shuffle.partitions: "64"}'
```

| Parameter | Meaning |
|---|---|
| `MASTER` | `local[*]` or `spark://spark-master:7077` (set by the script) |
| `DATA_DIR`, `OUTPUT_DIR`, `RESULTS_DIR` | default to `../data` etc. locally and `/data` etc. on the cluster |
| `YEARS`, `MONTHS` | scale: `2024`, `2019-2024`, `2024,2026`; `1-12`, `1-3` |
| `FRACTION` | sample fraction (smoke tests, dataset-scale experiments) |
| `OPS` | e.g. `m2,m3,o2,o3` (M1 always runs) |
| `RUN_NAME` | label used for `results/<RUN_NAME>.json` |
| `EXPLAIN` | save `explain(mode="formatted")` of each operation |
| `SPARK_CONF` | non-default Spark settings for an experiment (recorded in the results JSON) |

### Analysing results

Open `notebooks/analysis.ipynb`. It needs no Spark, only pandas and matplotlib.
Fill in `OPTIMISATIONS`, `SCALE_RUNS` and `WORKER_RUNS` with run names. Charts
are saved to `report/figures/`.

## Experiments (reproduction log)

**Hardware:** Apple M4 (10 cores), 16 GB RAM, macOS 27.0.1. Docker 29.8.2 with
10 CPUs / 7.75 GB for all containers. Local mode: Java 21.0.8, PySpark 4.0.1.
Cluster image: Java 17.0.18, Spark 4.0.1.

**Protocol:** each configuration runs once as a discarded warm-up (OS file
cache, Docker VM memory), then 3 times (`-r1..r3`); the median is reported.
Only one environment runs at a time (cluster containers stopped during local
runs). Wall time = sum of the profiled operations M1–O3. Previous runs
(2026-10-16) are kept in `results/archive/2026-10-16/` for comparison.

| Run name | Where | Topology | Data | Non-default conf | Wall time (median, min–max) |
|---|---|---|---|---|---|
| `local-baseline-r1..r3` (2026-10-19) | `local[*]` | 1 JVM, 10 cores, 8 GB driver | 2024 (41.2 M trips) | none | 63.3 s (62.8–72.5) |
| `cluster-3w-baseline-r1..r3` (2026-10-19) | Docker standalone | 3 workers × 2 cores, 1536m executors, 2g driver | 2024 | image defaults (see above) | 121.1 s (111.0–153.5) |
| archived (2026-10-16), no warm-up | as above | as above | 2024 | as above | local 64.6 s (61.3–65.9), cluster 102.6 s (96.4–108.2) |

Cluster timings vary with the host's state: on 2026-10-19 the Mac had 23 GB of
swap in use, and identical runs consumed 517–724 s of executor CPU time (vs.
434–492 s on 2026-10-16). Before the measurements used for comparisons,
restart the Mac, close other applications, and re-run the baseline in the same
session as the optimised runs.

```bash
scripts/run_pipeline.sh local   -p YEARS 2024 -p RUN_NAME local-baseline-r1 -p EXPLAIN True
scripts/run_pipeline.sh cluster -p YEARS 2024 -p RUN_NAME cluster-3w-baseline-r1 -p EXPLAIN True
```

### Optimisation study (2026-10-19)

Each switch is a notebook parameter; all off = baseline. Every configuration ran
3 times next to a same-session control (`<env>-control-r1..r3`), and its outputs were
compared value by value with the control's (all identical).

| Config | Switches | Local median (s) | Cluster median (s) |
|---|---|---|---|
| `control` | none | 69.1 | 95.8 |
| `opt1-cache-input` | `-p CACHE_INPUT True` | 33.9 | 48.3 |
| `opt2-cache-intermediate` | `-p CACHE_INTERMEDIATE True` | 37.6 | 46.1 |
| `opt3-split16` | `-p READ_SPLIT_MB 16` | 64.2 | – |
| `opt4-align-keys` | `-p ALIGN_KEYS True` | 64.5 | – |
| `opt5-broadcast` | `-p BROADCAST_HINTS True` | 55.9 | – |
| `opt1+2` | opt 1 + opt 2 | 27.8 | 37.7 |
| `all-opts` | all five | 25.1 | 38.7 |

```bash
scripts/run_pipeline.sh local -p YEARS 2024 -p RUN_NAME local-opt1-cache-input-r1 -p CACHE_INPUT True
```

A first batch (before the O2 tie-break fix, with opt 2 also caching O2's `routes`) is
kept in `results/archive/2026-10-19-batch1/`.

### Scalability (cluster, optimised pipeline = all five switches)

- Data size, 3 workers: `cluster-scale-{1,2,3,6}m-r1` (`-p MONTHS 1`, `1-2`, `1-3`, `1-6`),
  `cluster-scale-24m-r1` (`-p YEARS 2024-2025`); 12 months = `cluster-all-opts-r1..r3`.
- Cluster size, full 2024: `cluster-workers-{1,2,4,5}w-r1` after
  `docker compose up -d --scale spark-worker=N`; 3 workers = `cluster-all-opts-r1..r3`.
- Figures: run `notebooks/analysis.ipynb`; it writes `report/figures/*.pdf`.

## Acknowledgements

_TODO: AI tools used and how (required by the brief)._
