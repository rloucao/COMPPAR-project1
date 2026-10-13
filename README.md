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

_TODO: fill in as experiments are run — one row per run, matching `results/*.json`._

| Run name | Where | Topology (workers × cores, mem) | Data | Non-default conf | Wall time |
|---|---|---|---|---|---|
| | | | | | |

## Acknowledgements

_TODO: AI tools used and how (required by the brief)._
