"""
spark_profiler.py
------------------
Based on the Lab 3 `spark_profiler.py`. Extensions for the project:

* per-stage breakdown (`runs[name]["stages"]`): duration, tasks, shuffle
  bytes, spill, and a skew indicator (max task run time / median task run
  time, from the REST `taskSummary` endpoint) - the brief asks for per-stage
  duration and for stages where one task takes 10x the median;
* `save(path)` to dump every run as JSON, so baselines/optimisations from
  different machines and cluster sizes can be compared offline.

Original Lab 3 description follows.

A small, dependency-free helper for profiling PySpark jobs in class.

It does not require any extra JVM jar or Python package: it relies only on
two things Spark already gives you for free:

1. `SparkContext.setJobGroup(...)` to tag the jobs launched inside a block
   of code with a name.
2. The Spark Application UI's public REST API
   (http://<driver-host>:<ui-port>/api/v1/applications/<app-id>/stages),
   which is the only place PySpark exposes per-stage shuffle/spill metrics
   (`SparkContext.statusTracker()` does *not* expose them).

This is exactly what the Spark UI itself shows you in the Jobs/Stages tabs -
this helper just lets you capture it as numbers you can compare, instead of
eyeballing screenshots.

Usage
-----
    from spark_profiler import SparkProfiler

    profiler = SparkProfiler(spark)

    with profiler.profile("baseline"):
        ratings = spark.read.option("header", "true").option("inferSchema", "true") \\
                        .csv(PATH + "ratings*.csv.gz")
        ratings.groupBy("movieId").count().collect()

    # ... apply a fix ...

    with profiler.profile("repartitioned"):
        ratings = spark.read.option("header", "true").option("inferSchema", "true") \\
                        .csv(PATH + "ratings*.csv.gz").repartition(12)
        ratings.groupBy("movieId").count().collect()

    profiler.report()                       # table with every run so far
    profiler.compare("baseline", "repartitioned")   # before/after + % change

Notes
-----
* Open one browser tab on the Spark UI (the URL printed when the
  SparkSession is created, e.g. http://localhost:4040) to see the same
  information visually: DAG, task skew, executor timeline, SQL plan.
* `executor_cpu_time_ms` is derived from Spark's internal
  `executorCpuTime` metric, which Spark reports in **nanoseconds** while
  `executor_run_time_ms` (`executorRunTime`) is already in milliseconds.
  This mismatch trips people up constantly - the conversion is handled
  for you here.
* Each `profile(...)` block should trigger at least one Spark action
  (`.count()`, `.collect()`, `.show()`, ...). Nothing runs, nothing to
  measure - Spark is lazy.
* Do not nest `profile(...)` blocks, and avoid running unrelated Spark
  code (from another cell, or a background thread) while one is open -
  the job group tag would attribute its metrics to the wrong run.
"""

import json
import time
import urllib.request
import uuid
from contextlib import contextmanager
from datetime import datetime

try:
    import requests
except ImportError:  # pragma: no cover - fallback keeps this dependency-free
    requests = None


# Stage metrics we pull from the REST API and how to aggregate them
# across every stage that belongs to a profiled run.
_SUM_FIELDS = [
    "numTasks",
    "executorRunTime",
    "executorCpuTime",
    "inputBytes",
    "outputBytes",
    "shuffleReadBytes",
    "shuffleWriteBytes",
    "memoryBytesSpilled",
    "diskBytesSpilled",
    "inputRecords",
    "jvmGcTime",
    "shuffleFetchWaitTime",
    "shuffleWriteTime",
    "shuffleRemoteBytesRead",
]

_BYTE_FIELDS = {
    "inputBytes",
    "outputBytes",
    "shuffleReadBytes",
    "shuffleWriteBytes",
    "memoryBytesSpilled",
    "diskBytesSpilled",
}


class SparkProfiler:
    def __init__(self, spark, ui_base_url=None):
        """
        spark: the active SparkSession.
        ui_base_url: override the Spark UI base URL if auto-detection
                     doesn't work in your setup (e.g. running the driver
                     outside of the container that also hosts the UI).
                     Example: "http://spark-master:4040"
        """
        self.spark = spark
        self.sc = spark.sparkContext
        self.app_id = self.sc.applicationId
        self.ui_base_url = ui_base_url or self._detect_ui_base_url()
        self.runs = {}   # name -> aggregated metrics dict, in the order added

    def _detect_ui_base_url(self):
        # sc.uiWebUrl looks like 'http://<container-hostname>:4040'.
        # The notebook code runs *inside* the same container as the UI,
        # so talking to "localhost" on that same port always works, even
        # if the container's hostname isn't resolvable from itself.
        url = self.sc.uiWebUrl
        port = url.rsplit(":", 1)[-1] if url else "4040"
        return f"http://localhost:{port}"

    def _get_json(self, path):
        url = f"{self.ui_base_url}{path}"
        if requests is not None:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.json()
        with urllib.request.urlopen(url, timeout=10) as resp:  # pragma: no cover
            return json.loads(resp.read().decode())

    @contextmanager
    def profile(self, name):
        """Wrap the Spark code you want to measure in `with profiler.profile("name"):`."""
        group_id = f"{name}-{uuid.uuid4().hex[:8]}"
        self.sc.setJobGroup(group_id, name)
        wall_start = time.time()
        try:
            yield
        finally:
            wall_time = time.time() - wall_start
            # Collecting metrics is bookkeeping, not the point of the `with`
            # block - if it fails (most often because the code above it
            # already failed and took the Spark session down with it), don't
            # let that failure hide the real error with a second, confusing
            # traceback. Warn once and let the original exception propagate.
            try:
                self.sc.setJobGroup("", "")  # stop tagging jobs from later cells
                self.runs[name] = self._collect(group_id, wall_time)
                print(f"[{name}] wall time: {wall_time:.2f}s "
                      f"({self.runs[name]['num_stages']} stages, "
                      f"{self.runs[name]['num_tasks']} tasks) - "
                      f"see profiler.report() / profiler.compare(...)")
            except Exception as metrics_exc:
                print(f"[{name}] wall time: {wall_time:.2f}s (could not collect "
                      f"profiling metrics: {metrics_exc!r}). If the code inside "
                      f"this `with profiler.profile(...):` block also raised an "
                      f"error, that's the real problem - this is usually just "
                      f"the Spark session being unreachable as a consequence of "
                      f"that, most commonly because it was already `spark.stop()`-ed "
                      f"earlier in this kernel, or because the driver/executor "
                      f"ran out of memory. Restart the kernel and re-run from the "
                      f"Setup cells if that's the case.")

    def _collect(self, group_id, wall_time):
        status = self.sc.statusTracker()
        job_ids = status.getJobIdsForGroup(group_id)

        stage_ids = set()
        for jid in job_ids:
            info = status.getJobInfo(jid)
            if info is not None:
                stage_ids.update(info.stageIds)

        all_stages = self._get_json(f"/api/v1/applications/{self.app_id}/stages")
        # SKIPPED stages never ran (their shuffle output was reused), but the
        # REST API still reports their numTasks - counting them inflates task
        # counts, so they are left out.
        wanted = [s for s in all_stages
                  if s["stageId"] in stage_ids and s.get("status") != "SKIPPED"]

        agg = {field: sum(s.get(field, 0) for s in wanted) for field in _SUM_FIELDS}
        # executorCpuTime is reported in nanoseconds; executorRunTime in ms.
        agg["executorCpuTime"] = agg["executorCpuTime"] // 1_000_000
        agg["shuffleWriteTime"] = agg["shuffleWriteTime"] // 1_000_000   # also nanoseconds

        return {
            "stages": [self._stage_detail(s) for s in sorted(wanted, key=lambda s: s["stageId"])],
            "wall_time_s": round(wall_time, 2),
            "num_jobs": len(job_ids),
            "num_stages": len(wanted),
            "num_tasks": agg["numTasks"],
            "executor_run_time_ms": agg["executorRunTime"],
            "executor_cpu_time_ms": agg["executorCpuTime"],
            "input_bytes": agg["inputBytes"],
            "output_bytes": agg["outputBytes"],
            "shuffle_read_bytes": agg["shuffleReadBytes"],
            "shuffle_write_bytes": agg["shuffleWriteBytes"],
            "memory_spilled_bytes": agg["memoryBytesSpilled"],
            "disk_spilled_bytes": agg["diskBytesSpilled"],
            "input_records": agg["inputRecords"],
            "jvm_gc_time_ms": agg["jvmGcTime"],
            "shuffle_fetch_wait_ms": agg["shuffleFetchWaitTime"],
            "shuffle_write_time_ms": agg["shuffleWriteTime"],
            "shuffle_remote_read_bytes": agg["shuffleRemoteBytesRead"],
        }

    @staticmethod
    def _parse_ts(ts):
        # REST API timestamps look like '2026-10-09T14:03:12.345GMT'
        return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%f%Z")

    def _stage_detail(self, s):
        duration_ms = None
        if s.get("submissionTime") and s.get("completionTime"):
            delta = self._parse_ts(s["completionTime"]) - self._parse_ts(s["submissionTime"])
            duration_ms = int(delta.total_seconds() * 1000)

        # Skew indicator: max / median task executor run time for the stage.
        skew = median_ms = max_ms = None
        try:
            summary = self._get_json(
                f"/api/v1/applications/{self.app_id}/stages/{s['stageId']}/"
                f"{s['attemptId']}/taskSummary?quantiles=0.5,1.0")
            median_ms, max_ms = summary["executorRunTime"]
            skew = round(max_ms / median_ms, 2) if median_ms else None
        except Exception:
            pass  # stages with no completed tasks have no summary

        return {
            "stage_id": s["stageId"],
            "name": s.get("name", ""),
            "duration_ms": duration_ms,
            "num_tasks": s.get("numTasks", 0),
            "task_median_ms": median_ms,
            "task_max_ms": max_ms,
            "skew_max_over_median": skew,
            "input_bytes": s.get("inputBytes", 0),
            "shuffle_read_bytes": s.get("shuffleReadBytes", 0),
            "shuffle_write_bytes": s.get("shuffleWriteBytes", 0),
            "memory_spilled_bytes": s.get("memoryBytesSpilled", 0),
            "disk_spilled_bytes": s.get("diskBytesSpilled", 0),
            "executor_run_time_ms": s.get("executorRunTime", 0),
            "jvm_gc_time_ms": s.get("jvmGcTime", 0),
            "shuffle_fetch_wait_ms": s.get("shuffleFetchWaitTime", 0),
            "peak_execution_memory": s.get("peakExecutionMemory", 0),
        }

    def stages(self, name):
        """Return a pandas DataFrame with the per-stage breakdown of one run."""
        import pandas as pd
        return pd.DataFrame(self.runs[name]["stages"]).set_index("stage_id")

    def save(self, path, metadata=None):
        """Dump every run (totals + per-stage breakdown) to a JSON file."""
        with open(path, "w") as f:
            json.dump({"app_id": self.app_id, "metadata": metadata or {}, "runs": self.runs},
                      f, indent=2)

    @staticmethod
    def _fmt_bytes(n):
        n = float(n)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(n) < 1024:
                return f"{n:.1f}{unit}"
            n /= 1024
        return f"{n:.1f}PB"

    _BYTE_KEYS = (
        "input_bytes", "output_bytes", "shuffle_read_bytes",
        "shuffle_write_bytes", "memory_spilled_bytes", "disk_spilled_bytes",
        "shuffle_remote_read_bytes",
    )

    def report(self, names=None):
        """Return a pandas DataFrame with one row per profiled run (human-readable byte sizes)."""
        import pandas as pd
        names = names or list(self.runs.keys())
        rows = []
        for n in names:
            row = dict(self.runs[n])
            row["run"] = n
            for k in self._BYTE_KEYS:
                row[k] = self._fmt_bytes(row[k])
            rows.append(row)
        cols = ["run", "wall_time_s", "num_jobs", "num_stages", "num_tasks",
                "executor_run_time_ms", "executor_cpu_time_ms"] + list(self._BYTE_KEYS)
        return pd.DataFrame(rows)[cols].set_index("run")

    def compare(self, before, after):
        """Return a pandas DataFrame comparing two named runs, metric by metric, with % change."""
        import pandas as pd
        b, a = self.runs[before], self.runs[after]
        rows = []
        for key in (k for k in b if k != "stages"):
            bv, av = b[key], a[key]
            if isinstance(bv, (int, float)) and bv:
                change = f"{(av - bv) / bv * 100:+.1f}%"
            else:
                change = "-"
            bv_disp = self._fmt_bytes(bv) if key in self._BYTE_KEYS else bv
            av_disp = self._fmt_bytes(av) if key in self._BYTE_KEYS else av
            rows.append({"metric": key, before: bv_disp, after: av_disp, "change": change})
        return pd.DataFrame(rows).set_index("metric")
