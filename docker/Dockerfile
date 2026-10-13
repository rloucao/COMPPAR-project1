# Pinned so the Spark version in the report is reproducible.
FROM spark:4.0.1-python3

USER root
ENV SPARK_HOME=/opt/spark
ENV HOME=/tmp
ENV PYTHONPATH=/opt/spark/python:$PYTHONPATH

# py4j must match the version bundled in /opt/spark/python/lib (Spark 4.0.1 -> 0.10.9.9)
RUN pip install --no-cache-dir py4j==0.10.9.9 pandas pyarrow requests matplotlib jupyterlab papermill

WORKDIR /app

# Cluster runs: don't start the first job until every executor has registered
# (standalone mode defaults to not waiting), so timings never include a
# partially-assembled cluster.
RUN mkdir -p /opt/spark/conf && printf '%s\n' \
      'spark.scheduler.minRegisteredResourcesRatio 1.0' \
      'spark.scheduler.maxRegisteredResourcesWaitingTime 60s' \
      > /opt/spark/conf/spark-defaults.conf
