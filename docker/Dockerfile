# Pinned so the Spark version in the report is reproducible.
FROM spark:4.0.1-python3

USER root
ENV SPARK_HOME=/opt/spark
ENV HOME=/tmp
ENV PYTHONPATH=/opt/spark/python:$PYTHONPATH

RUN pip install --no-cache-dir pandas pyarrow requests matplotlib jupyterlab papermill

WORKDIR /app
