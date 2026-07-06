#!/usr/bin/env bash


set -euo pipefail

export HOME=/tmp
export OFD_HOME=/opt/outfordelivery


exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages "io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4" \
  /opt/outfordelivery/streaming/silver_job.py "$@"
