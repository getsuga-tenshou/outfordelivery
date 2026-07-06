#!/usr/bin/env bash


set -euo pipefail

export HOME=/tmp
export OFD_HOME=/opt/outfordelivery
export PYTHONUSERBASE=/tmp/ofd-deps

python3 -m pip install --user --quiet boto3 pyyaml


exec /opt/spark/bin/spark-submit \
  --master "local[*]" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8,org.apache.spark:spark-avro_2.12:3.5.8,io.delta:delta-spark_2.12:3.2.1,org.apache.hadoop:hadoop-aws:3.3.4" \
  /opt/outfordelivery/streaming/spark_job.py "$@"
